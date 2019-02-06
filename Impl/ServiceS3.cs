/* ------------------------------------------------------------------------- *
thZero.NetCore.Library.Factories.DryIoc
Copyright (C) 2016-2018 thZero.com

<development [at] thzero [dot] com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 * ------------------------------------------------------------------------- */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Nito.AsyncEx;

using Amazon.S3;
using Amazon.S3.Model;

namespace thZero.Services
{
    public class ServiceS3 : ServiceLoggableBase<ServiceS3>, IServiceS3
    {
        public ServiceS3(IAmazonS3 serviceS3, ILogger<ServiceS3> logger) : base(logger)
        {
            Service = serviceS3;
        }

        public async Task<FileObject> ListAsync(string bucketName, params string[] folders)
        {
            const string Declaration = "ListAsync";

            FileObject results = new FileObject();

            try
            {
                string key;
                ListObjectsV2Response response;
                ListObjectsV2Request request;
                foreach(string folder in folders)
                {
                    key = Key(bucketName, folder);

                    request = new ListObjectsV2Request
                    {
                        BucketName = bucketName,
                        Prefix = folder,
                        MaxKeys = 50
                    };
                    do
                    {
                        response = await Service.ListObjectsV2Async(request);

                        FileObject found;
                        FileObject temp;
                        string[] split;
                        string[] split2;
                        foreach (S3Object entry in response.S3Objects)
                        {
                            if (entry.Key.EndsWith(SeparatorSlash.ToString()))
                                continue;

                            temp = results;
                            split = entry.Key.Split(SeparatorSlash);
                            foreach (string item in split)
                            {
                                found = temp.Files.Where(l => l.Name.EqualsIgnore(item)).FirstOrDefault();
                                if (found == null)
                                {
                                    found = new FileObject();
                                    found.Name = item;

                                    split2 = item.Split('.');
                                    if (split2.Length > 1)
                                    {
                                        found.Name = split2[0];
                                        found.Extension = split2[1];
                                    }
                                    
                                    temp.Files.Add(found);
                                }

                                temp = found;
                            }
                        }

                        request.ContinuationToken = response.NextContinuationToken;
                    }
                    while (response.IsTruncated);
                }
            }
            catch (Exception ex)
            {
                Logger?.LogError(Declaration, ex);
                throw;
            }

            return results;
        }

        public async Task<FileObject> ListFolderAsync(string bucketName, string folder)
        {
            const string Declaration = "ListAsync";

            FileObject results = new FileObject();

            try
            {
                string key = Key(bucketName, folder);

                //IDisposable lockResult = null;
                try
                {
                    //lockResult = await _lock.ReaderLockAsync();

                    if (_cache.ContainsKey(key))
                        return _cache[key];
                }
                finally
                {
                    //if (lockResult != null)
                    //    lockResult.Dispose();
                }

                try
                {
                    //lockResult = await _lock.WriterLockAsync();
                    using (await _mutex.LockAsync())
                    {
                        if (_cache.ContainsKey(key))
                            return _cache[key];

                        ListObjectsV2Response response;
                        ListObjectsV2Request request = new ListObjectsV2Request
                        {
                            BucketName = bucketName,
                            Prefix = folder,
                            MaxKeys = 50
                        };
                        do
                        {
                            response = await Service.ListObjectsV2Async(request);

                            FileObject found;
                            string[] split;
                            string[] split2;
                            foreach (S3Object entry in response.S3Objects)
                            {
                                if (entry.Key.EndsWith(SeparatorSlash.ToString()))
                                    continue;

                                found = new FileObject();
                                found.Url = entry.Key;

                                split = entry.Key.Split(SeparatorSlash);
                                split2 = split[split.Length - 1].Split(SeparatorExtension);
                                found.Name = split2[0];
                                if (split2.Length > 1)
                                    found.Extension = split2[1];

                                results.Files.Add(found);
                            }

                            request.ContinuationToken = response.NextContinuationToken;
                        }
                        while (response.IsTruncated);

                        if (!_cache.ContainsKey(key))
                            _cache.Add(key, results);
                    }
                }
                finally
                {
                    //if (lockResult != null)
                    //    lockResult.Dispose();
                }
            }
            catch (Exception ex)
            {
                Logger?.LogError(Declaration, ex);
                throw;
            }

            return results;
        }

        #region Private Methods
        private string Key(string bucket, string folder)
        {
            return string.Concat(bucket, SeparatorUnderscore, folder);
        }
        #endregion

        #region Private Properties
        private IAmazonS3 Service { get; set; }
        #endregion

        #region Fields
        private static readonly IDictionary<string, FileObject> _cache = new Dictionary<string, FileObject>();
        private readonly AsyncReaderWriterLock _lock = new AsyncReaderWriterLock();
        private readonly AsyncLock _mutex = new AsyncLock();
        #endregion

        #region Constants
        private const char SeparatorExtension = '.';
        private const char SeparatorSlash = '/';
        private const string SeparatorUnderscore = "_";
        #endregion
    }
}
