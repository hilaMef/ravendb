﻿using System;
using System.Net;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.FileSystem;
using Raven.Json.Linq;

namespace Raven.Backup
{
	public class FilesystemBackupOperation : AbstractBackupOperation
    {
        private FilesStore store;

        public FilesystemBackupOperation(BackupParameters parameters) : base(parameters)
        {
        }

        public override bool InitBackup()
        {
            parameters.ServerUrl = parameters.ServerUrl.TrimEnd('/');
            try //precaution - to show error properly just in case
            {
                var serverUri = new Uri(parameters.ServerUrl);
                var serverHostname = serverUri.Scheme + Uri.SchemeDelimiter + serverUri.Host + ":" + serverUri.Port;

                store = new FilesStore { Url = serverHostname, DefaultFileSystem = parameters.Filesystem, ApiKey = parameters.ApiKey };
                store.Initialize();
            }
            catch (Exception exc)
            {
                Console.WriteLine(exc.Message);
                try
                {
                    store.Dispose();
                }
                // ReSharper disable once EmptyGeneralCatchClause
                catch (Exception) { }
                return false;
            }


            var backupRequest = new
            {
                BackupLocation = parameters.BackupPath.Replace("\\", "\\\\"),
            };


            var json = RavenJObject.FromObject(backupRequest).ToString();

            var url = "/admin/backup";
            if (parameters.Incremental)
                url += "?incremental=true";
            try
            {
	            using (var req = CreateRequest("/fs/" + parameters.Filesystem + url, "POST"))
	            {
					req.WriteAsync(json).Wait();

					Console.WriteLine("Sending json {0} to {1}", json, parameters.ServerUrl);

					var response = req.ReadResponseJson();
					Console.WriteLine(response);
	            }  
            }
            catch (Exception exc)
            {
                Console.WriteLine(exc.Message);
                return false;
            }

            return true;
        }

        protected override HttpJsonRequest CreateRequest(string url, string method)
        {
            var uriString = parameters.ServerUrl + url;
			return store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, uriString, method, new OperationCredentials(parameters.ApiKey, CredentialCache.DefaultCredentials), store.Conventions, parameters.Timeout.HasValue ? TimeSpan.FromMilliseconds(parameters.Timeout.Value) : (TimeSpan?)null));
        }

        public override BackupStatus GetStatusDoc()
        {
	        try
	        {
		        var backupStatus = AsyncHelpers.RunSync(() => store.AsyncFilesCommands.Configuration.GetKeyAsync<BackupStatus>(BackupStatus.RavenBackupStatusDocumentKey));

		        return backupStatus;
	        }
	        catch (Exception ex)
	        {
		        throw new Exception("Network error", ex);
	        }
        }

        public override void Dispose()
        {
            var _store = store;
            if (_store != null)
                _store.Dispose();
        }
    }
}