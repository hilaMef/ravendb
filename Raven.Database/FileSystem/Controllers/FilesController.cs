﻿using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Database.FileSystem.Actions;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Storage.Exceptions;
using Raven.Database.FileSystem.Util;
using Raven.Database.Plugins;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.FileSystem.Controllers
{
    public class FilesController : RavenFsApiController
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/files")]
		public HttpResponseMessage Get([FromUri] string[] fileNames)
		{
			var list = new List<FileHeader>();

			var startsWith = GetQueryStringValue("startsWith");
			if (string.IsNullOrEmpty(startsWith) == false)
			{
				var matches = GetQueryStringValue("matches");

				var endsWithSlash = startsWith.EndsWith("/") || startsWith.EndsWith("\\");
				startsWith = FileHeader.Canonize(startsWith);
				if (endsWithSlash) 
					startsWith += "/";

				Storage.Batch(accessor =>
				{
					var actualStart = 0;
					var filesToSkip = Paging.Start;
					int fileCount, matchedFiles = 0, addedFiles = 0;

					do
					{
						fileCount = 0;
						
						foreach (var file in accessor.GetFilesStartingWith(startsWith, actualStart, Paging.PageSize))
						{
							fileCount++;

							var keyTest = file.FullPath.Substring(startsWith.Length);

							if (WildcardMatcher.Matches(matches, keyTest) == false)
								continue;

                            if (FileSystem.ReadTriggers.CanReadFile(FileHeader.Canonize(file.FullPath), file.Metadata, ReadOperation.Load) == false) 
                                continue;

							if (file.Metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
							{
								continue;
							}

							matchedFiles++;

							if (matchedFiles <= filesToSkip)
								continue;

							list.Add(file);
							addedFiles++;
						}

						actualStart += Paging.PageSize;
					}
					while (fileCount > 0 && addedFiles < Paging.PageSize && actualStart > 0 && actualStart < int.MaxValue);
				});
			}
			else
			{
				if (fileNames != null && fileNames.Length > 0)
				{
					Storage.Batch(accessor =>
					{
						foreach (var path in fileNames.Where(x => x != null).Select(FileHeader.Canonize))
						{
							var file = accessor.ReadFile(path);

							if (file == null || file.Metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
							{
								list.Add(null);
								continue;
							}

							list.Add(file);
						}
					});
				}
				else
				{
					int results;
					var keys = Search.Query(null, null, Paging.Start, Paging.PageSize, out results);

					Storage.Batch(accessor => list.AddRange(keys.Select(accessor.ReadFile).Where(x => x != null)));
				}
			}

			return GetMessageWithObject(list)
				.WithNoCache();
		}

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
        public HttpResponseMessage Get(string name)
		{
            name = FileHeader.Canonize(name);
			FileAndPagesInformation fileAndPages = null;
			
			Storage.Batch(accessor => fileAndPages = accessor.GetFile(name, 0, 0));

            if (fileAndPages.Metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
			{
				log.Debug("File '{0}' is not accessible to get (Raven-Delete-Marker set)", name);
				throw new HttpResponseException(HttpStatusCode.NotFound);
			}

            var readingStream = StorageStream.Reading(Storage, name);

            var filename = Path.GetFileName(name);
            var result = StreamResult(filename, readingStream);

            var etag = new Etag(fileAndPages.Metadata.Value<string>(Constants.MetadataEtagField));
            fileAndPages.Metadata.Remove(Constants.MetadataEtagField);
            WriteHeaders(fileAndPages.Metadata, etag, result);

            log.Debug("File '{0}' with etag {1} is being retrieved.", name, etag);

            return result.WithNoCache();
		}

		[HttpDelete]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Delete(string name)
		{
            name = FileHeader.Canonize(name);

			Storage.Batch(accessor =>
			{
				Synchronizations.AssertFileIsNotBeingSynced(name);

				var fileAndPages = accessor.GetFile(name, 0, 0);

				var metadata = fileAndPages.Metadata;

				if(metadata == null)
					throw new FileNotFoundException();

				if (metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
					throw new FileNotFoundException();

				Files.IndicateFileToDelete(name, GetEtag());

				if (!name.EndsWith(RavenFileNameHelper.DownloadingFileSuffix)) // don't create a tombstone for .downloading file
				{
					Files.PutTombstone(name, metadata);
					accessor.DeleteConfig(RavenFileNameHelper.ConflictConfigNameForFile(name)); // delete conflict item too
				}
			});

			SynchronizationTask.Context.NotifyAboutWork();

            return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpHead]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Head(string name)
		{
            name = FileHeader.Canonize(name);
			FileAndPagesInformation fileAndPages = null;
			
			Storage.Batch(accessor => fileAndPages = accessor.GetFile(name, 0, 0));

			if (fileAndPages.Metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
			{
				log.Debug("Cannot get metadata of a file '{0}' because file was deleted", name);
				throw new FileNotFoundException();
			}
            
			var httpResponseMessage = GetEmptyMessage();

            var etag = new Etag(fileAndPages.Metadata.Value<string>(Constants.MetadataEtagField));
            fileAndPages.Metadata.Remove(Constants.MetadataEtagField);

            WriteHeaders(fileAndPages.Metadata, etag, httpResponseMessage);

			return httpResponseMessage;
		}

		[HttpPost]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Post(string name)
		{
            name = FileHeader.Canonize(name);

            var metadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);
			var etag = GetEtag();

			Storage.Batch(accessor =>
			{
				Synchronizations.AssertFileIsNotBeingSynced(name);

				Historian.Update(name, metadata);
				Files.UpdateMetadata(name, metadata, etag);

				SynchronizationTask.Context.NotifyAboutWork();
			});

			//Hack needed by jquery on the client side. We need to find a better solution for this
            return GetEmptyMessage(HttpStatusCode.NoContent);
		}

	    [HttpPatch]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
		public HttpResponseMessage Patch(string name, string rename)
		{
            name = FileHeader.Canonize(name);
            rename = FileHeader.Canonize(rename);
		    var etag = GetEtag();

		    if (rename.Length > SystemParameters.KeyMost)
		    {
				Log.Debug("File '{0}' was not renamed to '{1}' due to illegal name length", name, rename);
				return GetMessageWithString(string.Format("File '{0}' was not renamed to '{1}' due to illegal name length", name, rename),HttpStatusCode.BadRequest);
		    }

			Storage.Batch(accessor =>
			{
				FileSystem.Synchronizations.AssertFileIsNotBeingSynced(name);

				var existingFile = accessor.ReadFile(name);
				if (existingFile == null || existingFile.Metadata.Value<bool>(SynchronizationConstants.RavenDeleteMarker))
					throw new FileNotFoundException();

				var renamingFile = accessor.ReadFile(rename);
				if (renamingFile != null && renamingFile.Metadata.Value<bool>(SynchronizationConstants.RavenDeleteMarker) == false)
					throw new FileExistsException("Cannot rename because file " + rename + " already exists");

				var metadata = existingFile.Metadata;

				if (etag != null && existingFile.Etag != etag)
					throw new ConcurrencyException("Operation attempted on file '" + name + "' using a non current etag")
					{
						ActualETag = existingFile.Etag,
						ExpectedETag = etag
					};

				Historian.UpdateLastModified(metadata);

				var operation = new RenameFileOperation
				{
					FileSystem = FileSystem.Name,
					Name = name,
					Rename = rename,
					MetadataAfterOperation = metadata
				};

				accessor.SetConfig(RavenFileNameHelper.RenameOperationConfigNameForFile(name), JsonExtensions.ToJObject(operation));
				accessor.PulseTransaction(); // commit rename operation config

				Files.ExecuteRenameOperation(operation);
			});

			Log.Debug("File '{0}' was renamed to '{1}'", name, rename);

			SynchronizationTask.Context.NotifyAboutWork();

            return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpPut]
        [RavenRoute("fs/{fileSystemName}/files/{*name}")]
		public async Task<HttpResponseMessage> Put(string name, bool preserveTimestamps = false)
		{
			var metadata = GetFilteredMetadataFromHeaders(ReadInnerHeaders);
			var etag = GetEtag();

			if (name.Length > SystemParameters.KeyMost)
			{
				Log.Debug("File '{0}' was not created due to illegal name length", name);
				return GetMessageWithString(string.Format("File '{0}' was not created due to illegal name length", name), HttpStatusCode.BadRequest);
			}

			var options = new FileActions.PutOperationOptions();

			long contentSize;
			if (long.TryParse(GetHeader(Constants.FileSystem.RavenFsSize), out contentSize))
				options.ContentSize = contentSize;

			DateTimeOffset lastModified;
			if (DateTimeOffset.TryParse(GetHeader(Constants.RavenLastModified), out lastModified))
				options.LastModified = lastModified;

			options.PreserveTimestamps = preserveTimestamps;
			options.ContentLength = Request.Content.Headers.ContentLength;
			options.TransferEncodingChunked = Request.Headers.TransferEncodingChunked ?? false;

			await FileSystem.Files.PutAsync(name, etag, metadata, () => Request.Content.ReadAsStreamAsync(), options);

			SynchronizationTask.Context.NotifyAboutWork();

			return GetEmptyMessage(HttpStatusCode.Created);
		}
	}
}
