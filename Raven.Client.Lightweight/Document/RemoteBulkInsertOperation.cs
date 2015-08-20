﻿using System.Diagnostics;
using System.Net;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Json;
using Raven.Client.Connection.Async;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.Util;
using Raven.Imports.Newtonsoft.Json;

using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;
using System.IO.Compression;
using Raven.Client.Extensions;

namespace Raven.Client.Document
{

    public interface ILowLevelBulkInsertOperation : IDisposable
    {
        Guid OperationId { get; }

        bool IsAborted { get; }

        void Write(string id, RavenJObject metadata, RavenJObject data, int? dataSize = null);

        Task<int> DisposeAsync();

        /// <summary>
        ///     Report on the progress of the operation
        /// </summary>
        event Action<string> Report;
        void Abort();
    }

    public class RemoteBulkInsertOperation : ILowLevelBulkInsertOperation, IObserver<BulkInsertChangeNotification>
    {
        private readonly BulkInsertOptions options;
	    private readonly Task<int> previousTask;
	    private CancellationTokenSource cancellationTokenSource;
        private readonly AsyncServerClient operationClient;
        private readonly MemoryStream bufferedStream = new MemoryStream();
        private readonly BlockingCollection<RavenJObject> queue;
        private static readonly RavenJObject AbortMarker = new RavenJObject();
        private static readonly RavenJObject SkipMarker = new RavenJObject();
        private HttpJsonRequest operationRequest;
        private readonly Task operationTask;
	    private bool aborted;
	    private bool waitedForPreviousTask;
	    private readonly Stopwatch _timing = Stopwatch.StartNew();
        private const int BigDocumentSize = 64 * 1024;

        public RemoteBulkInsertOperation(BulkInsertOptions options, AsyncServerClient client, IDatabaseChanges changes, 
			Task<int> previousTask = null, Guid? existingOperationId = null)
        {
            this.options = options;
	        this.previousTask = previousTask;
	        using (NoSynchronizationContext.Scope())
            {
				OperationId = existingOperationId.HasValue?existingOperationId.Value:Guid.NewGuid();
                operationClient = client;
                queue = new BlockingCollection<RavenJObject>(Math.Max(128, (options.BatchSize * 3) / 2));

                operationTask = StartBulkInsertAsync(options);

#if !MONO
                SubscribeToBulkInsertNotifications(changes);
#endif
            }
        }

	    public int Total { get;set; }
	    public int localCount;
	    public long size;

#if !MONO
        private void SubscribeToBulkInsertNotifications(IDatabaseChanges changes)
        {
            subscription = changes
                .ForBulkInsert(OperationId)
                .Subscribe(this);
        }
#endif

        private async Task StartBulkInsertAsync(BulkInsertOptions options)
        {
            using (ConnectionOptions.Expect100Continue(operationClient.Url))
            {
                var operationUrl = CreateOperationUrl(options);
                var token = await GetToken().ConfigureAwait(false);
                try
                {
                    token = await ValidateThatWeCanUseAuthenticateTokens(token).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Could not authenticate token for bulk insert, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration", e);
                }

	            using (operationRequest = CreateOperationRequest(operationUrl, token))
	            {
		            var cancellationToken = CreateCancellationToken();
		            var response = await operationRequest.ExecuteRawRequestAsync((stream, source) => Task.Factory.StartNew(() =>
		            {
			            try
			            {
				            WriteQueueToServer(stream, options, cancellationToken);
				            var x = source.TrySetResult(null);
			            }
			            catch (Exception e)
			            {
				            source.TrySetException(e);
			            }
		            }, TaskCreationOptions.LongRunning)).ConfigureAwait(false);

		            await response.AssertNotFailingResponse();

		            long operationId;

		            using (response)
		            using (var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
		            using (var streamReader = new StreamReader(stream))
		            {
			            var result = RavenJObject.Load(new JsonTextReader(streamReader));
			            operationId = result.Value<long>("OperationId");
		            }

		            if (await IsOperationCompleted(operationId).ConfigureAwait(false)) responseOperationId = operationId;
	            }
            }
        }

        private CancellationToken CreateCancellationToken()
        {
            cancellationTokenSource = new CancellationTokenSource();
            return cancellationTokenSource.Token;
        }

        private async Task<string> GetToken()
        {
            // this will force the HTTP layer to authenticate, meaning that our next request won't have to
            var jsonToken = await GetAuthToken().ConfigureAwait(false);

            return jsonToken.Value<string>("Token");
        }

        private async Task<RavenJToken> GetAuthToken()
        {
	        using (var request = operationClient.CreateRequest("/singleAuthToken", "GET", disableRequestCompression: true))
			{
				return await request.ReadResponseJsonAsync().ConfigureAwait(false);
	        }
        }

        private async Task<string> ValidateThatWeCanUseAuthenticateTokens(string token)
        {
	        using (var request = operationClient.CreateRequest("/singleAuthToken", "GET", disableRequestCompression: true, disableAuthentication: true))
	        {
		        request.AddOperationHeader("Single-Use-Auth-Token", token);
		        var result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
		        return result.Value<string>("Token");
	        }
        }

        private HttpJsonRequest CreateOperationRequest(string operationUrl, string token)
        {
			// the request may take a long time to process, so we need to set a large timeout value
			var request = operationClient.CreateRequest(operationUrl, "POST", disableRequestCompression: true, disableAuthentication: true, timeout: TimeSpan.FromHours(6));
            request.AddOperationHeader("Single-Use-Auth-Token", token);

            return request;
        }

        private string CreateOperationUrl(BulkInsertOptions options)
        {
            string requestUrl = "/bulkInsert?";
            if (options.OverwriteExisting)
                requestUrl += "overwriteExisting=true";
            if (options.CheckReferencesInIndexes)
                requestUrl += "&checkReferencesInIndexes=true";
			if(options.SkipOverwriteIfUnchanged)
				requestUrl += "&skipOverwriteIfUnchanged=true";

            requestUrl += "&operationId=" + OperationId;

            return requestUrl;
        }

        private void WriteQueueToServer(Stream stream, BulkInsertOptions options, CancellationToken cancellationToken)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var batch = new List<RavenJObject>();
                RavenJObject document;
                while (queue.TryTake(out document, millisecondsTimeout: 200))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (document == null) // marker
                    {
                        FlushBatch(stream, batch);
                        return;
                    }
                    if (ReferenceEquals(SkipMarker, document)) // ignore this, just filling the queue
                    {
                        continue;
                    }
                    if (ReferenceEquals(AbortMarker, document)) // abort immediately
                    {
                        return;
                    }

                    batch.Add(document);

                    if (batch.Count >= options.BatchSize)
                        break;
                }

                FlushBatch(stream, batch);
            }
        }

        public event Action<string> Report;

        public Guid OperationId { get; private set; }

        public virtual void Write(string id, RavenJObject metadata, RavenJObject data, int? dataSize = null)
        {
            if (id == null) throw new ArgumentNullException("id");
            if (metadata == null) throw new ArgumentNullException("metadata");
            if (data == null) throw new ArgumentNullException("data");
            if (aborted) throw new InvalidOperationException("Operation has been aborted");


            metadata["@id"] = id;
            data[Constants.Metadata] = metadata;

            for (int i = 0; i < 2; i++)
            {
                if (operationTask.IsCanceled || operationTask.IsFaulted)
                    operationTask.Wait(); // error early if we have  any error

                if (queue.TryAdd(data, options.WriteTimeoutMilliseconds / 2))
                {
                    if (dataSize != null && dataSize >= BigDocumentSize)
                    {
                        //essentially for a BatchSize == 1024 and stream of 1MB documents - the actual batch size will be 128
                        // --> BatchSize = 1024 / (dataSize = 1024/BigDocumentSize = 250) * 2 == 128
                        for (int skipDocIndex = 0; skipDocIndex < (dataSize / BigDocumentSize) * 2; skipDocIndex++)
                        {
                            if (!queue.TryAdd(SkipMarker)) //if queue is full just stop adding dummy docs
                                break;
                        }
                    }
                    return;
                }
            }

            if (operationTask.IsCanceled || operationTask.IsFaulted)
                operationTask.Wait(); // error early if we have  any error

            throw new TimeoutException("Could not flush in the specified timeout, server probably not responding or responding too slowly.\r\nAre you writing very big documents?");
        }

        private async Task<bool> IsOperationCompleted(long operationId)
        {
            ErrorResponseException errorResponse;

            try
            {
                var status = await GetOperationStatus(operationId);

                if (status == null) return true;

                if (status.Value<bool>("Completed"))
                    return true;

                return false;
            }
            catch (ErrorResponseException e)
            {
                if (e.StatusCode != HttpStatusCode.Conflict)
                    throw;

                errorResponse = e;
            }

            var conflictsDocument = RavenJObject.Load(new RavenJsonTextReader(new StringReader(errorResponse.ResponseString)));

            throw new ConcurrencyException(conflictsDocument.Value<string>("Error"));
        }

        private Task<RavenJToken> GetOperationStatus(long operationId)
        {
            return operationClient.GetOperationStatusAsync(operationId);
        }

        private volatile bool disposed;
        private IDisposable subscription;

        private long responseOperationId;

        public async Task<int> DisposeAsync()
        {
	        if (disposed)
		        return -1;
            disposed = true;
            queue.Add(null);
            if (subscription != null)
            {
                subscription.Dispose();

            }

            // The first await call in this method MUST call ConfigureAwait(false) in order to avoid DEADLOCK when this code is called by synchronize code, like Dispose().
            try
            {
                await operationTask.ConfigureAwait(false);
                operationTask.AssertNotFailed();

	            if (previousTask == null)
		            ReportInternal("Finished writing all results to server");

                while (true)
                {
                    if (await IsOperationCompleted(responseOperationId))
                        break;

	                await Task.Delay(100);
                }
				if (previousTask == null)
	            {
		            ReportInternal("Done writing to server");
	            }
	            else
	            {
					ReportInternal("Wrote {0:#,#} [{3:#,#;;0} kb] (total {2:#,#;;0}) documents to server gzipped to {1:#,#;;0} kb in {4:#,#.#;;0} sec.",
					   localCount,
					   bufferedStream.Position / 1024d,
					   Total,
					   size / 1024d,
					   _timing.Elapsed.TotalSeconds);   
	            }
            }
            catch (Exception e)
            {
                ReportInternal("Failed to write all results to a server, probably something happened to the server. Exception : {0}", e);
                if (e.Message.Contains("Raven.Abstractions.Exceptions.ConcurrencyException"))
                    throw new ConcurrencyException("ConcurrencyException while writing bulk insert items in the server. Did you run bulk insert operation with OverwriteExisting == false?. Exception returned from server: " + e.Message, e);
                throw;
            }
	        return Total;
        }

        public void Dispose()
        {
            if (disposed)
                return;

            using (NoSynchronizationContext.Scope())
            {
                var disposeAsync = DisposeAsync().ConfigureAwait(false);
                disposeAsync.GetAwaiter().GetResult();
            }
        }

        private void FlushBatch(Stream requestStream, ICollection<RavenJObject> localBatch)
        {
	        if (localBatch.Count == 0)
		        return;
	        if (aborted) throw new InvalidOperationException("Operation was timed out or has been aborted");

			if (previousTask != null && waitedForPreviousTask == false)
	        {
		        Total += previousTask.Result;
		        waitedForPreviousTask = true;
	        }

	        bufferedStream.SetLength(0);
	        long bytesWrittenToServer;
	        WriteToBuffer(localBatch, out bytesWrittenToServer);

	        var requestBinaryWriter = new BinaryWriter(requestStream);
	        requestBinaryWriter.Write((int) bufferedStream.Position);
	        var sp = Stopwatch.StartNew();
	        bufferedStream.WriteTo(requestStream);
	        requestStream.Flush();

	        Total += localBatch.Count;
	        localCount += localBatch.Count;
	        size += bytesWrittenToServer;
			if (previousTask == null)
	        {
		        ReportInternal("Wrote {0:#,#} [{3:#,#;;0} kb] (total {2:#,#;;0}) documents to server gzipped to {1:#,#;;0} kb in {4:#,#.#;;0} sec.",
			        localBatch.Count,
			        bufferedStream.Position/1024d,
			        Total,
			        bytesWrittenToServer/1024d,
			        sp.Elapsed.TotalSeconds);
	        }
        }

	    private void WriteToBuffer(ICollection<RavenJObject> localBatch, out long bytesWritten)
        {
			using (var gzip = new GZipStream(bufferedStream, CompressionMode.Compress, leaveOpen: true))
			using (var stream = new CountingStream(gzip))
            {
                var binaryWriter = new BinaryWriter(stream);
                binaryWriter.Write(localBatch.Count);
                var bsonWriter = new BsonWriter(binaryWriter)
                                 {
                                     DateTimeKindHandling = DateTimeKind.Unspecified
                                 };

                foreach (var doc in localBatch)
                {
                    doc.WriteTo(bsonWriter);
                }

                bsonWriter.Flush();
                binaryWriter.Flush();
                stream.Flush();
	            bytesWritten = stream.NumberOfWrittenBytes;
            }
        }

        private void ReportInternal(string format, params object[] args)
        {
            var onReport = Report;
            if (onReport != null)
                onReport(string.Format(format, args));
        }

        public void OnNext(BulkInsertChangeNotification value)
        {
            if (value.Type == DocumentChangeTypes.BulkInsertError)
            {
                cancellationTokenSource.Cancel();
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        public void Abort()
        {
            aborted = true;
            queue.Add(AbortMarker);
        }


        public bool IsAborted
        {
            get { return aborted; }
        }
    }
}
