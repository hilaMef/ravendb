#if !DNXCORE50
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Database.Data;

namespace Raven.Abstractions.Smuggler
{
    public abstract class SmugglerDatabaseApiBase : ISmugglerApi<RavenConnectionStringOptions, SmugglerDatabaseOptions, OperationState>
    {
        const int RetriesCount = 5;

        public ISmugglerDatabaseOperations Operations { get; protected set; }

        public SmugglerDatabaseOptions Options { get; private set; }

        private const string IncrementalExportStateFile = "IncrementalExport.state.json";

        protected SmugglerDatabaseApiBase(SmugglerDatabaseOptions options)
        {
            Options = options;
        }

        protected bool IgnoreErrorsAndContinue
        {
            get
            {
                return Options != null && Options.IgnoreErrorsAndContinue;
            }
        }

        public virtual async Task<OperationState> ExportData(SmugglerExportOptions<RavenConnectionStringOptions> exportOptions)
        {
            if (exportOptions.IsIncrementalExport == true)
                Options.Incremental = true;

            Operations.Configure(Options);
            Operations.Initialize(Options);

            var result = new OperationState
            {
                FilePath = exportOptions.ToFile,
                LastAttachmentsEtag = Options.StartAttachmentsEtag,
                LastDocsEtag = Options.StartDocsEtag,
                LastDocDeleteEtag = Options.StartDocsDeletionEtag,
                LastAttachmentsDeleteEtag = Options.StartAttachmentsDeletionEtag
            };

            if (Options.Incremental)
            {
                if (Directory.Exists(result.FilePath) == false)
                {
                    if (File.Exists(result.FilePath))
                        result.FilePath = Path.GetDirectoryName(result.FilePath) ?? result.FilePath;
                    else
                        Directory.CreateDirectory(result.FilePath);
                }

                if (Options.StartDocsEtag == Etag.Empty && Options.StartAttachmentsEtag == Etag.Empty)
                {
                    ReadLastEtagsFromFile(result);
                }

                result.FilePath = Path.Combine(result.FilePath, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-0", CultureInfo.InvariantCulture) + ".ravendb-incremental-dump");
                if (File.Exists(result.FilePath))
                {
                    var counter = 1;
                    while (true)
                    {
                        result.FilePath = Path.Combine(Path.GetDirectoryName(result.FilePath), SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + "-" + counter + ".ravendb-incremental-dump");

                        if (File.Exists(result.FilePath) == false)
                            break;
                        counter++;
                    }
                }
            }

            SmugglerExportException lastException = null;

            bool ownedStream = exportOptions.ToStream == null;
            var stream = exportOptions.ToStream ?? File.Create(result.FilePath);

            try
            {
                SupportedFeatures = await DetectServerSupportedFeatures(Operations, exportOptions.From).ConfigureAwait(false);
            }
            catch (WebException e)
            {
                Operations.ShowProgress("Failed to query server for supported features. Reason : " + e.Message);
                SupportedFeatures = GetLegacyModeFeatures(); //could not detect supported features, then run in legacy mode
                //				lastException = new SmugglerExportException
                //				{
                //					LastEtag = Etag.Empty,
                //					File = ownedStream ? result.FilePath : null
                //				};
            }

            var maxSplitExportFileSize = !ownedStream ? 0 : Options.MaxSplitExportFileSize;

            if (exportOptions.MaxSplitExportFileSize > 0)
                maxSplitExportFileSize = exportOptions.MaxSplitExportFileSize;

            try
            {
                using (var countingStream = new CountingStream(stream))
                using (var gZipStream = new GZipStream(countingStream, CompressionMode.Compress, leaveOpen: true))
                using (var streamWriter = new StreamWriter(gZipStream))

                using (var jsonWriter = new SmugglerJsonTextWriter(streamWriter, maxSplitExportFileSize, countingStream, result.FilePath)
                {
                        Formatting = Formatting.Indented
                })
                {
                    jsonWriter.WriteStartObject();
                    jsonWriter.WritePropertyName("Indexes");
                    jsonWriter.WriteStartArray();
                    if (Options.OperateOnTypes.HasFlag(ItemType.Indexes))
                    {
                        await ExportIndexes(exportOptions.From, jsonWriter).ConfigureAwait(false);
                    }
                    jsonWriter.WriteEndArray();

                    var lastDocDeleteEtag = Operations.FetchLastDocDeleteEtag();
                    jsonWriter.WritePropertyName("Docs");
                    jsonWriter.WriteStartArray();
                    if (Options.OperateOnTypes.HasFlag(ItemType.Documents))
                    {
                        try
                        {
                            result.LastDocsEtag = await ExportDocuments(exportOptions.From, jsonWriter, 
                                result.LastDocsEtag, 
                                updateLastDocDeleteEtag: () => lastDocDeleteEtag = Operations.FetchLastDocDeleteEtag()).ConfigureAwait(false);
                        }
                        catch (SmugglerExportException e)
                        {
                            result.LastDocsEtag = e.LastEtag;
                            e.File = ownedStream ? result.FilePath : null;
                            lastException = e;
                        }
                    }
                    jsonWriter.WriteEndArray();

                    var lastAttachmentsDeleteEtag = Operations.FetchLastAttachmentsDeleteEtag();
                    jsonWriter.WritePropertyName("Attachments");
                    jsonWriter.WriteStartArray();
                    if (Options.OperateOnTypes.HasFlag(ItemType.Attachments) && lastException == null)
                    {
                        try
                        {
                            result.LastAttachmentsEtag = await ExportAttachments(exportOptions.From, jsonWriter, 
                                result.LastAttachmentsEtag,
                                updateLastAttachmentsDeleteEtag: () => lastAttachmentsDeleteEtag = Operations.FetchLastAttachmentsDeleteEtag()).ConfigureAwait(false);
                        }
                        catch (SmugglerExportException e)
                        {
                            result.LastAttachmentsEtag = e.LastEtag;
                            e.File = ownedStream ? result.FilePath : null;
                            lastException = e;
                        }
                    }
                    jsonWriter.WriteEndArray();

                    jsonWriter.WritePropertyName("Transformers");
                    jsonWriter.WriteStartArray();
                    if (Options.OperateOnTypes.HasFlag(ItemType.Transformers) && lastException == null)
                    {
                        await ExportTransformers(exportOptions.From, jsonWriter).ConfigureAwait(false);
                    }
                    jsonWriter.WriteEndArray();

                    if (Options.ExportDeletions)
                    {
                        var maxEtagsToFetch = new LastEtagsInfo
                        {
                            LastDocDeleteEtag = lastDocDeleteEtag,
                            LastAttachmentsDeleteEtag = lastAttachmentsDeleteEtag
                        };
                        await ExportDeletions(jsonWriter, result, maxEtagsToFetch).ConfigureAwait(false);
                    }

                    await ExportIdentities(jsonWriter, Options.OperateOnTypes).ConfigureAwait(false);

                    jsonWriter.WriteEndObject();
                    streamWriter.Flush();
                }

                if (Options.Incremental)
                    WriteLastEtagsToFile(result, result.FilePath, IncrementalExportStateFile);

                if (Options.ExportDeletions)
                {
                    Operations.PurgeTombstones(result);
                }

                if (lastException != null)
                    throw lastException;

                return result;
            }
            finally
            {
                if (ownedStream && stream != null)
                    stream.Dispose();
            }
        }

        private async Task ExportIdentities(SmugglerJsonTextWriter jsonWriter, ItemType operateOnTypes)
        {
            var retries = RetriesCount;

            Operations.ShowProgress("Exporting Identities");

            while (true)
            {
                List<KeyValuePair<string, long>> identities;
                try
                {
                    identities = await Operations.GetIdentities().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (retries-- == 0 && IgnoreErrorsAndContinue)
                    {
                        Operations.ShowProgress("Failed to fetch identities too much times. Cancelling identities export. Message: {0}", e.Message);
                        return;
                    }

                    if (IgnoreErrorsAndContinue == false)
                        throw;

                    Operations.ShowProgress("Failed to fetch identities. {0} retries remaining. Message: {1}", retries, e.Message);
                    continue;
                }

                Operations.ShowProgress("Exported {0} following identities: {1}", identities.Count, string.Join(", ", identities.Select(x => x.Key)));

                var filteredIdentities = identities.Where(x => FilterIdentity(x.Key, operateOnTypes)).ToList();

                Operations.ShowProgress("After filtering {0} identities need to be exported: {1}", filteredIdentities.Count, string.Join(", ", filteredIdentities.Select(x => x.Key)));

                jsonWriter.WritePropertyName("Identities");
                jsonWriter.WriteStartArray();

                foreach (var identityInfo in filteredIdentities)
                {
                    try
                    {
                        var o = new RavenJObject
                        {
                            {"Key", identityInfo.Key},
                            {"Value", identityInfo.Value}
                        };
                        jsonWriter.Write(o);
                    }
                    catch (Exception e)
                    {
                        if (IgnoreErrorsAndContinue == false)
                            throw;

                        Operations.ShowProgress("Export of identity {0} failed. Message: {1}", identityInfo.Key, e.Message);
                    }
                }

                jsonWriter.WriteEndArray();

                Operations.ShowProgress("Done with exporting identities");
                return;
            }
        }

        public bool FilterIdentity(string identityName, ItemType operateOnTypes)
        {
            if ("Raven/Etag".Equals(identityName, StringComparison.OrdinalIgnoreCase))
                return false;

            if ("IndexId".Equals(identityName, StringComparison.OrdinalIgnoreCase))
                return false;

            if (Constants.RavenSubscriptionsPrefix.Equals(identityName, StringComparison.OrdinalIgnoreCase))
                return false;

            if (operateOnTypes.HasFlag(ItemType.Documents))
                return true;

            return false;
        }

        public static void ReadLastEtagsFromFile(OperationState result, string etagFileLocation)
        {
            var log = LogManager.GetCurrentClassLogger();            

            if (!File.Exists(etagFileLocation))
                return;

            using (var streamReader = new StreamReader(new FileStream(etagFileLocation, FileMode.Open)))
            using (var jsonReader = new JsonTextReader(streamReader))
            {
                RavenJObject ravenJObject;
                try
                {
                    ravenJObject = RavenJObject.Load(jsonReader);
                }
                catch (Exception e)
                {
                    log.WarnException("Could not parse etag document from file : " + etagFileLocation + ", ignoring, will start from scratch", e);
                    return;
                }
                result.LastDocsEtag = Etag.Parse(ravenJObject.Value<string>("LastDocEtag"));
                result.LastAttachmentsEtag = Etag.Parse(ravenJObject.Value<string>("LastAttachmentEtag"));
                result.LastDocDeleteEtag = Etag.Parse(ravenJObject.Value<string>("LastDocDeleteEtag") ?? Etag.Empty.ToString());
                result.LastAttachmentsDeleteEtag = Etag.Parse(ravenJObject.Value<string>("LastAttachmentsDeleteEtag") ?? Etag.Empty.ToString());
            }
        }

        public static void ReadLastEtagsFromFile(OperationState result)
        {            
            var etagFileLocation = Path.Combine(result.FilePath, IncrementalExportStateFile);
            ReadLastEtagsFromFile(result, etagFileLocation);
        }

        public static void WriteLastEtagsToFile(OperationState result, string etagFileLocation)
        {
            using (var streamWriter = new StreamWriter(File.Create(etagFileLocation)))
            {
                new RavenJObject
                    {
                        {"LastDocEtag", result.LastDocsEtag.ToString()},
                        {"LastAttachmentEtag", result.LastAttachmentsEtag.ToString()},
                        {"LastDocDeleteEtag", result.LastDocDeleteEtag.ToString()},
                        {"LastAttachmentsDeleteEtag", result.LastAttachmentsDeleteEtag.ToString()}
                    }.WriteTo(new JsonTextWriter(streamWriter));
                streamWriter.Flush();
            }
        }

        public static void WriteLastEtagsToFile(OperationState result, string backupPath, string filename)
        {
            // ReSharper disable once AssignNullToNotNullAttribute
            var etagFileLocation = Path.Combine(Path.GetDirectoryName(backupPath), filename);
            WriteLastEtagsToFile(result, etagFileLocation);
        }

        private async Task ExportTransformers(RavenConnectionStringOptions src, SmugglerJsonTextWriter jsonWriter)
        {
            var totalCount = 0;
            var retries = RetriesCount;

            while (true)
            {
                RavenJArray transformers;

                try
                {
                    transformers = await Operations.GetTransformers(totalCount).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (retries-- == 0 & IgnoreErrorsAndContinue)
                    {
                        Operations.ShowProgress("Failed getting transformers too much times, stopping the transformer export entirely. Message: {0}", e.Message);
                        return;
                    }

                    if (IgnoreErrorsAndContinue == false)
                        throw;

                    Operations.ShowProgress("Failed fetching transformer information from exporting store. {0} retries remaining. Message: {1}", retries, e.Message);
                    continue;
                }

                if (transformers.Length == 0)
                {
                    Operations.ShowProgress("Done with reading transformers, total: {0}", totalCount);
                    break;
                }

                totalCount += transformers.Length;
                Operations.ShowProgress("Reading batch of {0,3} transformers, read so far: {1,10:#,#;;0}", transformers.Length, totalCount);

                foreach (var transformer in transformers)
                {
                    try
                    {
                        jsonWriter.Write(transformer);
                    }
                    catch (Exception e)
                    {
                        if (IgnoreErrorsAndContinue == false)
                            throw;

                        Operations.ShowProgress("PUT of a transformer {0} failed. Message: {1}", transformer.Value<string>("name"), e.Message);
                    }
                }
            }
        }

        public abstract Task ExportDeletions(SmugglerJsonTextWriter jsonWriter, OperationState result, LastEtagsInfo maxEtagsToFetch);

        [Obsolete("Use RavenFS instead.")]
        protected virtual async Task<Etag> ExportAttachments(RavenConnectionStringOptions src, SmugglerJsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag = null, Action updateLastAttachmentsDeleteEtag = null)
        {
            var totalCount = 0;
            var retries = RetriesCount;
            var maxEtagReached = false;

            while (true)
            {
                try
                {
                    if (Options.Limit - totalCount <= 0 || maxEtagReached)
                    {
                        Operations.ShowProgress("Done with reading attachments, total: {0}", totalCount);
                        return lastEtag;
                    }

                    var maxRecords = Math.Min(Options.Limit - totalCount, Options.BatchSize);
                    List<AttachmentInformation> attachments;

                    try
                    {
                        if (Options.ExportDeletions && updateLastAttachmentsDeleteEtag != null)
                            updateLastAttachmentsDeleteEtag();

                        attachments = await Operations.GetAttachments(totalCount, lastEtag, maxRecords).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        if (retries-- == 0 && IgnoreErrorsAndContinue)
                            return Etag.InvalidEtag;

                        if (IgnoreErrorsAndContinue == false)
                            throw;

                        Operations.ShowProgress("Failed to get attachments. {0} retries remaining. Message: {1}", retries, e.Message);
                        continue;
                    }

                    if (attachments.Count == 0)
                    {
                        DatabaseStatistics databaseStatistics;
                        try
                        {
                            databaseStatistics = await Operations.GetStats().ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            if (retries-- == 0 && IgnoreErrorsAndContinue)
                                return Etag.Empty;

                            if (IgnoreErrorsAndContinue == false)
                                throw;

                            Operations.ShowProgress("Failed to get database statistics. Message: {0}", e.Message);
                            continue;
                        }

                        if (lastEtag == null) lastEtag = Etag.Empty;
                        if (lastEtag.CompareTo(databaseStatistics.LastAttachmentEtag) < 0)
                        {
                            lastEtag = EtagUtil.Increment(lastEtag, maxRecords);
                            Operations.ShowProgress("Got no results but didn't get to the last attachment etag, trying from: {0}",
                                         lastEtag);
                            continue;
                        }
                        Operations.ShowProgress("Done with reading attachments, total: {0}", totalCount);
                        return lastEtag;
                    }

                    totalCount += attachments.Count;
                    Operations.ShowProgress("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", attachments.Count, totalCount);
                    foreach (var attachmentInformation in attachments)
                    {
                        if (maxEtag != null && attachmentInformation.Etag.CompareTo(maxEtag) > 0)
                        {
                            maxEtagReached = true;
                            break;
                        }

                        Operations.ShowProgress("Downloading attachment: {0}", attachmentInformation.Key);

                        try
                        {
                            var attachmentData = await Operations.GetAttachmentData(attachmentInformation).ConfigureAwait(false);
                            if (attachmentData == null)
                                continue;
                            var ravenJsonObj = new RavenJObject
                            {
                                {"Data", attachmentData},
                                {"Metadata", attachmentInformation.Metadata},
                                {"Key", attachmentInformation.Key},
                                {"Etag", new RavenJValue(attachmentInformation.Etag.ToString())}
                            };
                            jsonWriter.Write(ravenJsonObj);

                            lastEtag = attachmentInformation.Etag;
                        }
                        catch (Exception e)
                        {
                            if (IgnoreErrorsAndContinue == false)
                                throw;

                            Operations.ShowProgress("EXPORT of an attachment {0} failed. Message: {1}", attachmentInformation.Key, e.Message);
                        }
                    }
                }
                catch (Exception e)
                {
                    Operations.ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
                    Operations.ShowProgress("Done with reading attachments, total: {0}", totalCount, lastEtag);
                    throw new SmugglerExportException(e.Message, e)
                    {
                        LastEtag = lastEtag,
                    };
                }
            }
        }

        protected async Task<Etag> ExportDocuments(RavenConnectionStringOptions src, SmugglerJsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag = null, Action updateLastDocDeleteEtag = null)
        {
            var now = SystemTime.UtcNow;
            var totalCount = 0;
            var lastReport = SystemTime.UtcNow;
            var reportInterval = TimeSpan.FromSeconds(2);
            var reachedMaxEtag = false;
            Operations.ShowProgress("Exporting Documents");
            int numberOfSkippedDocs = 0;
            var lastForcedFlush = SystemTime.UtcNow;
            var maxFlushInterval = TimeSpan.FromSeconds(1);

            while (true)
            {
                bool hasDocs = false;
                try
                {
                    var maxRecords = Options.Limit - totalCount;
                    if (maxRecords > 0 && reachedMaxEtag == false)
                    {
                        var amountToFetchFromServer = Math.Min(Options.BatchSize, maxRecords);
                        using (var documents = await Operations.GetDocuments(lastEtag, amountToFetchFromServer).ConfigureAwait(false))
                        {
                            while (await documents.MoveNextAsync().ConfigureAwait(false))
                            {
                                if (numberOfSkippedDocs > 0 && (SystemTime.UtcNow - lastForcedFlush) > maxFlushInterval)
                                {
                                    Operations.ShowProgress("Skipped {0:#,#} documents", numberOfSkippedDocs);
                                    var currentJsonTextWriter = jsonWriter.GetCurrentJsonTextWriter();
                                    currentJsonTextWriter.WriteWhitespace(" ");
                                    currentJsonTextWriter.Flush();
                                    lastForcedFlush = SystemTime.UtcNow;
                                    numberOfSkippedDocs = 0;
                                }

                                if (Options.ExportDeletions && updateLastDocDeleteEtag != null)
                                    updateLastDocDeleteEtag();

                                hasDocs = true;
                                var document = documents.Current;

                                var tempLastEtag = Etag.Parse(document.Value<RavenJObject>("@metadata").Value<string>("@etag"));

                                Debug.Assert(!string.IsNullOrWhiteSpace(document.Value<RavenJObject>("@metadata").Value<string>("@id")));

                                if (maxEtag != null && tempLastEtag.CompareTo(maxEtag) > 0)
                                {
                                    reachedMaxEtag = true;
                                    break;
                                }
                                lastEtag = tempLastEtag;

                                if (!Options.MatchFilters(document))
                                {
                                    numberOfSkippedDocs++;
                                    continue;
                                }

                                if (Options.ShouldExcludeExpired && Options.ExcludeExpired(document, now))
                                {
                                    numberOfSkippedDocs++;
                                    continue;
                                }

                                if (string.IsNullOrEmpty(Options.TransformScript) == false)
                                    document = await Operations.TransformDocument(document, Options.TransformScript).ConfigureAwait(false);

                                // If document is null after a transform we skip it. 
                                if (document == null)
                                {
                                    numberOfSkippedDocs++;
                                    continue;
                                }

                                try
                                {
                                    jsonWriter.Write(document);
                                }
                                catch (Exception e)
                                {
                                    if (IgnoreErrorsAndContinue == false)
                                        throw;

                                    Operations.ShowProgress("EXPORT of a document {0} failed. Message: {1}", document, e.Message);
                                }

                                totalCount++;

                                if (totalCount % 1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
                                {
                                    Operations.ShowProgress("Exported {0} documents", totalCount);
                                    lastReport = SystemTime.UtcNow;
                                }
                            }
                        }

                        if (hasDocs)
                            continue;

                        // The server can filter all the results. In this case, we need to try to go over with the next batch.
                        // Note that if the ETag' server restarts number is not the same, this won't guard against an infinite loop.
                        // (This code provides support for legacy RavenDB version: 1.0)
                        var databaseStatistics = await Operations.GetStats().ConfigureAwait(false);
                        var lastEtagComparable = new ComparableByteArray(lastEtag);
                        if (lastEtagComparable.CompareTo(databaseStatistics.LastDocEtag) < 0)
                        {
                            lastEtag = EtagUtil.Increment(lastEtag, amountToFetchFromServer);
                            if (lastEtag.CompareTo(databaseStatistics.LastDocEtag) >= 0)
                            {
                                lastEtag = databaseStatistics.LastDocEtag;
                            }
                            Operations.ShowProgress("Got no results but didn't get to the last doc etag, trying from: {0}", lastEtag);
                            continue;
                        }
                    }

                    // Load HiLo documents for selected collections
                    Options.Filters.ForEach(filter =>
                    {
                        if (string.Equals(filter.Path, "@metadata.Raven-Entity-Name", StringComparison.OrdinalIgnoreCase))
                        {
                            filter.Values.ForEach(collectionName =>
                            {
                                JsonDocument doc = Operations.GetDocument("Raven/Hilo/" + collectionName);
                                if (doc != null)
                                {
                                    doc.Metadata["@id"] = doc.Key;
                                    jsonWriter.Write(doc.ToJson());
                                    totalCount++;
                                }
                            });
                        }
                    });

                    Operations.ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalCount, lastEtag);
                    return lastEtag;
                }
                catch (Exception e)
                {
                    Operations.ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
                    Operations.ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalCount, lastEtag);
                    throw new SmugglerExportException(e.Message, e)
                    {
                        LastEtag = lastEtag,
                    };
                }
            }
        }

        public async Task WaitForIndexingAsOfLastWrite()
        {
            var stopwatch = Stopwatch.StartNew();
            var justIndexingWait = Stopwatch.StartNew();
                       
            var stats = await Operations.GetStats().ConfigureAwait(false);
            
            int tries = 0;
            Etag cutOffEtag = stats.LastDocEtag;
            while (true)
            {
                if (stats.Indexes.All(x => x.LastIndexedEtag.CompareTo(cutOffEtag) >= 0))
                {
                    Operations.ShowProgress("\rWaited {0} for indexing ({1} total).", justIndexingWait.Elapsed, stopwatch.Elapsed);
                    break;
                }

                if (tries++ % 10 == 0)
                    Operations.ShowProgress("\rWaiting {0} for indexing ({1} total).", justIndexingWait.Elapsed, stopwatch.Elapsed);

                Thread.Sleep(1000);
                stats = await Operations.GetStats().ConfigureAwait(false);
            }

            stopwatch.Stop();
            justIndexingWait.Stop();
        }
        

        public async Task WaitForIndexing()
        {
            var stopwatch = Stopwatch.StartNew();
            var justIndexingWait = Stopwatch.StartNew();

            int tries = 0;
            while (true)
            {
                var stats = await Operations.GetStats().ConfigureAwait(false);
                if (stats.StaleIndexes.Length != 0)
                {
                    if (tries++ % 10 == 0)
                        Operations.ShowProgress("\rWaiting {0} for indexing ({1} total).", justIndexingWait.Elapsed, stopwatch.Elapsed);

                    Thread.Sleep(1000);
                    continue;
                }

                Operations.ShowProgress("\rWaited {0} for indexing ({1} total).", justIndexingWait.Elapsed, stopwatch.Elapsed);
                break;
            }

            stopwatch.Stop();
            justIndexingWait.Stop();
        }

        public virtual async Task ImportData(SmugglerImportOptions<RavenConnectionStringOptions> importOptions)
        {
            if (importOptions.IsIncrementalImport == true)
                Options.Incremental = true;
            int countSpinnedFiles;
            string nextPartFileName;
            if (Options.Incremental == false)
            {
                Stream stream = importOptions.FromStream;
                bool ownStream = false;
                try
                {
                    countSpinnedFiles = 0;
                    nextPartFileName = importOptions.FromFile;
                    do
                    {
                    if (stream == null)
                    {
                            stream = File.OpenRead(nextPartFileName);
                        ownStream = true;
                    }
                        Operations.ShowProgress("Starting to import file: {0}", nextPartFileName);
                        await ImportData(importOptions, stream).ConfigureAwait(false);

                        if (ownStream == true)
                        {
                            nextPartFileName = 
                                $"{importOptions.FromFile}.part{++countSpinnedFiles:D3}";
                            stream?.Dispose();
                            stream = null;
                }
                    } while (ownStream == true && File.Exists(nextPartFileName) == true);
                }
                finally
                {
                    if (stream != null && ownStream)
                        stream.Dispose();
                }
                return;
            }

            var files = Directory.GetFiles(Path.GetFullPath(importOptions.FromFile))
                .Where(file => ".ravendb-incremental-dump".Equals(Path.GetExtension(file), StringComparison.CurrentCultureIgnoreCase))
                .OrderBy(File.GetLastWriteTimeUtc)
                .ToArray();

            if (files.Length == 0)
                return;

            var oldItemType = Options.OperateOnTypes;

            Options.OperateOnTypes = Options.OperateOnTypes & ~(ItemType.Indexes | ItemType.Transformers);

            for (var i = 0; i < files.Length - 1; i++)
            {
                countSpinnedFiles = 0;
                nextPartFileName = Path.Combine(importOptions.FromFile, files[i]);
                do
                {
                    using (var fileStream = File.OpenRead(nextPartFileName))
                    {
                        Operations.ShowProgress("Starting to import file: {0}", nextPartFileName);
                        await ImportData(importOptions, fileStream).ConfigureAwait(false);
                }
                    nextPartFileName = 
                        $"{Path.Combine(importOptions.FromFile, files[i])}.part{++countSpinnedFiles:D3}";
                } while (File.Exists(nextPartFileName) == true);
            }

            Options.OperateOnTypes = oldItemType;

            countSpinnedFiles = 0;
            nextPartFileName = Path.Combine(importOptions.FromFile, files.Last());
            do
            {
                using (var fileStream = File.OpenRead(nextPartFileName))
                {
                    Operations.ShowProgress("Starting to import file: {0}", nextPartFileName);
                    await ImportData(importOptions, fileStream).ConfigureAwait(false);
            }
                nextPartFileName = 
                    $"{Path.Combine(importOptions.FromFile, files.Last())}.part{++countSpinnedFiles:D3}";
            } while (File.Exists(nextPartFileName) == true);
        }

        public abstract Task Between(SmugglerBetweenOptions<RavenConnectionStringOptions> betweenOptions);

        public async virtual Task ImportData(SmugglerImportOptions<RavenConnectionStringOptions> importOptions, Stream stream)
        {
            Operations.Configure(Options);
            Operations.Initialize(Options);
            SupportedFeatures = await DetectServerSupportedFeatures(Operations, importOptions.To).ConfigureAwait(false);

            Stream sizeStream;

            var sw = Stopwatch.StartNew();
            // Try to read the stream compressed, otherwise continue uncompressed.
            JsonTextReader jsonReader;
            try
            {
                stream.Position = 0;
                sizeStream = new CountingStream(new GZipStream(stream, CompressionMode.Decompress));
                var streamReader = new StreamReader(sizeStream);

                jsonReader = new RavenJsonTextReader(streamReader);

                if (jsonReader.Read() == false)
                    return;
            }
            catch (Exception e)
            {
                if (e is InvalidDataException == false)
                    throw;

                stream.Seek(0, SeekOrigin.Begin);

                sizeStream = new CountingStream(stream);

                var streamReader = new StreamReader(sizeStream);

                jsonReader = new JsonTextReader(streamReader);

                if (jsonReader.Read() == false)
                    return;
            }

            if (jsonReader.TokenType != JsonToken.StartObject)
                throw new InvalidDataException("StartObject was expected");

            var exportCounts = new Dictionary<string, int>();
            var exportSectionRegistar = new Dictionary<string, Func<Task<int>>>();

            Options.CancelToken.Token.ThrowIfCancellationRequested();

            exportSectionRegistar.Add("Indexes", async() =>
            {
                Operations.ShowProgress("Begin reading indexes");
                var indexCount = await ImportIndexes(jsonReader).ConfigureAwait(false);
                Operations.ShowProgress(string.Format("Done with reading indexes, total: {0}", indexCount));
                return indexCount;
            });

            exportSectionRegistar.Add("Docs", async() =>
            {
                Operations.ShowProgress("Begin reading documents");
                var documentCount = await ImportDocuments(jsonReader).ConfigureAwait(false);
                Operations.ShowProgress(string.Format("Done with reading documents, total: {0}", documentCount));
                return documentCount;
            });

            exportSectionRegistar.Add("Attachments", async() =>
            {
                Operations.ShowProgress("Begin reading attachments");
                var attachmentCount = await ImportAttachments(importOptions.To, jsonReader).ConfigureAwait(false);
                Operations.ShowProgress(string.Format("Done with reading attachments, total: {0}", attachmentCount));
                return attachmentCount;
            });

            exportSectionRegistar.Add("Transformers", async() =>
            {
                Operations.ShowProgress("Begin reading transformers");
                var transformersCount = await ImportTransformers(jsonReader).ConfigureAwait(false);
                Operations.ShowProgress(string.Format("Done with reading transformers, total: {0}", transformersCount));
                return transformersCount;
            });

            exportSectionRegistar.Add("DocsDeletions", async() =>
        {
                Operations.ShowProgress("Begin reading deleted documents");
                var deletedDocumentsCount = await ImportDeletedDocuments(jsonReader).ConfigureAwait(false);
                Operations.ShowProgress(string.Format("Done with reading deleted documents, total: {0}", deletedDocumentsCount));
                return deletedDocumentsCount;
            });

            exportSectionRegistar.Add("AttachmentsDeletions", async() =>
            {
                Operations.ShowProgress("Begin reading deleted attachments");
                var deletedAttachmentsCount = await ImportDeletedAttachments(jsonReader).ConfigureAwait(false);
                Operations.ShowProgress(string.Format("Done with reading deleted attachments, total: {0}", deletedAttachmentsCount));
                return deletedAttachmentsCount;
            });

            exportSectionRegistar.Add("Identities", async() =>
            {
                Operations.ShowProgress("Begin reading identities");
                var identitiesCount = await ImportIdentities(jsonReader).ConfigureAwait(false);
                Operations.ShowProgress(string.Format("Done with reading identities, total: {0}", identitiesCount));
                return identitiesCount;
            });

            exportSectionRegistar.Keys.ForEach(k => exportCounts[k] = 0);


            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndObject)
            {
                Options.CancelToken.Token.ThrowIfCancellationRequested();

            if (jsonReader.TokenType != JsonToken.PropertyName)
                throw new InvalidDataException("PropertyName was expected");
                Func<Task<int>> currentAction;
                var currentSection = jsonReader.Value.ToString();
                if (exportSectionRegistar.TryGetValue(currentSection, out currentAction) == false)
                {
                    throw new InvalidDataException("Unexpected property found: " + jsonReader.Value);
                }
            if (jsonReader.Read() == false)
                {
                    exportCounts[currentSection] = 0;
                    continue;
                }

            if (jsonReader.TokenType != JsonToken.StartArray)
                throw new InvalidDataException("StartArray was expected");

                if (currentAction != null) exportCounts[currentSection] = await currentAction().ConfigureAwait(false);
            }

            sw.Stop();

            Operations.ShowProgress("Imported {0:#,#;;0} documents and {1:#,#;;0} attachments, deleted {2:#,#;;0} documents and {3:#,#;;0} attachments in {4:#,#.###;;0} s", exportCounts["Docs"], exportCounts["Attachments"], exportCounts["DocsDeletions"], exportCounts["AttachmentsDeletions"], sw.ElapsedMilliseconds / 1000f);

            Options.CancelToken.Token.ThrowIfCancellationRequested();
        }

        private async Task<int> ImportIdentities(JsonTextReader jsonReader)
        {
            var count = 0;

            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
            {
                Options.CancelToken.Token.ThrowIfCancellationRequested();

                var identity = RavenJToken.ReadFrom(jsonReader);

                var identityName = identity.Value<string>("Key");

                if (FilterIdentity(identityName, Options.OperateOnTypes) == false)
                    continue;

                try
                {
                await Operations.SeedIdentityFor(identityName, identity.Value<long>("Value")).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (IgnoreErrorsAndContinue == false)
                        throw;

                    Operations.ShowProgress("Failed seeding identity for {0}. Message: {1}", identityName, e.Message);
                    continue;
                }

                count++;
            }

            await Operations.SeedIdentityFor(null, -1).ConfigureAwait(false); // force flush

            return count;
        }

        private async Task<int> ImportDeletedDocuments(JsonReader jsonReader)
        {
            var count = 0;

            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
            {
                Options.CancelToken.Token.ThrowIfCancellationRequested();

                var item = RavenJToken.ReadFrom(jsonReader);

                var deletedDocumentInfo = new JsonSerializer { Converters = DefaultConverters }
                                                    .Deserialize<Tombstone>(new RavenJTokenReader(item));

                Operations.ShowProgress("Importing deleted document {0}", deletedDocumentInfo.Key);

                try
                {
                await Operations.DeleteDocument(deletedDocumentInfo.Key).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (IgnoreErrorsAndContinue == false)
                        throw;

                    Operations.ShowProgress("IMPORT of an deleted document {0} failed. Message: {1}", deletedDocumentInfo.Key, e.Message);
                }

                count++;
            }

            return count;
        }

        [Obsolete("Use RavenFS instead.")]
        private async Task<int> ImportDeletedAttachments(JsonReader jsonReader)
        {
            var count = 0;

            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
            {
                Options.CancelToken.Token.ThrowIfCancellationRequested();

                var item = RavenJToken.ReadFrom(jsonReader);

                var deletedAttachmentInfo = new JsonSerializer { Converters = DefaultConverters }
                                                    .Deserialize<Tombstone>(new RavenJTokenReader(item));

                Operations.ShowProgress("Importing deleted attachment {0}", deletedAttachmentInfo.Key);

                try
                {
                await Operations.DeleteAttachment(deletedAttachmentInfo.Key).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (IgnoreErrorsAndContinue == false)
                        throw;

                    Operations.ShowProgress("IMPORT of an deleted attachment {0} failed. Message: {1}", deletedAttachmentInfo.Key, e.Message);
                }

                count++;
            }

            return count;
        }

        private async Task<int> ImportTransformers(JsonTextReader jsonReader)
        {
            var count = 0;

            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
            {
                Options.CancelToken.Token.ThrowIfCancellationRequested();

                var transformer = RavenJToken.ReadFrom(jsonReader);
                if ((Options.OperateOnTypes & ItemType.Transformers) != ItemType.Transformers)
                    continue;

                var transformerName = transformer.Value<string>("name");

                try
                {
                await Operations.PutTransformer(transformerName, transformer).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (IgnoreErrorsAndContinue == false)
                        throw;

                    Operations.ShowProgress("PUT of a transformer {0} failed. Message: {1}", transformerName, e.Message);
                }

                count++;
            }

            await Operations.PutTransformer(null, null).ConfigureAwait(false); // force flush

            return count;
        }

        private static readonly Lazy<JsonConverterCollection> defaultConverters = new Lazy<JsonConverterCollection>(() =>
        {
            var converters = new JsonConverterCollection()
            {
                new JsonToJsonConverter(),
                new StreamFromJsonConverter()
            };
            converters.Freeze();

            return converters;
        }, true);

        private static JsonConverterCollection DefaultConverters 
        {
            get { return defaultConverters.Value; }
        }

        [Obsolete("Use RavenFS instead.")]
        private async Task<int> ImportAttachments(RavenConnectionStringOptions dst, JsonTextReader jsonReader)
        {
            var count = 0;

            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
            {
                Options.CancelToken.Token.ThrowIfCancellationRequested();

                var item = RavenJToken.ReadFrom(jsonReader);
                if ((Options.OperateOnTypes & ItemType.Attachments) != ItemType.Attachments)
                    continue;

                var attachmentExportInfo = new JsonSerializer { Converters = DefaultConverters }
                                                    .Deserialize<AttachmentExportInfo>(new RavenJTokenReader(item));

                Operations.ShowProgress("Importing attachment {0}", attachmentExportInfo.Key);

                try
                {
                    if (Options.StripReplicationInformation)
                        attachmentExportInfo.Metadata = Operations.StripReplicationInformationFromMetadata(attachmentExportInfo.Metadata);

                await Operations.PutAttachment(attachmentExportInfo).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (IgnoreErrorsAndContinue == false)
                        throw;

                    Operations.ShowProgress("IMPORT of an attachment {0} failed. Message: {1}", attachmentExportInfo.Key, e.Message);
                }

                count++;
            }

            await Operations.PutAttachment(null).ConfigureAwait(false); // force flush

            return count;
        }

        private async Task<int> ImportDocuments(JsonTextReader jsonReader)
        {
            var now = SystemTime.UtcNow;
            var count = 0;
            string continuationDocId = "Raven/Smuggler/Continuation/" + Options.ContinuationToken;

            var state = new OperationState
            {
                FilePath = Options.ContinuationToken,
                LastDocsEtag = Options.StartDocsEtag,
            };

            JsonDocument lastEtagsDocument = null;

            try
            {
                if (Options.UseContinuationFile)
                {
                    lastEtagsDocument = Operations.GetDocument(continuationDocId);
                    if (lastEtagsDocument == null)
                    {
                        lastEtagsDocument = new JsonDocument
                        {
                            Key = continuationDocId,
                            Etag = Etag.Empty,
                            DataAsJson = RavenJObject.FromObject(state)
                        };
                    }
                    else
                    {
                        state = lastEtagsDocument.DataAsJson.JsonDeserialization<OperationState>();
                    }

                    JsonDocument.EnsureIdInMetadata(lastEtagsDocument);
                }
            }
            catch (Exception e)
            {
                if (IgnoreErrorsAndContinue == false)
                    throw;

                Operations.ShowProgress("Failed loading continuation state. Message: {0}", e.Message);
            }

            int skippedDocuments = 0;
            long skippedDocumentsSize = 0;

            var tempLastEtag = Etag.Empty;

            var affectedCollections = new List<string>();
            Options.Filters.ForEach(filter =>
            {
                if (string.Equals(filter.Path, "@metadata.Raven-Entity-Name", StringComparison.OrdinalIgnoreCase))
                {
                    filter.Values.ForEach(affectedCollections.Add);
                }
            });

            var timeSinceLastWrite = Stopwatch.StartNew();

            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
            {
                Options.CancelToken.Token.ThrowIfCancellationRequested();
                if (timeSinceLastWrite.Elapsed > Options.HeartbeatLatency)
                {
                    var buildSkipDocument = BuildSkipDocument();
                    var heartbeatDocSize = (int)DocumentHelpers.GetRoughSize(buildSkipDocument);
                    await Operations.PutDocument(buildSkipDocument, heartbeatDocSize).ConfigureAwait(false);
                    timeSinceLastWrite.Restart();
                }

                try
                {
                    var document = (RavenJObject) RavenJToken.ReadFrom(jsonReader);
                    var size = DocumentHelpers.GetRoughSize(document);
                    if (size > 1024*1024)
                    {
                        Operations.ShowProgress("Large document warning: {0:#,#.##;;0} kb - {1}",
                            (double) size/1024,
                            document["@metadata"].Value<string>("@id"));
                    }
                    if ((Options.OperateOnTypes & ItemType.Documents) != ItemType.Documents)
                        continue;

                    if (Options.MatchFilters(document) == false)
                    {
                        if (affectedCollections.Count <= 0)
                            continue;

                        if (document.ContainsKey("@metadata") == false)
                            continue;

                        var key = document["@metadata"].Value<string>("@id");
                        if (key == null || key.StartsWith("Raven/Hilo/", StringComparison.OrdinalIgnoreCase) == false || affectedCollections.Any(x => key.EndsWith("/" + x, StringComparison.OrdinalIgnoreCase)) == false)
                            continue;
                    }

                    if (Options.ShouldExcludeExpired && Options.ExcludeExpired(document, now))
                        continue;

                    if (!string.IsNullOrEmpty(Options.TransformScript))
                    document = await Operations.TransformDocument(document, Options.TransformScript).ConfigureAwait(false);

                    // If document is null after a transform we skip it. 
                    if (document == null)
                        continue;

                    var metadata = document["@metadata"] as RavenJObject;
                    if (metadata != null)
                    {
                        if (Options.SkipConflicted && metadata.ContainsKey(Constants.RavenReplicationConflictDocument))
                            continue;

                        if (Options.StripReplicationInformation)
                            document["@metadata"] = Operations.StripReplicationInformationFromMetadata(metadata);

                        if (Options.ShouldDisableVersioningBundle)
                            document["@metadata"] = SmugglerHelper.DisableVersioning(metadata);

                        document["@metadata"] = SmugglerHelper.HandleConflictDocuments(metadata);
                    }

                    if (Options.UseContinuationFile)
                    {
                        tempLastEtag = Etag.Parse(document.Value<RavenJObject>("@metadata").Value<string>("@etag"));
                        if (tempLastEtag.CompareTo(state.LastDocsEtag) <= 0) // tempLastEtag < lastEtag therefore we are skipping.
                        {
                            skippedDocuments++;
                            skippedDocumentsSize += size;

                            continue;
                        }
                    }

                    timeSinceLastWrite.Restart();
                    await Operations.PutDocument(document, (int)size).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (IgnoreErrorsAndContinue == false)
                        throw;

                    Operations.ShowProgress("Failed to import document. Message: {0}", e.Message);
                }
                count++;

                if (count%Options.BatchSize == 0)
                {
                    if (Options.UseContinuationFile)
                    {
                        if (tempLastEtag.CompareTo(state.LastDocsEtag) > 0)
                            state.LastDocsEtag = tempLastEtag;

                        await WriteLastEtagToDatabase(state, lastEtagsDocument).ConfigureAwait(false);
                    }

                    // Wait for the batch to be indexed before continue.
                    if (Options.WaitForIndexing)
                        await WaitForIndexingAsOfLastWrite().ConfigureAwait(false);

                    Operations.ShowProgress("Read {0:#,#;;0} documents", count + skippedDocuments);
                }
            }

            if (Options.UseContinuationFile)
            {
                if (tempLastEtag.CompareTo(state.LastDocsEtag) > 0)
                    state.LastDocsEtag = tempLastEtag;

                await WriteLastEtagToDatabase(state, lastEtagsDocument).ConfigureAwait(false);

                Operations.ShowProgress("Documents skipped by continuation {0:#,#;;0} - approx. {1:#,#.##;;0} Mb.", skippedDocuments, (double) skippedDocumentsSize/1024/1024);
            }

            //precaution:
            //delete the heartbeat document in case the server is older version and doesn't recognize it
            await Operations.DeleteDocument(Constants.BulkImportHeartbeatDocKey).ConfigureAwait(false);

            await Operations.PutDocument(null, -1).ConfigureAwait(false); // force flush    

            return count;
        }


        private static RavenJObject BuildSkipDocument()
        {
            var metadata = new RavenJObject();
            metadata.Add("@id",Constants.BulkImportHeartbeatDocKey);
            var skipDoc = new JsonDocument
            {
                Key = Constants.BulkImportHeartbeatDocKey,
                DataAsJson = RavenJObject.FromObject(new 
                {
                    LastHearbeatSent = SystemTime.UtcNow
                }),
                Metadata = metadata
            };

            return skipDoc.ToJson();
        }

        private async Task WriteLastEtagToDatabase(OperationState state, JsonDocument lastEtagsDocument)
        {
            lastEtagsDocument.DataAsJson = RavenJObject.FromObject(state);

            var stateDocument = lastEtagsDocument.ToJson();
            int stateDocumentSize = (int)DocumentHelpers.GetRoughSize(stateDocument);
            await Operations.PutDocument(stateDocument, stateDocumentSize).ConfigureAwait(false);
        }

        private async Task<int> ImportIndexes(JsonReader jsonReader)
        {
            var count = 0;

            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
            {
                Options.CancelToken.Token.ThrowIfCancellationRequested();

                var index = (RavenJObject)RavenJToken.ReadFrom(jsonReader);
                if ((Options.OperateOnTypes & ItemType.Indexes) != ItemType.Indexes)
                    continue;

                var indexName = index.Value<string>("name");
                if (indexName.StartsWith("Temp/"))
                    continue;

                var definition = index.Value<RavenJObject>("definition");
                if (definition.Value<bool>("IsCompiled"))
                    continue; // can't import compiled indexes

                if ((Options.OperateOnTypes & ItemType.RemoveAnalyzers) == ItemType.RemoveAnalyzers)
                {
                    definition.Remove("Analyzers");
                }

                try
                {
                await Operations.PutIndex(indexName, index).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (IgnoreErrorsAndContinue == false)
                        throw;

                    Operations.ShowProgress("Failed to import index {0}. Message: {1}", indexName, e.Message);
                }

                count++;
            }

            await Operations.PutIndex(null, null).ConfigureAwait(false);

            return count;
        }

        private async Task ExportIndexes(RavenConnectionStringOptions src, SmugglerJsonTextWriter jsonWriter)
        {
            var totalCount = 0;
            int retries = RetriesCount;

            while (true)
            {
                RavenJArray indexes;

                try
                {
                    indexes = await Operations.GetIndexes(totalCount).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (retries-- == 0 && IgnoreErrorsAndContinue)
                    {
                        Operations.ShowProgress("Failed getting indexes too much times, stopping the index export entirely. Message: {0}", e.Message);
                        return;
                    }

                    if (IgnoreErrorsAndContinue == false)
                        throw new SmugglerExportException(e.Message, e);

                    Operations.ShowProgress("Failed fetching indexes. {0} retries remaining. Message: {1}", retries, e.Message);
                    continue;
                }

                if (indexes.Length == 0)
                {
                    Operations.ShowProgress("Done with reading indexes, total: {0}", totalCount);
                    break;
                }
                totalCount += indexes.Length;
                Operations.ShowProgress("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", indexes.Length, totalCount);
                foreach (var index in indexes)
                {
                    try
                    {
                        jsonWriter.Write(index);
                    }
                    catch (Exception e)
                    {
                        if (IgnoreErrorsAndContinue == false)
                            throw new SmugglerExportException(e.Message, e);

                        Operations.ShowProgress("Failed to export index {0}. Message: {1}", index, e.Message);
                    }
                }
            }
        }

        protected async Task<ServerSupportedFeatures> DetectServerSupportedFeatures(ISmugglerDatabaseOperations ops, RavenConnectionStringOptions server)
        {
            var serverVersion = await ops.GetVersion(server).ConfigureAwait(false);
            if (string.IsNullOrEmpty(serverVersion))
            {
                return GetLegacyModeFeatures();
            }

            var smugglerVersion = FileVersionInfo.GetVersionInfo(AssemblyHelper.GetAssemblyLocationFor<SmugglerDatabaseApiBase>()).ProductVersion;

            var subServerVersion = serverVersion.Substring(0, 3);
            var subSmugglerVersion = smugglerVersion.Substring(0, 3);

            var intServerVersion = int.Parse(subServerVersion.Replace(".", string.Empty));

            if (intServerVersion < 25)
            {
                
                ops.ShowProgress("Running in legacy mode, importing/exporting transformers is not supported. Server version: {0}. Smuggler version: {1}.", subServerVersion, subSmugglerVersion);
                return new ServerSupportedFeatures
                {
                    IsTransformersSupported = false,
                    IsDocsStreamingSupported = false,
                    IsIdentitiesSmugglingSupported = false
                };
            }

            if (intServerVersion == 25)
            {
                ops.ShowProgress("Running in legacy mode, importing/exporting identities is not supported. Server version: {0}. Smuggler version: {1}.", subServerVersion, subSmugglerVersion);

                return new ServerSupportedFeatures
                {
                    IsTransformersSupported = true,
                    IsDocsStreamingSupported = true,
                    IsIdentitiesSmugglingSupported = false
                };
            }

            return new ServerSupportedFeatures
            {
                IsTransformersSupported = true,
                IsDocsStreamingSupported = true,
                IsIdentitiesSmugglingSupported = true
            };
        }

        private ServerSupportedFeatures GetLegacyModeFeatures()
        {
            var result = new ServerSupportedFeatures
            {
                IsTransformersSupported = false,
                IsDocsStreamingSupported = false,
                IsIdentitiesSmugglingSupported = false
            };
            
            Operations.ShowProgress("Server version is not available. Running in legacy mode which does not support transformers.");

            return result;
        }

        public ServerSupportedFeatures SupportedFeatures { get; private set; }

        protected static void SetDatabaseNameIfEmpty(RavenConnectionStringOptions connection)
        {
            if (string.IsNullOrWhiteSpace(connection.DefaultDatabase) == false)
                return;
            var index = connection.Url.IndexOf("/databases/", StringComparison.OrdinalIgnoreCase);
            if (index != -1)
            {
                connection.DefaultDatabase = connection.Url.Substring(index + "/databases/".Length).Trim(new[] { '/' });
    }
}
    }
}
#endif
