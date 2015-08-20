﻿// -----------------------------------------------------------------------
//  <copyright file="IndexActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Raven.Database.Queries;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    public class IndexActions : ActionsBase
    {
		private volatile bool isPrecomputedBatchForNewIndexIsRunning;
	    private readonly object precomputedLock = new object();

	    public IndexActions(DocumentDatabase database, SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo> recentTouches, IUuidGenerator uuidGenerator, ILog log)
            : base(database, recentTouches, uuidGenerator, log)
        {
        }

	    internal IndexDefinition[] Definitions
	    {
		    get { return Database.IndexDefinitionStorage.IndexDefinitions.Select(inx => inx.Value).ToArray(); }
	    }

        public string[] GetIndexFields(string index)
        {
            var abstractViewGenerator = IndexDefinitionStorage.GetViewGenerator(index);
            return abstractViewGenerator == null ? new string[0] : abstractViewGenerator.Fields;
        }

        public Etag GetIndexEtag(string indexName, Etag previousEtag, string resultTransformer = null)
        {
            Etag lastDocEtag = Etag.Empty;
	        Etag lastIndexedEtag = null;
            Etag lastReducedEtag = null;
            bool isStale = false;
            int touchCount = 0;
            TransactionalStorage.Batch(accessor =>
            {
                var indexInstance = Database.IndexStorage.GetIndexInstance(indexName);
                if (indexInstance == null)
                    return;
                isStale = (indexInstance.IsMapIndexingInProgress) ||
                          accessor.Staleness.IsIndexStale(indexInstance.indexId, null, null);
                lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
                var indexStats = accessor.Indexing.GetIndexStats(indexInstance.indexId);
                if (indexStats != null)
                {
                    lastReducedEtag = indexStats.LastReducedEtag;
	                lastIndexedEtag = indexStats.LastIndexedEtag;
                }
                touchCount = accessor.Staleness.GetIndexTouchCount(indexInstance.indexId);
            });


            var indexDefinition = GetIndexDefinition(indexName);
            if (indexDefinition == null)
                return Etag.Empty; // this ensures that we will get the normal reaction of IndexNotFound later on.

            var list = new List<byte>();
            list.AddRange(indexDefinition.GetIndexHash());
            list.AddRange(Encoding.Unicode.GetBytes(indexName));
            if (string.IsNullOrWhiteSpace(resultTransformer) == false)
            {
                var abstractTransformer = IndexDefinitionStorage.GetTransformer(resultTransformer);
                if (abstractTransformer == null)
                    throw new InvalidOperationException("The result transformer: " + resultTransformer + " was not found");
                list.AddRange(abstractTransformer.GetHashCodeBytes());
            }
            list.AddRange(lastDocEtag.ToByteArray());
            list.AddRange(BitConverter.GetBytes(touchCount));
            list.AddRange(BitConverter.GetBytes(isStale));
            if (lastReducedEtag != null)
            {
                list.AddRange(lastReducedEtag.ToByteArray());
            }
	        if (lastIndexedEtag != null)
	        {
		        list.AddRange(lastIndexedEtag.ToByteArray());
	        }
            list.AddRange(BitConverter.GetBytes(UuidGenerator.LastDocumentTransactionEtag));

            var indexEtag = Etag.Parse(Encryptor.Current.Hash.Compute16(list.ToArray()));

            if (previousEtag != null && previousEtag != indexEtag)
            {
                // the index changed between the time when we got it and the time 
                // we actually call this, we need to return something random so that
                // the next time we won't get 304

                return Etag.InvalidEtag;
            }

            return indexEtag;
        }

        internal void CheckReferenceBecauseOfDocumentUpdate(string key, IStorageActionsAccessor actions, string[] participatingIds = null)
        {
            TouchedDocumentInfo touch;
            RecentTouches.TryRemove(key, out touch);
	        Stopwatch sp = null;
	        int count = 0;

	        using (Database.TransactionalStorage.DisableBatchNesting())
	        {
				// in external transaction number of references will be >= from current transaction references
		        Database.TransactionalStorage.Batch(externalActions =>
		        {
			        var referencingKeys = externalActions.Indexing.GetDocumentsReferencing(key);
			        if (participatingIds != null)
				        referencingKeys = referencingKeys.Except(participatingIds);

			        foreach (var referencing in referencingKeys)
					{
						Etag preTouchEtag = null;
						Etag afterTouchEtag = null;
						try
						{
							count++;
							actions.Documents.TouchDocument(referencing, out preTouchEtag, out afterTouchEtag);

							if (afterTouchEtag != null)
							{
								var docMetadata = actions.Documents.DocumentMetadataByKey(referencing);

								if (docMetadata != null)
								{
									var entityName = docMetadata.Metadata.Value<string>(Constants.RavenEntityName);

									if (string.IsNullOrEmpty(entityName) == false) 
										Database.LastCollectionEtags.Update(entityName, afterTouchEtag);
								}
							}
						}
						catch (ConcurrencyException)
						{
						}

						if (preTouchEtag == null || afterTouchEtag == null)
							continue;

						if (actions.General.MaybePulseTransaction())
						{
							if (sp == null)
								sp = Stopwatch.StartNew();
							if (sp.Elapsed >= TimeSpan.FromSeconds(30))
							{
								throw new TimeoutException("Early failure when checking references for document '" + key + "', we waited over 30 seconds to touch all of the documents referenced by this document.\r\n" +
														   "The operation (and transaction) has been aborted, since to try longer (we already touched " + count + " documents) risk a thread abort.\r\n" +
														   "Consider restructuring your indexes to avoid LoadDocument on such a popular document.");
							}
						}

						RecentTouches.Set(referencing, new TouchedDocumentInfo
						{
							PreTouchEtag = preTouchEtag,
							TouchedEtag = afterTouchEtag
						});
					}
		        });
	        }
        }

        private static void IsIndexNameValid(string name)
        {
	        var error = string.Format("Index name {0} not permitted. ", name).Replace("//","__");

            if (name.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase))
            {
				throw new ArgumentException(error + "Index names starting with dynamic_ or dynamic/ are reserved!", "name");
            }

            if (name.Equals("dynamic", StringComparison.OrdinalIgnoreCase))
            {
				throw new ArgumentException(error + "Index name dynamic is reserved!", "name");
            }

            if (name.Contains("//"))
            {
				throw new ArgumentException(error + "Index name cannot contain // (double slashes)", "name");
            }
        }

        public bool IndexHasChanged(string name, IndexDefinition definition)
        {
            if (name == null)
                throw new ArgumentNullException("name");

			name = name.Trim();
            IsIndexNameValid(name);

            var existingIndex = IndexDefinitionStorage.GetIndexDefinition(name);
            if (existingIndex == null)
                return true;

            var creationOption = FindIndexCreationOptions(definition, ref name);
            return creationOption != IndexCreationOptions.Noop;
        }

        // only one index can be created at any given time
        // the method already handle attempts to create the same index, so we don't have to 
        // worry about this.
        [MethodImpl(MethodImplOptions.Synchronized)]
		public string PutIndex(string name, IndexDefinition definition)
        {
	        return PutIndexInternal(name, definition);
        }

		private string PutIndexInternal(string name, IndexDefinition definition, bool disableIndexBeforePut = false, bool isUpdateBySideSide = false, IndexCreationOptions? creationOptions = null)
	    {
		    if (name == null)
			    throw new ArgumentNullException("name");

			name = name.Trim();
		    IsIndexNameValid(name);

		    var existingIndex = IndexDefinitionStorage.GetIndexDefinition(name);
		    if (existingIndex != null)
		    {
			    switch (existingIndex.LockMode)
			    {
				    case IndexLockMode.SideBySide:
					    if (isUpdateBySideSide == false)
					    {
						    Log.Info("Index {0} not saved because it might be only updated by side-by-side index");
						    throw new InvalidOperationException("Can not overwrite locked index: " + name + ". This index can be only updated by side-by-side index.");
					    }
					    break;
				    case IndexLockMode.LockedIgnore:
					    Log.Info("Index {0} not saved because it was lock (with ignore)", name);
					    return null;

				    case IndexLockMode.LockedError:
					    throw new InvalidOperationException("Can not overwrite locked index: " + name);
			    }
		    }

		    AssertAnalyzersValid(definition);

			switch (creationOptions ?? FindIndexCreationOptions(definition, ref name))
		    {
			    case IndexCreationOptions.Noop:
					return null;
			    case IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex:
				    // ensure that the code can compile
				    new DynamicViewCompiler(definition.Name, definition, Database.Extensions, IndexDefinitionStorage.IndexDefinitionsPath, Database.Configuration).GenerateInstance();
				    IndexDefinitionStorage.UpdateIndexDefinitionWithoutUpdatingCompiledIndex(definition);
					return null;
			    case IndexCreationOptions.Update:
				    // ensure that the code can compile
				    new DynamicViewCompiler(definition.Name, definition, Database.Extensions, IndexDefinitionStorage.IndexDefinitionsPath, Database.Configuration).GenerateInstance();
				    DeleteIndex(name);
				    break;
		    }

		    PutNewIndexIntoStorage(name, definition, disableIndexBeforePut);

		    WorkContext.ClearErrorsFor(name);

		    TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() => Database.Notifications.RaiseNotifications(new IndexChangeNotification
		    {
			    Name = name,
			    Type = IndexChangeTypes.IndexAdded,
		    }));

		    return name;
	    }

	    [MethodImpl(MethodImplOptions.Synchronized)]
		public string[] PutIndexes(IndexToAdd[] indexesToAdd)
		{
			var createdIndexes = new List<string>();
			var prioritiesList = new List<IndexingPriority>();
			try
			{
				foreach (var indexToAdd in indexesToAdd)
				{
					var nameToAdd = PutIndexInternal(indexToAdd.Name, indexToAdd.Definition, disableIndexBeforePut: true);
					if (nameToAdd == null)
						continue;

					createdIndexes.Add(nameToAdd);
					prioritiesList.Add(indexToAdd.Priority);
				}

                var indexesIds = createdIndexes.Select(x => Database.IndexStorage.GetIndexInstance(x).indexId).ToArray();
                Database.TransactionalStorage.Batch(accessor => accessor.Indexing.SetIndexesPriority(indexesIds, prioritiesList.ToArray()));
			
				return createdIndexes.ToArray();
			}
			catch (Exception e)
			{
			    Log.WarnException("Could not create index batch", e);
                foreach (var index in createdIndexes)
                {
                    DeleteIndex(index);
                }
				throw;
			}
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public SideBySideIndexInfo[] PutSideBySideIndexes(IndexToAdd[] indexesToAdd)
		{
			var createdIndexes = new List<SideBySideIndexInfo>();
			var prioritiesList = new List<IndexingPriority>();
			try
			{
				foreach (var indexToAdd in indexesToAdd)
				{
					var originalIndexName = indexToAdd.Name.Trim();
					var indexName = Constants.SideBySideIndexNamePrefix + originalIndexName;
					var isSideBySide = true;

					IndexCreationOptions? creationOptions = null;
					//if there is no existing side by side index, we might need to update the old index
					if (IndexDefinitionStorage.GetIndexDefinition(indexName) == null)
					{
						var originalIndexCreationOptions = FindIndexCreationOptions(indexToAdd.Definition, ref originalIndexName);
						switch (originalIndexCreationOptions)
						{
							case IndexCreationOptions.Noop:
								continue;
							case IndexCreationOptions.Create:
							case IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex:
								//cases in which we don't need to create a side by side index:
								//1) index doesn't exist => need to create a new regular index
								//2) there is an existing index and we need to update its definition without reindexing
								indexName = originalIndexName;
								isSideBySide = false;
								creationOptions = originalIndexCreationOptions;
								break;
						}
					}

					var nameToAdd = PutIndexInternal(indexName, indexToAdd.Definition, disableIndexBeforePut: true, isUpdateBySideSide: true, creationOptions: creationOptions);
					if (nameToAdd == null)
						continue;

					createdIndexes.Add(new SideBySideIndexInfo
					{
						OriginalName = originalIndexName,
						Name = nameToAdd,
						IsSideBySide = isSideBySide
					});
					prioritiesList.Add(indexToAdd.Priority);
				}

				var indexesIds = createdIndexes.Select(x => Database.IndexStorage.GetIndexInstance(x.Name).indexId).ToArray();
				Database.TransactionalStorage.Batch(accessor => accessor.Indexing.SetIndexesPriority(indexesIds, prioritiesList.ToArray()));

				return createdIndexes.ToArray();
			}
			catch (Exception e)
			{
				Log.WarnException("Could not create index batch", e);
				foreach (var index in createdIndexes)
				{
					DeleteIndex(index.Name);
				}
				throw;
			}
		}

	    public class SideBySideIndexInfo
	    {
			public string OriginalName { get; set; }

			public string Name { get; set; }

			public bool IsSideBySide { get; set; }
	    }

        private static void AssertAnalyzersValid(IndexDefinition indexDefinition)
        {
            foreach (var analyzer in indexDefinition.Analyzers)
            {
				//this throws if the type cannot be found
				IndexingExtensions.GetAnalyzerType(analyzer.Key, analyzer.Value);
            }
        }

        internal void PutNewIndexIntoStorage(string name, IndexDefinition definition, bool disableIndex = false)
        {
            Debug.Assert(Database.IndexStorage != null);
            Debug.Assert(TransactionalStorage != null);
            Debug.Assert(WorkContext != null);

	        Index index = null;
			TransactionalStorage.Batch(actions =>
            {
	            var maxId = 0;
	            if (Database.IndexStorage.Indexes.Length > 0)
	            {
		            maxId = Database.IndexStorage.Indexes.Max();
	            }
	            definition.IndexId = (int) Database.Documents.GetNextIdentityValueWithoutOverwritingOnExistingDocuments("IndexId", actions);
	            if (definition.IndexId <= maxId)
	            {
		            actions.General.SetIdentityValue("IndexId", maxId + 1);
					definition.IndexId = (int)Database.Documents.GetNextIdentityValueWithoutOverwritingOnExistingDocuments("IndexId", actions);
	            }

	            IndexDefinitionStorage.RegisterNewIndexInThisSession(name, definition);

	            // this has to happen in this fashion so we will expose the in memory status after the commit, but 
	            // before the rest of the world is notified about this.

	            IndexDefinitionStorage.CreateAndPersistIndex(definition);
	            Database.IndexStorage.CreateIndexImplementation(definition);
				index = Database.IndexStorage.GetIndexInstance(definition.IndexId);

				// If we execute multiple indexes at once and want to activate them all at once we will disable the index from the endpoint
				if (disableIndex)
					index.Priority = IndexingPriority.Disabled;

				//ensure that we don't start indexing it right away, let the precomputation run first, if applicable
	            index.IsMapIndexingInProgress = true;
	            if (definition.IsTestIndex)
					index.MarkQueried(); // test indexes should be mark queried, so the cleanup task would not delete them immediately

				InvokeSuggestionIndexing(name, definition, index);

	            actions.Indexing.AddIndex(definition.IndexId, definition.IsMapReduce);
            });

	        Debug.Assert(index != null);

	        Action precomputeTask = null;
	        if (WorkContext.RunIndexing &&
				name.Equals(Constants.DocumentsByEntityNameIndex, StringComparison.InvariantCultureIgnoreCase) == false &&
	            Database.IndexStorage.HasIndex(Constants.DocumentsByEntityNameIndex) && isPrecomputedBatchForNewIndexIsRunning == false)
	        {
		        // optimization of handling new index creation when the number of document in a database is significantly greater than
		        // number of documents that this index applies to - let us use built-in RavenDocumentsByEntityName to get just appropriate documents

				precomputeTask = TryCreateTaskForApplyingPrecomputedBatchForNewIndex(index, definition);
	        }
	        else
	        {
		        index.IsMapIndexingInProgress = false;// we can't apply optimization, so we'll make it eligible for running normally
	        }

			// The act of adding it here make it visible to other threads
			// we have to do it in this way so first we prepare all the elements of the 
			// index, then we add it to the storage in a way that make it public
			IndexDefinitionStorage.AddIndex(definition.IndexId, definition);

			// we start the precomuteTask _after_ we finished adding the index
			if (precomputeTask != null)
			{
				precomputeTask();
			}

	        WorkContext.ShouldNotifyAboutWork(() => "PUT INDEX " + name);
            WorkContext.NotifyAboutWork();
        }

        private Action TryCreateTaskForApplyingPrecomputedBatchForNewIndex(Index index, IndexDefinition definition)
        {
            var generator = IndexDefinitionStorage.GetViewGenerator(definition.IndexId);
            if (generator.ForEntityNames.Count == 0 && index.IsTestIndex == false)
            {
                // we don't optimize if we don't have what to optimize _on_, we know this is going to return all docs.
                // no need to try to optimize that, then
				index.IsMapIndexingInProgress = false;
	            return null;
            }

			lock (precomputedLock)
			{
				if (isPrecomputedBatchForNewIndexIsRunning)
				{
					index.IsMapIndexingInProgress = false;
					return null;
				}

				isPrecomputedBatchForNewIndexIsRunning = true;
			}

            try
            {
				var cts = new CancellationTokenSource();
				var task = new Task(() =>
				{
					try
					{
						ApplyPrecomputedBatchForNewIndex(index, generator, index.IsTestIndex == false ? Database.Configuration.MaxNumberOfItemsToProcessInSingleBatch : Database.Configuration.Indexing.MaxNumberOfItemsToProcessInTestIndexes, cts);
					}
					catch (Exception e)
					{
						Log.Warn("Could not apply precomputed batch for index " + index, e);
					}
					finally
					{
						isPrecomputedBatchForNewIndexIsRunning = false;
						index.IsMapIndexingInProgress = false;
						WorkContext.ShouldNotifyAboutWork(() => "Precomputed indexing batch for " + index.PublicName + " is completed");
						WorkContext.NotifyAboutWork();
					}
				}, TaskCreationOptions.LongRunning);

	            return () =>
	            {
		            try
		            {
			            task.Start();

			            long id;
			            Database
				            .Tasks
				            .AddTask(
					            task,
					            new TaskBasedOperationState(task),
					            new TaskActions.PendingTaskDescription
					            {
						            StartTime = DateTime.UtcNow,
						            Payload = index.PublicName,
						            TaskType = TaskActions.PendingTaskType.NewIndexPrecomputedBatch
					            },
					            out id,
					            cts);
		            }
		            catch (Exception)
		            {
						index.IsMapIndexingInProgress = false;
			            isPrecomputedBatchForNewIndexIsRunning = false;
			            throw;
		            }
	            };
            }
            catch (Exception)
            {
                index.IsMapIndexingInProgress = false;
				isPrecomputedBatchForNewIndexIsRunning = false;
                throw;
            }
        }

	    private void ApplyPrecomputedBatchForNewIndex(Index index, AbstractViewGenerator generator, int pageSize, CancellationTokenSource cts)
        {
            PrecomputedIndexingBatch result = null;

            var docsToIndex = new List<JsonDocument>();
            TransactionalStorage.Batch(actions =>
            {
	            var query = GetQueryForAllMatchingDocumentsForIndex(generator);

	            JsonDocument highestByEtag = null;

				using (var linked = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, WorkContext.CancellationToken))
				using (var op = new QueryActions.DatabaseQueryOperation(Database, Constants.DocumentsByEntityNameIndex, new IndexQuery
                {
                    Query = query,
					PageSize = pageSize
                }, actions, linked)
                {
                    ShouldSkipDuplicateChecking = true
                })
                {
                    op.Init();
                    if (op.Header.TotalResults == 0 ||
                        (op.Header.TotalResults > Database.Configuration.MaxNumberOfItemsToProcessInSingleBatch))
                    {
                        // we don't apply this optimization if the total number of results 
						// to index is more than the max numbers to index in a single batch. 
						// The idea here is that we need to keep the amount
                        // of memory we use to a manageable level even when introducing a new index to a BIG 
                        // database
                        try
                        {
                            cts.Cancel();
                            // we have to run just a little bit of the query to properly setup the disposal
                            op.Execute(o => { });
                        }
                        catch (OperationCanceledException)
                        {
                        }
                        return;
                    }

                    Log.Debug("For new index {0}, using precomputed indexing batch optimization for {1} docs", index,
                              op.Header.TotalResults);
                    op.Execute(document =>
                    {
                        var metadata = document.Value<RavenJObject>(Constants.Metadata);
                        var key = metadata.Value<string>("@id");
                        var etag = Etag.Parse(metadata.Value<string>("@etag"));
                        var lastModified = DateTime.Parse(metadata.Value<string>(Constants.LastModified));
                        document.Remove(Constants.Metadata);

	                    var doc = new JsonDocument
	                    {
		                    DataAsJson = document,
		                    Etag = etag,
		                    Key = key,
		                    LastModified = lastModified,
		                    SkipDeleteFromIndex = true,
		                    Metadata = metadata
	                    };

	                    docsToIndex.Add(doc);

	                    if (highestByEtag == null || doc.Etag.CompareTo(highestByEtag.Etag) > 0)
		                    highestByEtag = doc;
                    });
                }

	            result = new PrecomputedIndexingBatch
                {
                    LastIndexed = highestByEtag.Etag,
                    LastModified = highestByEtag.LastModified.Value,
                    Documents = docsToIndex,
                    Index = index
                };
            });

			if (result != null && result.Documents != null && result.Documents.Count > 0)
			{
				using (var linked = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, WorkContext.CancellationToken))
				{
					Database.IndexingExecuter.IndexPrecomputedBatch(result, linked.Token);

					if (index.IsTestIndex) 
						TransactionalStorage.Batch(accessor => accessor.Indexing.TouchIndexEtag(index.IndexId));
				}
			}

        }

	    private string GetQueryForAllMatchingDocumentsForIndex(AbstractViewGenerator generator)
	    {
		    var terms = new TermsQueryRunner(Database).GetTerms(Constants.DocumentsByEntityNameIndex, "Tag", null, int.MaxValue);

		    var sb = new StringBuilder();

		    foreach (var entityName in generator.ForEntityNames)
		    {
			    foreach (var term in terms)
			    {
				    if (string.Equals(entityName, term, StringComparison.OrdinalIgnoreCase))
				    {
						if (sb.Length != 0)
							sb.Append(" OR ");

						sb.Append("Tag:[[").Append(term).Append("]]");
				    }
			    }
		    }

		    var query = sb.ToString();
		    return query;
	    }

	    private void InvokeSuggestionIndexing(string name, IndexDefinition definition, Index index)
        {
            foreach (var suggestion in definition.Suggestions)
            {
                var field = suggestion.Key;
                var suggestionOption = suggestion.Value;

                if (suggestionOption.Distance == StringDistanceTypes.None)
                    continue;

                var indexExtensionKey =
                    MonoHttpUtility.UrlEncode(field + "-" + suggestionOption.Distance + "-" +
                                              suggestionOption.Accuracy);

                var suggestionQueryIndexExtension = new SuggestionQueryIndexExtension(
					index,
                     WorkContext,
                     Path.Combine(Database.Configuration.IndexStoragePath, "Raven-Suggestions", name, indexExtensionKey),
                     SuggestionQueryRunner.GetStringDistance(suggestionOption.Distance),
                     Database.Configuration.RunInMemory,
                     field,
                     suggestionOption.Accuracy);

                Database.IndexStorage.SetIndexExtension(name, indexExtensionKey, suggestionQueryIndexExtension);
            }
        }

        private IndexCreationOptions FindIndexCreationOptions(IndexDefinition definition, ref string name)
        {
            definition.Name = name;
            definition.RemoveDefaultValues();
            IndexDefinitionStorage.ResolveAnalyzers(definition);
            var findIndexCreationOptions = IndexDefinitionStorage.FindIndexCreationOptions(definition);
            return findIndexCreationOptions;
        }

        internal Task StartDeletingIndexDataAsync(int id, string indexName)
        {
            var sp = Stopwatch.StartNew();
            //remove the header information in a sync process
            TransactionalStorage.Batch(actions => actions.Indexing.PrepareIndexForDeletion(id));
            var deleteIndexTask = Task.Run(() =>
            {
                Debug.Assert(Database.IndexStorage != null);
                Database.IndexStorage.DeleteIndexData(id); // Data can take a while

                TransactionalStorage.Batch(actions =>
                {
                    // And Esent data can take a while too
                    actions.Indexing.DeleteIndex(id, WorkContext.CancellationToken);
                    if (WorkContext.CancellationToken.IsCancellationRequested)
                        return;

                    actions.Lists.Remove("Raven/Indexes/PendingDeletion", id.ToString(CultureInfo.InvariantCulture));
                });
            });

            long taskId;
            Database.Tasks.AddTask(deleteIndexTask, null, new TaskActions.PendingTaskDescription
                                                          {
                                                              StartTime = SystemTime.UtcNow,
                                                              TaskType = TaskActions.PendingTaskType.IndexDeleteOperation,
                                                              Payload = indexName
                                                          }, out taskId);

            deleteIndexTask.ContinueWith(t =>
            {
                if (t.IsFaulted || t.IsCanceled)
                {
                    Log.WarnException("Failure when deleting index " + indexName, t.Exception);
                }
                else
                {
                    Log.Info("The async deletion of index {0} was completed in {1}", indexName, sp.Elapsed);
                }
            });

            return deleteIndexTask;
        }

        public RavenJArray GetIndexNames(int start, int pageSize)
        {
            return new RavenJArray(
                IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
                    .Select(s => new RavenJValue(s))
                );
        }

        public RavenJArray GetIndexes(int start, int pageSize)
        {
            return new RavenJArray(
                from indexName in IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
                let indexDefinition = IndexDefinitionStorage.GetIndexDefinition(indexName)
                select new RavenJObject
		        {
			        {"name", new RavenJValue(indexName)},
			        {"definition", indexDefinition != null ? RavenJObject.FromObject(indexDefinition) : null},
		        });
        }

        public IndexDefinition GetIndexDefinition(string index)
        {
            return IndexDefinitionStorage.GetIndexDefinition(index);
        }

        public void ResetIndex(string index)
        {
            var indexDefinition = IndexDefinitionStorage.GetIndexDefinition(index);
            if (indexDefinition == null)
                throw new InvalidOperationException("There is no index named: " + index);
            DeleteIndex(index);
            PutIndex(index, indexDefinition);
        }

        public bool DeleteIndex(string name)
        {
            var instance = IndexDefinitionStorage.GetIndexDefinition(name);
            if (instance == null) 
				return false;

			DeleteIndex(instance);
	        return true;
        }

		internal void DeleteIndex(IndexDefinition instance, bool removeByNameMapping = true, bool clearErrors = true, bool removeIndexReplaceDocument = true, bool isSideBySideReplacement = false)
		{
			using (IndexDefinitionStorage.TryRemoveIndexContext())
			{
				if (instance == null) 
					return;

				// Set up a flag to signal that this is something we're doing
				TransactionalStorage.Batch(actions => actions.Lists.Set("Raven/Indexes/PendingDeletion", instance.IndexId.ToString(CultureInfo.InvariantCulture), (RavenJObject.FromObject(new
				{
					TimeOfOriginalDeletion = SystemTime.UtcNow,
					instance.IndexId,
					IndexName = instance.Name
				})), UuidType.Tasks));

				// Delete the main record synchronously
				IndexDefinitionStorage.RemoveIndex(instance.IndexId, removeByNameMapping);
				Database.IndexStorage.DeleteIndex(instance.IndexId);

				if (clearErrors)
					WorkContext.ClearErrorsFor(instance.Name);

                if (removeIndexReplaceDocument && instance.IsSideBySideIndex)
                {
                    Database.Documents.Delete(Constants.IndexReplacePrefix + instance.Name, null, null);
                }

				// And delete the data in the background
				StartDeletingIndexDataAsync(instance.IndexId, instance.Name);


				var indexChangeType = isSideBySideReplacement ? IndexChangeTypes.SideBySideReplace : IndexChangeTypes.IndexRemoved;

				// We raise the notification now because as far as we're concerned it is done *now*
				TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() => 
					Database.Notifications.RaiseNotifications(new IndexChangeNotification
					{
						Name = instance.Name,
						Type = indexChangeType,
					})
				);
			}
		}

    }
}