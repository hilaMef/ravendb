//-----------------------------------------------------------------------
// <copyright file="WorkContext.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using System.Linq;
using System.Runtime.InteropServices;
using Raven.Database.Util;
using Raven.Json.Linq;
using Sparrow.Collections;

namespace Raven.Database.Indexing
{
	public class WorkContext : IDisposable
	{
		private readonly ConcurrentSet<FutureBatchStats> futureBatchStats = new ConcurrentSet<FutureBatchStats>();

		private readonly SizeLimitedConcurrentSet<string> recentlyDeleted = new SizeLimitedConcurrentSet<string>(100, StringComparer.OrdinalIgnoreCase);

		private readonly SizeLimitedConcurrentSet<IndexingBatchInfo> lastActualIndexingBatchInfo = new SizeLimitedConcurrentSet<IndexingBatchInfo>(25);
		private readonly SizeLimitedConcurrentSet<ReducingBatchInfo> lastActualReducingBatchInfo = new SizeLimitedConcurrentSet<ReducingBatchInfo>(25);
		private readonly ConcurrentQueue<IndexingError> indexingErrors = new ConcurrentQueue<IndexingError>();
		private readonly object waitForWork = new object();
		private volatile bool doWork = true;
		private volatile bool doIndexing = true;
        private volatile bool doReducing = true;
        private int workCounter;
		private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
		private static readonly ILog log = LogManager.GetCurrentClassLogger();
		private readonly ThreadLocal<Stack<List<Func<string>>>> shouldNotifyOnWork = new ThreadLocal<Stack<List<Func<string>>>>(() =>
		{
			var stack = new Stack<List<Func<string>>>();
			stack.Push(new List<Func<string>>());
			return stack;
		});
		private long errorsCounter = 0;
		
	    public WorkContext()
	    {
            CurrentlyRunningQueries = new ConcurrentDictionary<string, ConcurrentSet<ExecutingQueryInfo>>(StringComparer.OrdinalIgnoreCase);
            MetricsCounters = new MetricsCountersManager();
	        InstallGauges();
		    LastIdleTime = SystemTime.UtcNow;
	    }

		public OrderedPartCollection<AbstractIndexUpdateTrigger> IndexUpdateTriggers { get; set; }
		public OrderedPartCollection<AbstractReadTrigger> ReadTriggers { get; set; }
        public OrderedPartCollection<AbstractIndexReaderWarmer> IndexReaderWarmers { get; set; }
		public string DatabaseName { get; set; }

	    public DateTime LastWorkTime
	    {
            get { return new DateTime(lastWorkTimeTicks); }
	    }
	    public DateTime LastIdleTime { get; set; }
		public bool DoWork
		{
			get { return doWork; }
		}

		public bool RunIndexing
		{
			get { return doWork && doIndexing; }
		}
        public bool RunReducing
        {
            get { return doWork && doReducing; }
        }
        public void UpdateFoundWork()
		{
		    var now = SystemTime.UtcNow;
		    var lastWorkTime = LastWorkTime;
		    if ((now - lastWorkTime).TotalSeconds < 2)
		    {
                // to avoid too much pressure on this, we only update this every 2 seconds
		        return;
		    }
            // set the value atomically
		    Interlocked.Exchange(ref lastWorkTimeTicks, now.Ticks);
		}

	    //collection that holds information about currently running queries, in the form of [Index name -> (When query started,IndexQuery data)]
        public ConcurrentDictionary<string,ConcurrentSet<ExecutingQueryInfo>> CurrentlyRunningQueries { get; private set; }

	    private int nextQueryId = 0;

		public InMemoryRavenConfiguration Configuration { get; set; }
		public IndexStorage IndexStorage { get; set; }

		public TaskScheduler TaskScheduler { get; set; }
		public IndexDefinitionStorage IndexDefinitionStorage { get; set; }

		[CLSCompliant(false)]
		public ITransactionalStorage TransactionalStorage { get; set; }

		public IndexingError[] Errors
		{
			get { return indexingErrors.ToArray(); }
		}

		public int CurrentNumberOfParallelTasks
		{
			get
			{
				var currentNumberOfParallelTasks = Configuration.MaxNumberOfParallelProcessingTasks*BackgroundTaskExecuter.Instance.MaxNumberOfParallelProcessingTasksRatio;
				var numberOfParallelTasks = Math.Min((int)currentNumberOfParallelTasks, Configuration.MaxNumberOfParallelProcessingTasks);
				return Math.Max(numberOfParallelTasks, 1);
			}
		}

		public int CurrentNumberOfItemsToIndexInSingleBatch { get; set; }

		public int CurrentNumberOfItemsToReduceInSingleBatch { get; set; }

		public int NumberOfItemsToExecuteReduceInSingleStep
		{
			get { return Configuration.NumberOfItemsToExecuteReduceInSingleStep; }
		}

		public bool WaitForWork(TimeSpan timeout, ref int workerWorkCounter, string name)
		{
			return WaitForWork(timeout, ref workerWorkCounter, null, name);
		}

        private void InstallGauges()
        {
            MetricsCounters.AddGauge(GetType(), "RunningQueriesCount", () => CurrentlyRunningQueries.Count);
        }

		public void RecoverIndexingErrors()
		{
			var storedIndexingErrors = new List<ListItem>();
			TransactionalStorage.Batch(accessor =>
			{
				foreach (var indexName in IndexDefinitionStorage.IndexNames)
				{
					storedIndexingErrors.AddRange(accessor.Lists.Read("Raven/Indexing/Errors/" + indexName, Etag.Empty, null, 5000));
				}
			});

			if(storedIndexingErrors.Count == 0)
				return;

			var errors = storedIndexingErrors.Select(x => x.Data.JsonDeserialization<IndexingError>()).OrderBy(x => x.Timestamp);

			foreach (var error in errors)
			{
				indexingErrors.Enqueue(error);
			}

			TransactionalStorage.Batch(accessor =>
			{
				while (indexingErrors.Count > 50)
				{
					IndexingError error;
					if (indexingErrors.TryDequeue(out error) == false)
						continue;

					accessor.Lists.Remove("Raven/Indexing/Errors/" + error.IndexName, error.Id.ToString(CultureInfo.InvariantCulture));
				}
			});

			errorsCounter = errors.Max(x => x.Id);
		}

		public bool WaitForWork(TimeSpan timeout, ref int workerWorkCounter, Action beforeWait, string name)
		{
			if (!doWork)
				return false;
			var currentWorkCounter = Thread.VolatileRead(ref workCounter);
			if (currentWorkCounter != workerWorkCounter)
			{
				workerWorkCounter = currentWorkCounter;
				return true;
			}
			if (beforeWait != null)
				beforeWait();
			lock (waitForWork)
			{
				if (!doWork)
					return false;
				currentWorkCounter = Thread.VolatileRead(ref workCounter);
				if (currentWorkCounter != workerWorkCounter)
				{
					workerWorkCounter = currentWorkCounter;
					return true;
				}
                CancellationToken.ThrowIfCancellationRequested();
				log.Debug("No work was found, workerWorkCounter: {0}, for: {1}, will wait for additional work", workerWorkCounter, name);
				var forWork = Monitor.Wait(waitForWork, timeout);
				if (forWork)
					UpdateFoundWork();
				return forWork;
			}
		}

		public void ShouldNotifyAboutWork(Func<string> why)
		{
			shouldNotifyOnWork.Value.Peek().Add(why);
			UpdateFoundWork();
		}

		public void HandleWorkNotifications()
		{
			if (disposed)
				return;
			if (shouldNotifyOnWork.Value.Peek().Count == 0)
				return;
			NotifyAboutWork();
		}

		public void NestedTransactionEnter()
		{
			shouldNotifyOnWork.Value.Push(new List<Func<string>>());
		}

		public void NestedTransactionExit()
		{
			if (shouldNotifyOnWork.Value.Count == 1)
				throw new InvalidOperationException("BUG: Cannot empty the should notify work stack");
			shouldNotifyOnWork.Value.Pop();
		}

		public int GetWorkCount()
		{
			return workCounter;
		}

		public void NotifyAboutWork()
		{
			lock (waitForWork)
			{
				var notifications = shouldNotifyOnWork.Value.Peek();
				if (doWork == false)
				{
					// need to clear this anyway
					if(disposed == false)
						notifications.Clear();
					return;
				}
				var increment = Interlocked.Increment(ref workCounter);
				if (log.IsDebugEnabled)
				{
					var reason = string.Join(", ", notifications.Select(action => action()).Where(x => x != null));
					log.Debug("Incremented work counter to {0} because: {1}", increment, reason);
				}
				notifications.Clear();
				Monitor.PulseAll(waitForWork);
			}
		}

		public void StartWork()
		{
			doWork = true;
			doIndexing = true;
		    doReducing = true;
		}

		public void StopWork()
		{
			log.Debug("Stopping background workers");
			doWork = false;
			doIndexing = false;
		    doReducing = false;
			lock (waitForWork)
			{
				Monitor.PulseAll(waitForWork);
			}
		}

		public void AddError(int index, string indexName, string key, Exception exception)
		{
			AddError(index, indexName, key, exception, "Unknown");
		}

		public void AddError(int index, string indexName, string key, Exception exception, string component)
		{
			var aggregateException = exception as AggregateException;
			if (aggregateException != null)
				exception = aggregateException.ExtractSingleInnerException();

			AddError(index, indexName, key, exception != null ? exception.Message : "Unknown message", component);
		}

		public void AddError(int index, string indexName, string key, string error)
		{
			AddError(index, indexName, key, error, "Unknown");
		}

		public void AddError(int index, string indexName, string key, string error, string component)
		{
			errorsCounter = Interlocked.Increment(ref errorsCounter);

			var indexingError = new IndexingError
			{
				Id = errorsCounter,
				Document = key,
				Error = error,
				Index = index,
				IndexName = indexName,
				Action = component,
				Timestamp = SystemTime.UtcNow
			};

			indexingErrors.Enqueue(indexingError);

			if (indexingErrors.Count <= 50)
			{
				TransactionalStorage.Batch(accessor => accessor.Lists.Set("Raven/Indexing/Errors/" + indexName, indexingError.Id.ToString(CultureInfo.InvariantCulture), RavenJObject.FromObject(indexingError), UuidType.Indexing));
				return;
			}

			IndexingError ignored;
			indexingErrors.TryDequeue(out ignored);

			if ((SystemTime.UtcNow - ignored.Timestamp).TotalSeconds > 10)
			{
				TransactionalStorage.Batch(accessor =>
				{
					accessor.Lists.Set("Raven/Indexing/Errors/" + indexName, indexingError.Id.ToString(CultureInfo.InvariantCulture), RavenJObject.FromObject(indexingError), UuidType.Indexing);
					accessor.Lists.RemoveAllOlderThan("Raven/Indexing/Errors/" + ignored.IndexName, ignored.Timestamp);
				});
				
			}
		}

		public void StopWorkRude()
		{
			StopWork();
			cancellationTokenSource.Cancel();
		}

		public CancellationToken CancellationToken
		{
			get { return cancellationTokenSource.Token; }
		}

		public void Dispose()
		{
			disposed = true;

			shouldNotifyOnWork.Dispose();

            MetricsCounters.Dispose();
			cancellationTokenSource.Dispose();
		}

		public void ClearErrorsFor(string indexName)
		{
			var list = new List<IndexingError>();
			var removed = new List<IndexingError>();

			IndexingError error;
			while (indexingErrors.TryDequeue(out error))
			{
				if (StringComparer.OrdinalIgnoreCase.Equals(error.IndexName, indexName) == false)
					list.Add(error);
				else
					removed.Add(error);
			}

			foreach (var indexingError in list)
			{
				indexingErrors.Enqueue(indexingError);
			}

			TransactionalStorage.Batch(accessor =>
			{
				foreach (var removedError in removed)
				{
					accessor.Lists.Remove("Raven/Indexing/Errors/" + indexName, removedError.Id.ToString(CultureInfo.InvariantCulture));
				}
			});
		}

		public Action<IndexChangeNotification> RaiseIndexChangeNotification { get; set; }

		private bool disposed;
	    private long lastWorkTimeTicks;

	    [CLSCompliant(false)]
        public MetricsCountersManager MetricsCounters { get; private set; }

		public IndexingBatchInfo ReportIndexingBatchStarted(int documentsCount, long documentsSize, List<string> indexesToWorkOn)
		{
			return new IndexingBatchInfo
			{
				IndexesToWorkOn = indexesToWorkOn,
				TotalDocumentCount = documentsCount,
				TotalDocumentSize = documentsSize,
				StartedAt = SystemTime.UtcNow,
				PerformanceStats = new ConcurrentDictionary<string, IndexingPerformanceStats>(),
			};
		}

		public void ReportIndexingBatchCompleted(IndexingBatchInfo batchInfo)
		{
			batchInfo.BatchCompleted();
			lastActualIndexingBatchInfo.Add(batchInfo);
		}

		public ReducingBatchInfo ReportReducingBatchStarted(List<string> indexesToWorkOn)
		{
			return new ReducingBatchInfo
			{
				IndexesToWorkOn = indexesToWorkOn,
				StartedAt = SystemTime.UtcNow,
				PerformanceStats = new ConcurrentDictionary<string, ReducingPerformanceStats[]>()
			};
		}

		public void ReportReducingBatchCompleted(ReducingBatchInfo batchInfo)
		{
			batchInfo.BatchCompleted();
			lastActualReducingBatchInfo.Add(batchInfo);
		}

		public ConcurrentSet<FutureBatchStats> FutureBatchStats
		{
			get { return futureBatchStats; }
		}

		public SizeLimitedConcurrentSet<IndexingBatchInfo> LastActualIndexingBatchInfo
		{
			get { return lastActualIndexingBatchInfo; }
		}

		public SizeLimitedConcurrentSet<ReducingBatchInfo> LastActualReducingBatchInfo
		{
			get { return lastActualReducingBatchInfo; }
		}

		public DocumentDatabase Database { get; set; }
		public DateTime? ShowTimingByDefaultUntil { get; set; }

		public void AddFutureBatch(FutureBatchStats futureBatchStat)
		{
			futureBatchStats.Add(futureBatchStat);
			if (futureBatchStats.Count <= 30)
				return;

			foreach (var source in futureBatchStats.OrderBy(x => x.Timestamp).Take(5))
			{
				futureBatchStats.TryRemove(source);
			}
		}

		public void StopIndexing()
		{
			log.Debug("Stopping indexing workers");
			doIndexing = false;
		    doReducing = false;
			lock (waitForWork)
			{
				Monitor.PulseAll(waitForWork);
			}
		}

        public void StopReducing()
        {
            log.Debug("Stopping reducing workers");
            doReducing = false;
            lock (waitForWork)
            {
                Monitor.PulseAll(waitForWork);
            }
        }

        public void StartIndexing()
		{
			doIndexing = true;
            doReducing = true;
		}

		public void MarkAsRemovedFromIndex(HashSet<string> keys)
		{
			foreach (var key in keys)
			{
				recentlyDeleted.TryRemove(key);
			}
		}

		public bool ShouldRemoveFromIndex(string key)
		{
			var shouldRemoveFromIndex = recentlyDeleted.Contains(key);
			return shouldRemoveFromIndex;
		}

		public void MarkDeleted(string key)
		{
			recentlyDeleted.Add(key);
		}

        public int GetNextQueryId()
        {
            return Interlocked.Increment(ref nextQueryId);
        }
	}
}