﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using System.Web.Http.Controllers;
using System.Web.Http.Routing;
using ICSharpCode.NRefactory.CSharp;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Bundles.SqlReplication;
using Raven.Database.Linq;
using Raven.Database.Linq.Ast;
using Raven.Database.Server.WebApi;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;
using IOExtensions = Raven.Database.Extensions.IOExtensions;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class DebugController : RavenDbApiController
	{

		[HttpGet]
		[RavenRoute("debug/cache-details")]
		[RavenRoute("databases/{databaseName}/debug/cache-details")]
		public HttpResponseMessage CacheDetails()
		{
			return GetMessageWithObject(Database.TransactionalStorage.DocumentCacher.GetStatistics());
		}

		[HttpGet]
		[RavenRoute("debug/enable-query-timing")]
		[RavenRoute("databases/{databaseName}/debug/enable-query-timing")]
		public HttpResponseMessage EnableQueryTiming()
		{
			var time = SystemTime.UtcNow + TimeSpan.FromMinutes(5);
			Database.WorkContext.ShowTimingByDefaultUntil = time;
			return GetMessageWithObject(new { Enabled = true, Until = time });
		}

		[HttpGet]
		[RavenRoute("debug/disable-query-timing")]
		[RavenRoute("databases/{databaseName}/debug/disable-query-timing")]
		public HttpResponseMessage DisableQueryTiming()
		{
			Database.WorkContext.ShowTimingByDefaultUntil = null;
			return GetMessageWithObject(new { Enabled = false });
		}

		[HttpGet]
		[RavenRoute("debug/prefetch-status")]
		[RavenRoute("databases/{databaseName}/debug/prefetch-status")]
		public HttpResponseMessage PrefetchingQueueStatus()
		{
			return GetMessageWithObject(DebugInfoProvider.GetPrefetchingQueueStatusForDebug(Database));
		}

		[HttpPost]
		[RavenRoute("debug/format-index")]
		[RavenRoute("databases/{databaseName}/debug/format-index")]
		public async Task<HttpResponseMessage> FormatIndex()
		{
			RavenJArray array;

			try
			{
				array = await ReadJsonArrayAsync();
			}
			catch (InvalidOperationException e)
			{
				Log.DebugException("Failed to deserialize debug request.", e);
				return GetMessageWithObject(new
				{
					Message = "Could not understand json, please check its validity."
				}, (HttpStatusCode)422); //http code 422 - Unprocessable entity

			}
			catch (InvalidDataException e)
			{
				Log.DebugException("Failed to deserialize debug request." , e);
				return GetMessageWithObject(new
				{
					e.Message
				}, (HttpStatusCode)422); //http code 422 - Unprocessable entity
			}

			var results = new string[array.Length];
			for (int i = 0; i < array.Length; i++)
			{
				var value = array[i].Value<string>();
				try
				{
					results[i] = IndexPrettyPrinter.Format(value);
				}
				catch (Exception e)
				{
					results[i] = "Could not format:" + Environment.NewLine +
								 value + Environment.NewLine + e;
				}
			}

			return GetMessageWithObject(results);
		}

		/// <remarks>
		/// as we sum data we have to guarantee that we don't sum the same record twice on client side.
		/// to prevent such situation we don't send data from current second
		/// </remarks>
		/// <returns></returns>
		[HttpGet]
		[RavenRoute("debug/sql-replication-perf-stats")]
		[RavenRoute("databases/{databaseName}/debug/sql-replication-perf-stats")]
		public HttpResponseMessage SqlReplicationPerfStats()
		{
			var now = SystemTime.UtcNow;
			var nowTruncToSeconds = new DateTime(now.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, now.Kind);

			var sqlReplicationTask = Database.StartupTasks.OfType<SqlReplicationTask>().FirstOrDefault();
			if (sqlReplicationTask == null)
			{
				return GetMessageWithString("Unable to find SQL Replication task. Maybe it is not enabled?", HttpStatusCode.BadRequest);
			}

			var stats = from nameAndStatsManager in sqlReplicationTask.SqlReplicationMetricsCounters
						from perf in nameAndStatsManager.Value.ReplicationPerformanceStats
						where perf.Started < nowTruncToSeconds
						let k = new { Name = nameAndStatsManager.Key, perf }
						group k by k.perf.Started.Ticks / TimeSpan.TicksPerSecond
							into g
							orderby g.Key
							select new
							{
								Started = new DateTime(g.Key * TimeSpan.TicksPerSecond, DateTimeKind.Utc),
								Stats = from k in g
										group k by k.Name into gg
										select new
										{
											ReplicationName = gg.Key,
											DurationMilliseconds = gg.Sum(x => x.perf.DurationMilliseconds),
											BatchSize = gg.Sum(x => x.perf.BatchSize)
										}
							};
			return GetMessageWithObject(stats);
		}

		/// <remarks>
		/// as we sum data we have to guarantee that we don't sum the same record twice on client side.
		/// to prevent such situation we don't send data from current second
		/// </remarks>
		/// <returns></returns>
		[HttpGet]
		[RavenRoute("debug/replication-perf-stats")]
		[RavenRoute("databases/{databaseName}/debug/replication-perf-stats")]
		public HttpResponseMessage ReplicationPerfStats()
		{
			var now = SystemTime.UtcNow;
			var nowTruncToSeconds = new DateTime(now.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, now.Kind);

			var stats = from nameAndStatsManager in Database.WorkContext.MetricsCounters.ReplicationPerformanceStats
						from perf in nameAndStatsManager.Value
						where perf.Started < nowTruncToSeconds
						let k = new { Name = nameAndStatsManager.Key, perf }
						group k by k.perf.Started.Ticks / TimeSpan.TicksPerSecond
							into g
							orderby g.Key
							select new
							{
								Started = new DateTime(g.Key * TimeSpan.TicksPerSecond, DateTimeKind.Utc),
								Stats = from k in g
										group k by k.Name into gg
										select new
										{
											Destination = gg.Key,
											DurationMilliseconds = gg.Sum(x => x.perf.DurationMilliseconds),
											BatchSize = gg.Sum(x => x.perf.BatchSize)
										}
							};
			return GetMessageWithObject(stats);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <remarks>
		/// as we sum data we have to guarantee that we don't sum the same record twice on client side.
		/// to prevent such situation we don't send data from current second
		/// </remarks>
		/// <param name="format"></param>
		/// <returns></returns>
		[HttpGet]
		[RavenRoute("debug/indexing-perf-stats")]
		[RavenRoute("databases/{databaseName}/debug/indexing-perf-stats")]
		public HttpResponseMessage IndexingPerfStats(string format = "json")
		{
			var now = SystemTime.UtcNow;
			var nowTruncToSeconds = new DateTime(now.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, now.Kind);

			var databaseStatistics = Database.Statistics;
			var stats = from index in databaseStatistics.Indexes
						from perf in index.Performance
						where (perf.Operation == "Map" || perf.Operation == "Index") && perf.Started < nowTruncToSeconds
						let k = new { index, perf }
						group k by k.perf.Started.Ticks / TimeSpan.TicksPerSecond into g
						orderby g.Key
						select new
						{
							Started = new DateTime(g.Key * TimeSpan.TicksPerSecond, DateTimeKind.Utc),
							Stats = from k in g
									group k by k.index.Name into gg
									select new
									{
										Index = gg.Key,
										DurationMilliseconds = gg.Sum(x => x.perf.DurationMilliseconds),
										InputCount = gg.Sum(x => x.perf.InputCount),
										OutputCount = gg.Sum(x => x.perf.OutputCount),
										ItemsCount = gg.Sum(x => x.perf.ItemsCount)
									}
						};

			switch (format)
			{
				case "csv":
				case "CSV":
					var sw = new StringWriter();
					sw.WriteLine();
					foreach (var stat in stats)
					{
						sw.WriteLine(stat.Started.ToString("o"));
						sw.WriteLine("Index, Duration (ms), Input, Output, Items");
						foreach (var indexStat in stat.Stats)
						{
							sw.Write('"');
							sw.Write(indexStat.Index);
							sw.Write("\",{0},{1},{2},{3}", indexStat.DurationMilliseconds, indexStat.InputCount, indexStat.OutputCount, indexStat.ItemsCount);
							sw.WriteLine();
						}
						sw.WriteLine();
					}
					var msg = sw.GetStringBuilder().ToString();
					return new HttpResponseMessage(HttpStatusCode.OK)
					{
						Content = new MultiGetSafeStringContent(msg)
						{
							Headers =
							{
								ContentType = new MediaTypeHeaderValue("text/plain")
							}
						}
					};
				default:
					return GetMessageWithObject(stats);
			}
		}


		[HttpGet]
		[RavenRoute("debug/indexing-batch-stats")]
		[RavenRoute("databases/{databaseName}/debug/indexing-batch-stats")]
		public HttpResponseMessage IndexingBatchStats()
		{
			var indexingBatches = Database.WorkContext.LastActualIndexingBatchInfo.ToArray();

			return GetMessageWithObject(indexingBatches);
		}

		[HttpGet]
		[RavenRoute("debug/reducing-batch-stats")]
		[RavenRoute("databases/{databaseName}/debug/reducing-batch-stats")]
		public HttpResponseMessage ReducingBatchStats()
		{
			var reducingBatches = Database.WorkContext.LastActualReducingBatchInfo.ToArray();

			return GetMessageWithObject(reducingBatches);
		}

		[HttpGet]
		[RavenRoute("debug/plugins")]
		[RavenRoute("databases/{databaseName}/debug/plugins")]
		public HttpResponseMessage Plugins()
		{
			return GetMessageWithObject(Database.PluginsInfo);
		}

		[HttpGet]
		[RavenRoute("debug/changes")]
		[RavenRoute("databases/{databaseName}/debug/changes")]
		public HttpResponseMessage Changes()
		{
			return GetMessageWithObject(Database.TransportState.DebugStatuses);
		}

		[HttpGet]
		[RavenRoute("debug/sql-replication-stats")]
		[RavenRoute("databases/{databaseName}/debug/sql-replication-stats")]
		public HttpResponseMessage SqlReplicationStats()
		{
			var task = Database.StartupTasks.OfType<SqlReplicationTask>().FirstOrDefault();
			if (task == null)
				return GetMessageWithObject(new
				{
					Error = "SQL Replication bundle is not installed"
				}, HttpStatusCode.NotFound);


			//var metrics = task.SqlReplicationMetricsCounters.ToDictionary(x => x.Key, x => x.Value.ToSqlReplicationMetricsData());

			var statisticsAndMetrics = task.GetConfiguredReplicationDestinations().Select(x =>
			{
				SqlReplicationStatistics stats;
				task.Statistics.TryGetValue(x.Name, out stats);
				var metrics = task.GetSqlReplicationMetricsManager(x).ToSqlReplicationMetricsData();
				return new
				{
					x.Name,
					Statistics = stats,
					Metrics = metrics
				};
			});
			return GetMessageWithObject(statisticsAndMetrics);
		}


		[HttpGet]
		[RavenRoute("debug/metrics")]
		[RavenRoute("databases/{databaseName}/debug/metrics")]
		public HttpResponseMessage Metrics()
		{
			return GetMessageWithObject(Database.CreateMetrics());
		}

		[HttpGet]
		[RavenRoute("debug/config")]
		[RavenRoute("databases/{databaseName}/debug/config")]
		public HttpResponseMessage Config()
		{
			if (CanExposeConfigOverTheWire() == false)
			{
				return GetEmptyMessage(HttpStatusCode.Forbidden);
			}

			return GetMessageWithObject(DebugInfoProvider.GetConfigForDebug(Database));
		}

		[HttpGet]
		[RavenRoute("debug/raw-doc")]
		[RavenRoute("databases/{databaseName}/debug/raw-doc")]
		public HttpResponseMessage RawDocBytes()
		{
			var docId = GetQueryStringValue("id");
			if (String.IsNullOrWhiteSpace(docId))
				return GetMessageWithObject(new
				{
					Error = "Query string 'id' is mandatory"
				}, HttpStatusCode.BadRequest);


			bool hasDoc = false;
			Database.TransactionalStorage.Batch(accessor =>
			{
				using (var s = accessor.Documents.RawDocumentByKey(docId))
				{
					hasDoc = s != null;
				}
			});

			if (hasDoc == false)
			{
				return GetMessageWithObject(new
				{
					Error = "No document with id " + docId + " was found"
				}, HttpStatusCode.NotFound);
			}

			return new HttpResponseMessage(HttpStatusCode.OK)
			{
				Content = new PushStreamContent((stream, content, transportContext) => Database.TransactionalStorage.Batch(accessor =>
				{
					using(stream)
					using (var docContent = accessor.Documents.RawDocumentByKey(docId))
					{
						docContent.CopyTo(stream);
						stream.Flush();
					}
				}))
				{
					Headers =
					{
						ContentType = new MediaTypeHeaderValue("application/octet-stream"),
						ContentDisposition = new ContentDispositionHeaderValue("attachment")
						{
							FileName = docId +".raw-doc"
						}
					}
				}
			};
		}

	    [HttpGet]
	    [RavenRoute("debug/slow-dump-ref-csv")]
		[RavenRoute("databases/{databaseName}/debug/slow-dump-ref-csv")]
        public HttpResponseMessage DumpRefsToCsv(int sampleCount = 10)
	    {
		    return new HttpResponseMessage
		    {
			    Content = new PushStreamContent((stream, content, context) =>
			    {
					using (var writer = new StreamWriter(stream))
					{
						writer.WriteLine("ref count,document key,sample references");
						Database.TransactionalStorage.Batch(accessor =>
						{
							accessor.Indexing.DumpAllReferancesToCSV(writer, sampleCount);
						});
						writer.Flush();
                        stream.Flush();
					}
			    })
			    {
				    Headers =
				    {
					    ContentDisposition = new ContentDispositionHeaderValue("attachment")
					    {
						    FileName = "doc-refs.csv",
					    },
					    ContentType = new MediaTypeHeaderValue("application/octet-stream")
				    }
			    }
		    };
	    }

		[HttpGet]
		[RavenRoute("debug/docrefs")]
		[RavenRoute("databases/{databaseName}/debug/docrefs")]
		public HttpResponseMessage DocRefs(string id)
		{
		    var op = GetQueryStringValue("op");
			op = op == "from" ? "from" : "to";

			var totalCountReferencing = -1;
			List<string> results = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				totalCountReferencing = accessor.Indexing.GetCountOfDocumentsReferencing(id);
				var documentsReferencing =
					op == "from"
					? accessor.Indexing.GetDocumentsReferencesFrom(id)
					: accessor.Indexing.GetDocumentsReferencing(id);
				results = documentsReferencing.Skip(GetStart()).Take(GetPageSize(Database.Configuration.MaxPageSize)).ToList();
			});

			return GetMessageWithObject(new
			{
				TotalCountReferencing = totalCountReferencing,
				Results = results
			});
		}

        //DumpAllReferancesToCSV
		[HttpGet]
		[RavenRoute("debug/d0crefs-t0ps")]
		[RavenRoute("databases/{databaseName}/debug/d0crefs-t0ps")]
		public HttpResponseMessage DocRefsTops()
		{
			var sp = Stopwatch.StartNew();
			Dictionary<string, int> documentReferencesStats = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				documentReferencesStats = accessor.Indexing.GetDocumentReferencesStats();
			});

			return GetMessageWithObject(new
			{
				TotalReferences = documentReferencesStats.Count,
				GenerationCost = sp.Elapsed,
				Results = documentReferencesStats.OrderByDescending(x => x.Value)
					.Select(x => new { Document = x.Key, Count = x.Value })
			});
		}

		[HttpPost]
		[RavenRoute("debug/index-fields")]
		[RavenRoute("databases/{databaseName}/debug/index-fields")]
		public async Task<HttpResponseMessage> IndexFields()
		{
			var indexStr = await ReadStringAsync();
			bool querySyntax = indexStr.Trim().StartsWith("from");
			var mapDefinition = querySyntax
				? QueryParsingUtils.GetVariableDeclarationForLinqQuery(indexStr, true)
				: QueryParsingUtils.GetVariableDeclarationForLinqMethods(indexStr, true);

			var captureSelectNewFieldNamesVisitor = new CaptureSelectNewFieldNamesVisitor(querySyntax == false, new HashSet<string>(), new Dictionary<string, Expression>());
			mapDefinition.AcceptVisitor(captureSelectNewFieldNamesVisitor, null);

			return GetMessageWithObject(new { FieldNames = captureSelectNewFieldNamesVisitor.FieldNames });
		}

		[HttpGet]
		[RavenRoute("debug/list")]
		[RavenRoute("databases/{databaseName}/debug/list")]
		public HttpResponseMessage List(string id)
		{
			var listName = id;
			var key = InnerRequest.RequestUri.ParseQueryString()["key"];
			if (key == null)
				throw new ArgumentException("Key query string variable is mandatory");

			ListItem listItem = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				listItem = accessor.Lists.Read(listName, key);
			});

			if (listItem == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			return GetMessageWithObject(listItem);
		}

		[HttpGet]
		[RavenRoute("debug/list-all")]
		[RavenRoute("databases/{databaseName}/debug/list-all")]
		public HttpResponseMessage ListAll(string id)
		{
			var listName = id;

			List<ListItem> listItems = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				listItems = accessor.Lists.Read(listName, Etag.Empty, null, GetPageSize(Database.Configuration.MaxPageSize)).ToList();
			});

			if (listItems == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			return GetMessageWithObject(listItems);
		}

		[HttpGet]
		[RavenRoute("debug/queries")]
		[RavenRoute("databases/{databaseName}/debug/queries")]
		public HttpResponseMessage Queries()
		{
			return GetMessageWithObject(Database.WorkContext.CurrentlyRunningQueries);
		}

		[HttpGet]
		[RavenRoute("debug/suggest-index-merge")]
		[RavenRoute("databases/{databaseName}/debug/suggest-index-merge")]
		public HttpResponseMessage IndexMerge()
		{
			var mergeIndexSuggestions = Database.WorkContext.IndexDefinitionStorage.ProposeIndexMergeSuggestions();
			return GetMessageWithObject(mergeIndexSuggestions);
		}

		[HttpGet]
		[RavenRoute("debug/sl0w-d0c-c0unts")]
		[RavenRoute("databases/{databaseName}/debug/sl0w-d0c-c0unts")]
		public HttpResponseMessage SlowDocCounts()
		{
			DebugDocumentStats stat = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				stat = accessor.Documents.GetDocumentStatsVerySlowly();
			});

			return GetMessageWithObject(stat);
		}

		[HttpGet]
		[RavenRoute("debug/user-info")]
		[RavenRoute("databases/{databaseName}/debug/user-info")]
		public HttpResponseMessage UserInfo()
		{
			var userInfo = GetUserInfo();
			return GetMessageWithObject(userInfo);
		}

		[HttpGet]
		[RavenRoute("debug/tasks")]
		[RavenRoute("databases/{databaseName}/debug/tasks")]
		public HttpResponseMessage Tasks()
		{
			return GetMessageWithObject(DebugInfoProvider.GetTasksForDebug(Database));
		}

		[HttpGet]
		[RavenRoute("debug/routes")]
		[Description(@"Output the debug information for all the supported routes in Raven Server.")]
		public HttpResponseMessage Routes()
		{
			var routes = new SortedDictionary<string, RouteInfo>();

			foreach (var route in ControllerContext.Configuration.Routes)
			{
				var inner = route as IEnumerable<IHttpRoute>;
				if (inner == null) continue;

				foreach (var httpRoute in inner)
				{
					var key = httpRoute.RouteTemplate;
					bool forDatabase = false;
					if (key.StartsWith("databases/{databaseName}/"))
					{
						key = key.Substring("databases/{databaseName}/".Length);
						forDatabase = true;
					}
					var data = new RouteInfo(key);
					if (routes.ContainsKey(key))
						data = routes[key];

					if (forDatabase)
						data.CanRunForSpecificDatabase = true;

					var actions = ((IEnumerable)httpRoute.DataTokens["actions"]).OfType<ReflectedHttpActionDescriptor>();

					foreach (var reflectedHttpActionDescriptor in actions)
					{

						foreach (var httpMethod in reflectedHttpActionDescriptor.SupportedHttpMethods)
						{
							if (data.Methods.Any(method => method.Name == httpMethod.Method))
								continue;

							string description = null;
							var descriptionAttibute =
								reflectedHttpActionDescriptor.MethodInfo.CustomAttributes.FirstOrDefault(attributeData => attributeData.AttributeType == typeof(DescriptionAttribute));
							if (descriptionAttibute != null)
								description = descriptionAttibute.ConstructorArguments[0].Value.ToString();

							data.Methods.Add(new Method
							{
								Name = httpMethod.Method,
								Description = description
							});
						}
					}

					routes[key] = data;
				}
			}

			return GetMessageWithObject(routes);
		}

		[HttpGet]
		[RavenRoute("debug/currently-indexing")]
		[RavenRoute("databases/{databaseName}/debug/currently-indexing")]
		public HttpResponseMessage CurrentlyIndexing()
		{
			return GetMessageWithObject(DebugInfoProvider.GetCurrentlyIndexingForDebug(Database));
		}

		[HttpGet]
		[RavenRoute("debug/remaining-reductions")]
		[RavenRoute("databases/{databaseName}/debug/remaining-reductions")]
		public HttpResponseMessage CurrentlyRemainingReductions()
		{
			return GetMessageWithObject(Database.GetRemainingScheduledReductions());
		}

		[HttpGet]
		[RavenRoute("debug/clear-remaining-reductions")]
		[RavenRoute("databases/{databaseName}/debug/clear-remaining-reductions")]
		public HttpResponseMessage ResetRemainingReductionsTracking()
		{
		    Database.TransactionalStorage.ResetScheduledReductionsTracking();
		    return GetEmptyMessage();
		}

		[HttpGet]
		[RavenRoute("debug/request-tracing")]
		[RavenRoute("databases/{databaseName}/debug/request-tracing")]
		public HttpResponseMessage RequestTracing()
		{
			if (CanExposeConfigOverTheWire() == false)
			{
				return GetEmptyMessage(HttpStatusCode.Forbidden);
			}

			return GetMessageWithObject(DebugInfoProvider.GetRequestTrackingForDebug(RequestManager, DatabaseName));
		}

		[HttpGet]
		[RavenRoute("debug/identities")]
		[RavenRoute("databases/{databaseName}/debug/identities")]
		public HttpResponseMessage Identities()
		{
			var start = GetStart();
			var pageSize = GetPageSize(1024);

			long totalCount = 0;
			IEnumerable<KeyValuePair<string, long>> identities = null;
			Database.TransactionalStorage.Batch(accessor => identities = accessor.General.GetIdentities(start, pageSize, out totalCount));

			return GetMessageWithObject(new
										{
											TotalCount = totalCount,
											Identities = identities
										});
		}

		[HttpGet]
		[RavenRoute("databases/{databaseName}/debug/info-package")]
		[RavenRoute("debug/info-package")]
		public HttpResponseMessage InfoPackage()
		{
			if (CanExposeConfigOverTheWire() == false)
			{
				return GetEmptyMessage(HttpStatusCode.Forbidden);
			}

			var tempFileName = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
			try
			{
				using (var file = new FileStream(tempFileName, FileMode.Create))
				using (var package = new ZipArchive(file, ZipArchiveMode.Create))
				{
					DebugInfoProvider.CreateInfoPackageForDatabase(package, Database, RequestManager);
				}

				var response = new HttpResponseMessage();

				response.Content = new StreamContent(new FileStream(tempFileName, FileMode.Open, FileAccess.Read))
								   {
									   Headers =
									   {
										   ContentDisposition = new ContentDispositionHeaderValue("attachment")
																{
																	FileName = string.Format("Debug-Info-{0}.zip", SystemTime.UtcNow),
																},
										   ContentType = new MediaTypeHeaderValue("application/octet-stream")
									   }
								   };

				return response;
			}
			finally
			{
				IOExtensions.DeleteFile(tempFileName);
			}
		}

		[HttpGet]
		[RavenRoute("databases/{databaseName}/debug/transactions")]
		[RavenRoute("debug/transactions")]
		public HttpResponseMessage Transactions()
		{
			return GetMessageWithObject(new
			{
				PreparedTransactions = Database.TransactionalStorage.GetPreparedTransactions()
			});
		}

		[HttpGet]
		[RavenRoute("debug/subscriptions")]
		[RavenRoute("databases/{databaseName}/debug/subscriptions")]
		public HttpResponseMessage Subscriptions()
		{
			return GetMessageWithObject(Database.Subscriptions.GetDebugInfo());
		}

		[HttpGet]
		[RavenRoute("debug/gc-info")]
		public HttpResponseMessage GCInfo()
		{
			return GetMessageWithObject(new GCInfo {LastForcedGCTime = RavenGC.LastForcedGCTime, MemoryBeforeLastForcedGC = RavenGC.MemoryBeforeLastForcedGC, MemoryAfterLastForcedGC = RavenGC.MemoryAfterLastForcedGC});
		}
	}

	public class RouteInfo
	{
		public string Key { get; set; }
		public List<Method> Methods { get; set; }

		public bool CanRunForSpecificDatabase { get; set; }

		public RouteInfo(string key)
		{
			Key = key;
			Methods = new List<Method>();
		}
	}

	public class Method
	{
		public string Name { get; set; }
		public string Description { get; set; }
	}
}