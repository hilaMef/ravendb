using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Bundles.Replication.Data;
using Raven.Database.Config;
using Raven.Database.Indexing;
using Raven.Database.Prefetching;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Authorization.Bugs;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3539 : RavenTestBase
	{
		//private EmbeddableDocumentStore documentStore;
		public class Person
		{
			public string Name { get; set; }

		}

		public class Simple : AbstractIndexCreationTask<Person>
		{
			public Simple()
			{
				this.Map = results => from result in results
					select new
					{
						Name = result.Name
					};
			}
		}

		[Fact]
		public void get_debug_package_info()
		{

			using (var documentStore = NewDocumentStore())
			{


				documentStore.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
				{
					Id = "Dba1",
					Settings =
					{
						{"Raven/DataDir", "Dba1"}
					}
				});
				documentStore.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
				{
					Id = "Dba2",
					Settings =
					{
						{"Raven/DataDir", "Dba2"}
					}

				});

				documentStore.DatabaseCommands.EnsureDatabaseExists("Dba2");


				/*	var dbs = new List<ResourceAccess>
				{
					new ResourceAccess
					{
						TenantId = "Dba1",
						Admin = true
					},
					new ResourceAccess
					{
						TenantId = "Dba2",
						Admin = false
					},

				};
				*/

				//var dbWC = documentStore.ServerIfEmbedded.Options.DatabaseLandlord.GetDatabaseInternal("trala").Result.WorkContext;


				var workContext = documentStore.SystemDatabase.WorkContext;


				var prefetchingBehavior = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new IndexBatchSizeAutoTuner(workContext), string.Empty);

				var prefetchingBehavior2 = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new IndependentBatchSizeAutoTuner(workContext, PrefetchingUser.Indexer), string.Empty);

				var prefetchingBehavior3 = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new ReduceBatchSizeAutoTuner(workContext), string.Empty);

				var indexBatchAutoTuner = new IndependentBatchSizeAutoTuner(workContext, prefetchingBehavior.PrefetchingUser);
				var independentBatchSizeAutoTuner = new IndependentBatchSizeAutoTuner(workContext, prefetchingBehavior2.PrefetchingUser);
				var reduceBatchSizeAutoTuner = new IndependentBatchSizeAutoTuner(workContext, prefetchingBehavior3.PrefetchingUser);

				indexBatchAutoTuner.NumberOfItemsToProcessInSingleBatch = 200;
				indexBatchAutoTuner.AutoThrottleBatchSize(300, 1024, TimeSpan.MinValue);
				indexBatchAutoTuner.HandleLowMemory();



				var url = "http://localhost:8079" + "/debug/auto-tuning-info";
				var request = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
					documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
				var response = request.ReadResponseJson();
				//var json = documentStore.JsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams).ReadResponseJson();

				var reasonBatchSizeChanged = response.Values();


				independentBatchSizeAutoTuner.AutoThrottleBatchSize(100, 1024, TimeSpan.MinValue);
				reduceBatchSizeAutoTuner.AutoThrottleBatchSize(500, 1024, TimeSpan.MinValue);
				indexBatchAutoTuner.HandleLowMemory();

				reduceBatchSizeAutoTuner.AutoThrottleBatchSize(800, 2050, TimeSpan.MinValue);
				reduceBatchSizeAutoTuner.HandleLowMemory();
				prefetchingBehavior.OutOfMemoryExceptionHappened();

				var url2 = string.Format("http://localhost:8079/databases/{0}/debug/auto-tuning-info", "Dba1");
				var requestWithDbName = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
					documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
				var response2 = requestWithDbName.ReadResponseJson();
				var json = (RavenJObject) requestWithDbName.ReadResponseJson();

				var reason = json.Value<RavenJArray>("Reason");
				var lowMemoryCallsRecords = json.Value<RavenJArray>("LowMemoryCallsRecords");

				var threeHours = new TimeSpan(3, 30, 0);

				indexBatchAutoTuner.NumberOfItemsToProcessInSingleBatch = 200;
				indexBatchAutoTuner.AutoThrottleBatchSize(300, 1024, threeHours);
				indexBatchAutoTuner.AutoThrottleBatchSize(400, 1024, threeHours);
				indexBatchAutoTuner.HandleLowMemory();
				indexBatchAutoTuner.NumberOfItemsToProcessInSingleBatch = 100;
				indexBatchAutoTuner.AutoThrottleBatchSize(500, 1024, threeHours);
				indexBatchAutoTuner.AutoThrottleBatchSize(800, 1024, threeHours);

				var prefetchingBehavior4 = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new IndexBatchSizeAutoTuner(workContext), string.Empty);
				var indexBatchAutoTuner4 = new IndependentBatchSizeAutoTuner(workContext, prefetchingBehavior4.PrefetchingUser);

				indexBatchAutoTuner.NumberOfItemsToProcessInSingleBatch = 200;
				indexBatchAutoTuner4.AutoThrottleBatchSize(1000, 1024, TimeSpan.MaxValue);
				indexBatchAutoTuner4.HandleLowMemory();
				indexBatchAutoTuner4.NumberOfItemsToProcessInSingleBatch = 200;
				indexBatchAutoTuner4.HandleLowMemory();
				indexBatchAutoTuner4.NumberOfItemsToProcessInSingleBatch = 300;
				indexBatchAutoTuner4.AutoThrottleBatchSize(300, 1024, threeHours);



				var url4 = "http://localhost:8079" + "/debug/auto-tuning-info";
				var request4 = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url4, HttpMethods.Get,
					documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
				var response4 = request4.ReadResponseJson();
				//var reasonBatchSizeChanged4 = response4.Values();

				var url3 = string.Format("http://localhost:8079/databases/{0}/admin/debug/auto-tuning-info", "Dba1");
				var urlAd = string.Format("http://localhost:8079/databases/{0}/admin/debug/auto-tuning-info");
				var requestWithDbName3 = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
					documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
				var response3 = requestWithDbName3.ReadResponseJson();

			}
		}

		[Fact]
		public void get_debug_info_IndexBatchSizeAutoTuner()
		{

			using (var documentStore = NewDocumentStore())
			{


				var workContext = documentStore.SystemDatabase.WorkContext;
				var dbName = workContext.DatabaseName;

				var prefetchingBehavior = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new IndexBatchSizeAutoTuner(workContext), string.Empty);
				var indexBatchAutoTuner = new IndependentBatchSizeAutoTuner(workContext, prefetchingBehavior.PrefetchingUser);


				indexBatchAutoTuner.NumberOfItemsToProcessInSingleBatch = 200;
				var threeHours = new TimeSpan(3, 30, 0);

				indexBatchAutoTuner.AutoThrottleBatchSize(300, 1024, threeHours);
				indexBatchAutoTuner.HandleLowMemory();

				var url = "http://localhost:8079" + "/debug/auto-tuning-info";
				var requestWithDbName = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
					documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));

				var json = (RavenJObject)requestWithDbName.ReadResponseJson();
				var conv = documentStore.Conventions;
				var topology2 = json.JsonDeserialization<AutoTunerInfo>();
				var topology = json.Deserialize<AutoTunerInfo>(conv);

				//var reason = json.Value<RavenJArray>("Reason");
				//var lowMemoryCallsRecords = json.Value<RavenJArray>("LowMemoryCallsRecords");

				


			}
		}
		[Fact]
		public void get_debug_info_IndependentBatchSizeAutoTuner()
		{

			using (var documentStore = NewDocumentStore())
			{
				var workContext = documentStore.SystemDatabase.WorkContext;
				var dbName = workContext.DatabaseName;

				var prefetchingBehavior = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new IndexBatchSizeAutoTuner(workContext), string.Empty);
				var independentBatchSizeAutoTuner = new IndependentBatchSizeAutoTuner(workContext, prefetchingBehavior.PrefetchingUser);

				independentBatchSizeAutoTuner.NumberOfItemsToProcessInSingleBatch = 200;
				//independentBatchSizeAutoTuner.AutoThrottleBatchSize(300,1024);


				var url = "http://localhost:8079/databases/debug/auto-tuning-info";
				var requestWithDbName = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
					documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
				var json = (RavenJObject)requestWithDbName.ReadResponseJson();
				var topology = json.Deserialize<AutoTunerInfo>(documentStore.Conventions);

			
			}
		}
		[Fact]
		public void get_debug_info_ReduceBatchSizeAutoTuner()
		{
			using (var documentStore = NewDocumentStore())
			{

				var workContext = documentStore.SystemDatabase.WorkContext;
				var dbName = workContext.DatabaseName;

				var prefetchingBehavior = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new IndexBatchSizeAutoTuner(workContext), string.Empty);
				var reduceBatchSizeAutoTuner = new ReduceBatchSizeAutoTuner(workContext);


				var url = "http://localhost:8079/databases/debug/auto-tuning-info";
				var requestWithDbName = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
					documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
				var json = (RavenJObject)requestWithDbName.ReadResponseJson();
				var reason = json.Value<RavenJArray>("Reason");
				var lowMemoryCallsRecords = json.Value<RavenJArray>("LowMemoryCallsRecords");

			}
		}
		[Fact]
		public void get_debug_info_ForSpecifiedDatabase()
		{
			using (var documentStore = NewDocumentStore())
			{

				documentStore.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
				{
					Id = "Dba1",
					Settings =
					{
						{"Raven/DataDir", "Dba1"}
					}
				});
				var dbWorkContext = documentStore.ServerIfEmbedded.Options.DatabaseLandlord.GetDatabaseInternal("Dba1").Result.WorkContext;

				var dbName = dbWorkContext.DatabaseName;



				var prefetchingBehavior = new PrefetchingBehavior(PrefetchingUser.Indexer, dbWorkContext, new IndexBatchSizeAutoTuner(dbWorkContext), string.Empty);


				MemoryStatistics.SimulateLowMemoryNotification();

				var prefetchingBehavior2 = new PrefetchingBehavior(PrefetchingUser.Indexer, dbWorkContext, new IndependentBatchSizeAutoTuner(dbWorkContext, PrefetchingUser.Indexer), string.Empty);

				var prefetchingBehavior3 = new PrefetchingBehavior(PrefetchingUser.Indexer, dbWorkContext, new ReduceBatchSizeAutoTuner(dbWorkContext), string.Empty);

				var indexBatchAutoTuner = new IndependentBatchSizeAutoTuner(dbWorkContext, prefetchingBehavior.PrefetchingUser);
				var independentBatchSizeAutoTuner = new IndependentBatchSizeAutoTuner(dbWorkContext, prefetchingBehavior2.PrefetchingUser);
				var reduceBatchSizeAutoTuner = new IndependentBatchSizeAutoTuner(dbWorkContext, prefetchingBehavior3.PrefetchingUser);



				independentBatchSizeAutoTuner.AutoThrottleBatchSize(100, 1024, TimeSpan.MinValue);
				
				reduceBatchSizeAutoTuner.AutoThrottleBatchSize(500, 1024, TimeSpan.MinValue);
				indexBatchAutoTuner.HandleLowMemory();


				reduceBatchSizeAutoTuner.AutoThrottleBatchSize(100, 1024, TimeSpan.MinValue);
				prefetchingBehavior3.HandleLowMemory();
			
				indexBatchAutoTuner.AutoThrottleBatchSize(100, 1024, TimeSpan.MinValue);
				prefetchingBehavior2.HandleLowMemory();

				prefetchingBehavior.OutOfMemoryExceptionHappened();
				prefetchingBehavior.HandleLowMemory();
				MemoryStatistics.SimulateLowMemoryNotification();
				

				Thread.Sleep(1000);
				var url = string.Format("http://localhost:8079/databases/{0}/debug/auto-tuning-info", dbName);
				var requestWithDbName = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
					documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
				var results = requestWithDbName.ReadResponseJson().JsonDeserialization<AutoTunerInfo>();

				

				var reason = results.Reason;
				var lowMemoryRecords = results.LowMemoryCallsRecords.First().Operations;
		


			}
		}
	}
}
