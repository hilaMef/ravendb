using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Raven.Database.Prefetching;
using Raven.Database.Server;
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
					documentStore.DatabaseCommands.EnsureDatabaseExists("Dba1");

				var workContext = documentStore.SystemDatabase.WorkContext;

			//	var dbName = documentStore.DatabaseCommands.GlobalAdmin.Commands.Admin.GetDatabaseConfiguration();

				var prefetchingBehavior = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new IndexBatchSizeAutoTuner(workContext), string.Empty);

				var prefetchingBehavior2 = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new IndependentBatchSizeAutoTuner(workContext,PrefetchingUser.Indexer), string.Empty);

				var prefetchingBehavior3 = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new ReduceBatchSizeAutoTuner(workContext), string.Empty);
				
				var indexBatchAutoTuner = new IndependentBatchSizeAutoTuner(workContext, prefetchingBehavior.PrefetchingUser);
				var independentBatchSizeAutoTuner = new IndependentBatchSizeAutoTuner(workContext, prefetchingBehavior2.PrefetchingUser);
				var reduceBatchSizeAutoTuner = new IndependentBatchSizeAutoTuner(workContext, prefetchingBehavior3.PrefetchingUser);

				indexBatchAutoTuner.NumberOfItemsToProcessInSingleBatch = 200;
				indexBatchAutoTuner.AutoThrottleBatchSize(300, 1024, TimeSpan.MinValue);
				indexBatchAutoTuner.HandleLowMemory();

				independentBatchSizeAutoTuner.AutoThrottleBatchSize(100, 1024, TimeSpan.MinValue);
				reduceBatchSizeAutoTuner.AutoThrottleBatchSize(500, 1024, TimeSpan.MinValue);
			
				reduceBatchSizeAutoTuner.AutoThrottleBatchSize(800, 2050, TimeSpan.MinValue);

				prefetchingBehavior.OutOfMemoryExceptionHappened();

				var name = prefetchingBehavior.GetStats().Name;
				//var url = ""
				var url = "http://localhost:8079" + "/debug/auto-tuning-info";
				var request = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
					documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
				var response = request.ReadResponseJson();
				var reasonBatchSizeChanged = response.Values();

				//databases/{databaseName}/debug/auto-tuning-info

				var url2 = string.Format("http://localhost:8079/databases/{0}/debug/auto-tuning-info", "Dba1");
				var requestWithDbName = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url2, HttpMethods.Get,
					documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));

				var response2 = requestWithDbName.ReadResponseJson();
				var reasonBatchSizeChanged2 = response2.Values();
			

			}
		}
	}
}
