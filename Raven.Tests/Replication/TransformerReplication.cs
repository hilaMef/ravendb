﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Tasks;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Helpers;

using Xunit;

namespace Raven.Tests.Replication
{
	public class TransformerReplication : RavenTestBase
	{
		public class UserWithExtraInfo
		{
			public string Id { get; set; }

			public string Name { get; set; }

			public string Address { get; set; }
		}

		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class UserWithoutExtraInfoTransformer : AbstractTransformerCreationTask<UserWithExtraInfo>
		{
			public UserWithoutExtraInfoTransformer()
			{
				TransformResults = usersWithExtraInfo => from u in usersWithExtraInfo
														 select new
														 {
															 u.Id,
															 u.Name
														 };
			}
		}

		public class AnotherTransformer : AbstractTransformerCreationTask<UserWithExtraInfo>
		{
			public AnotherTransformer()
			{
				TransformResults = usersWithExtraInfo => from u in usersWithExtraInfo
														 select new
														 {
															 u.Id,
															 u.Name
														 };
			}
		}

		public class YetAnotherTransformer : AbstractTransformerCreationTask<UserWithExtraInfo>
		{
			public YetAnotherTransformer()
			{
				TransformResults = usersWithExtraInfo => from u in usersWithExtraInfo
														 select new
														 {
															 u.Id,
															 u.Name
														 };
			}
		}



		[Fact]
		public void Should_replicate_transformers_by_default()
		{
			using (var sourceServer = GetNewServer(8077))
			using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
			using (var destinationServer1 = GetNewServer(8078))
			using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1))
			using (var destinationServer2 = GetNewServer())
			using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2))
			using (var destinationServer3 = GetNewServer(8081))
			using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3))
			{
				CreateDatabaseWithReplication(source, "testDB");
				CreateDatabaseWithReplication(destination1, "testDB");
				CreateDatabaseWithReplication(destination2, "testDB");
				CreateDatabaseWithReplication(destination3, "testDB");

				// ReSharper disable once AccessToDisposedClosure
				SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

				var transformer = new UserWithoutExtraInfoTransformer();
				transformer.Execute(source.DatabaseCommands.ForDatabase("testDB"), source.Conventions);

				var transformersOnDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(1, transformersOnDestination1.Count(x => x.Name == transformer.TransformerName));

				var transformersOnDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(1, transformersOnDestination2.Count(x => x.Name == transformer.TransformerName));

				var transformersOnDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(1, transformersOnDestination3.Count(x => x.Name == transformer.TransformerName));
			}
		}

		[Fact]
		public async Task Should_replicate_transformer_deletion()
		{
			using (var sourceServer = GetNewServer(8077))
			using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
			using (var destinationServer1 = GetNewServer(8078))
			using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1))
			using (var destinationServer2 = GetNewServer())
			using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2))
			using (var destinationServer3 = GetNewServer(8081))
			using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3))
			{
				source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

				CreateDatabaseWithReplication(source, "testDB");
				CreateDatabaseWithReplication(destination1, "testDB");
				CreateDatabaseWithReplication(destination2, "testDB");
				CreateDatabaseWithReplication(destination3, "testDB");

				// ReSharper disable once AccessToDisposedClosure
				SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

				var transformer = new UserWithoutExtraInfoTransformer();
				transformer.Execute(source.DatabaseCommands.ForDatabase("testDB"), source.Conventions);

				var sourceDB = await sourceServer.Server.GetDatabaseInternal("testDB");
				var replicationTask = sourceDB.StartupTasks.OfType<ReplicationTask>().First();
				replicationTask.TimeToWaitBeforeSendingDeletesOfIndexesToSiblings = TimeSpan.Zero;
				SpinWait.SpinUntil(() => replicationTask.ReplicateIndexesAndTransformersTask(null));

				var transformersOnDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(1, transformersOnDestination1.Count(x => x.Name == transformer.TransformerName));

				var transformersOnDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(1, transformersOnDestination2.Count(x => x.Name == transformer.TransformerName));

				var transformersOnDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(1, transformersOnDestination3.Count(x => x.Name == transformer.TransformerName));

				//now delete the transformer at the source and verify that the deletion is replicated
				source.DatabaseCommands.ForDatabase("testDB").DeleteTransformer(transformer.TransformerName);
				SpinWait.SpinUntil(() => replicationTask.ReplicateIndexesAndTransformersTask(null));

				transformersOnDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(0, transformersOnDestination1.Count(x => x.Name == transformer.TransformerName));

				transformersOnDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(0, transformersOnDestination2.Count(x => x.Name == transformer.TransformerName));

				transformersOnDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(0, transformersOnDestination3.Count(x => x.Name == transformer.TransformerName));

			}
		}

		[Fact]
		public void Should_skip_transformer_replication_if_flag_is_set()
		{
			using (var sourceServer = GetNewServer(8077))
			using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
			using (var destinationServer1 = GetNewServer(8078))
			using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1))
			using (var destinationServer2 = GetNewServer())
			using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2))
			using (var destinationServer3 = GetNewServer(8081))
			using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3))
			{
				CreateDatabaseWithReplication(source, "testDB");
				CreateDatabaseWithReplication(destination1, "testDB");
				CreateDatabaseWithReplication(destination2, "testDB");
				CreateDatabaseWithReplication(destination3, "testDB");

				// ReSharper disable once AccessToDisposedClosure
				SetupReplication(source, "testDB", store => destination2 == store, destination1, destination2, destination3);

				var transformer = new UserWithoutExtraInfoTransformer();
				transformer.Execute(source.DatabaseCommands.ForDatabase("testDB"), source.Conventions);

				var transformersOnDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(1, transformersOnDestination1.Count(x => x.Name == transformer.TransformerName));

				var transformersOnDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(0, transformersOnDestination2.Length);

				var transformersOnDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(1, transformersOnDestination3.Count(x => x.Name == transformer.TransformerName));
			}
		}

		[Fact]
		public void Transformer_replication_should_respect_skip_replication_flag()
		{
			using (var sourceServer = GetNewServer(8077))
			using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
			using (var destinationServer1 = GetNewServer(8078))
			using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1))
			using (var destinationServer2 = GetNewServer())
			using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2))
			using (var destinationServer3 = GetNewServer(8081))
			using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3))
			{
				CreateDatabaseWithReplication(source, "testDB");
				CreateDatabaseWithReplication(destination1, "testDB");
				CreateDatabaseWithReplication(destination2, "testDB");
				CreateDatabaseWithReplication(destination3, "testDB");

				// ReSharper disable once AccessToDisposedClosure
				SetupReplication(source, "testDB", store => store == destination2, destination1, destination2, destination3);

				var transformer = new UserWithoutExtraInfoTransformer();
				transformer.Execute(source.DatabaseCommands.ForDatabase("testDB"), source.Conventions);

				var transformersOnDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(1, transformersOnDestination1.Count(x => x.Name == transformer.TransformerName));

				var transformersOnDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(0, transformersOnDestination2.Length);

				var transformersOnDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				Assert.Equal(1, transformersOnDestination3.Count(x => x.Name == transformer.TransformerName));
			}

		}

		[Fact]
		public async Task Should_replicate_all_transformers_periodically()
		{
			using (var sourceServer = GetNewServer(8077))
			using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
			using (var destinationServer1 = GetNewServer(8078))
			using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1))
			using (var destinationServer2 = GetNewServer())
			using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2))
			using (var destinationServer3 = GetNewServer(8081))
			using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3))
			{
				CreateDatabaseWithReplication(source, "testDB");
				CreateDatabaseWithReplication(destination1, "testDB");
				CreateDatabaseWithReplication(destination2, "testDB");
				CreateDatabaseWithReplication(destination3, "testDB");

				//turn-off automatic index replication - precaution
				source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;
				// ReSharper disable once AccessToDisposedClosure
				SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

				var userTransformer = new UserWithoutExtraInfoTransformer();
				var anotherTransformer = new AnotherTransformer();
				var yetAnotherTransformer = new YetAnotherTransformer();

				source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());
				source.DatabaseCommands.ForDatabase("testDB").PutTransformer(anotherTransformer.TransformerName, anotherTransformer.CreateTransformerDefinition());
				source.DatabaseCommands.ForDatabase("testDB").PutTransformer(yetAnotherTransformer.TransformerName, yetAnotherTransformer.CreateTransformerDefinition());

				var sourceDB = await sourceServer.Server.GetDatabaseInternal("testDB");
				var replicationTask = sourceDB.StartupTasks.OfType<ReplicationTask>().First();
				SpinWait.SpinUntil(() => replicationTask.ReplicateIndexesAndTransformersTask(null));

				var expectedTransformerNames = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase)
						{ userTransformer.TransformerName, 
							anotherTransformer.TransformerName, 
							yetAnotherTransformer.TransformerName };

				var transformerNamesAtDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				var transformerNamesAtDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				var transformerNamesAtDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);

				Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination1.Select(x => x.Name).ToArray()));
				Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination2.Select(x => x.Name).ToArray()));
				Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination3.Select(x => x.Name).ToArray()));
			}
		}

		[Fact]
		public void Should_replicate_all_transformers_only_to_specific_destination_if_relevant_endpoint_is_hit()
		{
			var requestFactory = new HttpRavenRequestFactory();
			using (var sourceServer = GetNewServer(8077))
			using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer, fiddler: true))
			using (var destinationServer1 = GetNewServer(8078))
			using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1, fiddler: true))
			using (var destinationServer2 = GetNewServer())
			using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2, fiddler: true))
			using (var destinationServer3 = GetNewServer(8081))
			using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3, fiddler: true))
			{
				CreateDatabaseWithReplication(source, "testDB");
				CreateDatabaseWithReplication(destination1, "testDB");
				CreateDatabaseWithReplication(destination2, "testDB");
				CreateDatabaseWithReplication(destination3, "testDB");

				//make sure replication is off for indexes/transformers
				source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

				var userTransformer = new UserWithoutExtraInfoTransformer();
				var anotherTransformer = new AnotherTransformer();
				var yetAnotherTransformer = new YetAnotherTransformer();

				source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());
				source.DatabaseCommands.ForDatabase("testDB").PutTransformer(anotherTransformer.TransformerName, anotherTransformer.CreateTransformerDefinition());
				source.DatabaseCommands.ForDatabase("testDB").PutTransformer(yetAnotherTransformer.TransformerName, yetAnotherTransformer.CreateTransformerDefinition());

				var expectedTransformerNames = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase)
						{ userTransformer.TransformerName, 
							anotherTransformer.TransformerName, 
							yetAnotherTransformer.TransformerName };

				// ReSharper disable once AccessToDisposedClosure
				var destinations = SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

				var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all-to-destination", source.Url);
				var replicationRequest = requestFactory.Create(replicationRequestUrl, "POST", new RavenConnectionStringOptions
				{
					Url = source.Url
				});
				replicationRequest.Write(RavenJObject.FromObject(destinations[1]));
				replicationRequest.ExecuteRequest();

				var transformerNamesAtDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				var transformerNamesAtDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				var transformerNamesAtDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);

				Assert.Equal(0, transformerNamesAtDestination1.Length);
				Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination2.Select(x => x.Name).ToArray()));
				Assert.Equal(0, transformerNamesAtDestination3.Length);
			}
		}

		[Fact]
		public void Should_replicate_all_transformers_if_relevant_endpoint_is_hit()
		{
			var requestFactory = new HttpRavenRequestFactory();
			using (var sourceServer = GetNewServer(8077))
			using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer, fiddler: true))
			using (var destinationServer1 = GetNewServer(8078))
			using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1, fiddler: true))
			using (var destinationServer2 = GetNewServer())
			using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2, fiddler: true))
			using (var destinationServer3 = GetNewServer(8081))
			using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3, fiddler: true))
			{
				CreateDatabaseWithReplication(source, "testDB");
				CreateDatabaseWithReplication(destination1, "testDB");
				CreateDatabaseWithReplication(destination2, "testDB");
				CreateDatabaseWithReplication(destination3, "testDB");

				//make sure replication is off for indexes/transformers
				source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

				var userTransformer = new UserWithoutExtraInfoTransformer();
				var anotherTransformer = new AnotherTransformer();
				var yetAnotherTransformer = new YetAnotherTransformer();

				source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());
				source.DatabaseCommands.ForDatabase("testDB").PutTransformer(anotherTransformer.TransformerName, anotherTransformer.CreateTransformerDefinition());
				source.DatabaseCommands.ForDatabase("testDB").PutTransformer(yetAnotherTransformer.TransformerName, yetAnotherTransformer.CreateTransformerDefinition());

				var expectedTransformerNames = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase)
						{ userTransformer.TransformerName, 
							anotherTransformer.TransformerName, 
							yetAnotherTransformer.TransformerName };

				// ReSharper disable once AccessToDisposedClosure
				SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

				var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all", source.Url);
				var replicationRequest = requestFactory.Create(replicationRequestUrl, "POST", new RavenConnectionStringOptions
				{
					Url = source.Url
				});
				replicationRequest.ExecuteRequest();

				var transformerNamesAtDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				var transformerNamesAtDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				var transformerNamesAtDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);

				Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination1.Select(x => x.Name).ToArray()));
				Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination2.Select(x => x.Name).ToArray()));
				Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination3.Select(x => x.Name).ToArray()));
			}
		}

		[Fact]
		public void Replicate_all_transformers_should_respect_disable_replication_flag()
		{
			var requestFactory = new HttpRavenRequestFactory();
			using (var sourceServer = GetNewServer(8077))
			using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer, fiddler: true))
			using (var destinationServer1 = GetNewServer(8078))
			using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1, fiddler: true))
			using (var destinationServer2 = GetNewServer())
			using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2, fiddler: true))
			using (var destinationServer3 = GetNewServer(8081))
			using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3, fiddler: true))
			{
				CreateDatabaseWithReplication(source, "testDB");
				CreateDatabaseWithReplication(destination1, "testDB");
				CreateDatabaseWithReplication(destination2, "testDB");
				CreateDatabaseWithReplication(destination3, "testDB");

				//make sure replication is off for indexes/transformers
				source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

				var userTransformer = new UserWithoutExtraInfoTransformer();
				var anotherTransformer = new AnotherTransformer();
				var yetAnotherTransformer = new YetAnotherTransformer();

				source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());
				source.DatabaseCommands.ForDatabase("testDB").PutTransformer(anotherTransformer.TransformerName, anotherTransformer.CreateTransformerDefinition());
				source.DatabaseCommands.ForDatabase("testDB").PutTransformer(yetAnotherTransformer.TransformerName, yetAnotherTransformer.CreateTransformerDefinition());

				var expectedTransformerNames = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase)
						{ userTransformer.TransformerName, 
							anotherTransformer.TransformerName, 
							yetAnotherTransformer.TransformerName };

				// ReSharper disable once AccessToDisposedClosure
				SetupReplication(source, "testDB", store => store == destination2, destination1, destination2, destination3);

				var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all", source.Url);
				var replicationRequest = requestFactory.Create(replicationRequestUrl, "POST", new RavenConnectionStringOptions
				{
					Url = source.Url
				});
				replicationRequest.ExecuteRequest();

				var transformerNamesAtDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				var transformerNamesAtDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
				var transformerNamesAtDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);

				Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination1.Select(x => x.Name).ToArray()));
				Assert.Equal(0, transformerNamesAtDestination2.Length);
				Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination3.Select(x => x.Name).ToArray()));
			}
		}

		private static void CreateDatabaseWithReplication(DocumentStore store, string databaseName)
		{
			store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
			{
				Id = databaseName,
				Settings =
				{
					{"Raven/DataDir", "~/Tenants/" + databaseName},
					{"Raven/ActiveBundles", "Replication"}
				}
			});
		}

		private static List<ReplicationDestination> SetupReplication(IDocumentStore source, string databaseName, Func<IDocumentStore, bool> shouldSkipIndexReplication, params IDocumentStore[] destinations)
		{
			var replicationDocument = new ReplicationDocument
			{
				Destinations = new List<ReplicationDestination>(destinations.Select(destination =>
					new ReplicationDestination
					{
						Database = databaseName,
						Url = destination.Url,
						SkipIndexReplication = shouldSkipIndexReplication(destination)
					}))

			};

			using (var session = source.OpenSession(databaseName))
			{
				session.Store(replicationDocument, Constants.RavenReplicationDestinations);
				session.SaveChanges();
			}

			return replicationDocument.Destinations;
		}
	}
}
