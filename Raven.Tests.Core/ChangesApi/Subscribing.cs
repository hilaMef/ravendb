﻿using System.Threading;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Core.Replication;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using System;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Xunit;using Raven.Tests.Core.Utils.Transformers;
using Sparrow.Collections;

namespace Raven.Tests.Core.ChangesApi
{
    public class Subscribing : RavenReplicationCoreTest
    {
        private ConcurrentSet<string> output= new ConcurrentSet<string>(), output2 = new ConcurrentSet<string>();

        [Fact]
        public void CanSubscribeToDocumentChanges()
        {
            using (var store = GetDocumentStore())
            {

                store.Changes().Task.Result
                    .ForAllDocuments()
                    .Subscribe(change =>
                    {
                        output.Add("passed_foralldocuments");
                    });

                store.Changes().Task.Result
                    .ForDocumentsStartingWith("companies")
                    .Subscribe(change => 
                    {
                        output.Add("passed_forfordocumentsstartingwith");
                    });

                store.Changes().Task.Result
                    .ForDocumentsInCollection("posts")
                    .Subscribe(change =>
                    {
                        output.Add("passed_ForDocumentsInCollection");
                    });

                store.Changes().Task.Result
                    .ForDocumentsOfType(new Camera().GetType())
                    .Subscribe(changes =>
                    {
                        output.Add("passed_ForDocumentsOfType");
                    });

                store.Changes().Task.Result
                    .ForDocument("companies/1")
                    .Subscribe(change => 
                    {
                        if (change.Type == DocumentChangeTypes.Delete)
                        {
                            output.Add("passed_fordocumentdelete");
                        }
                    });

                using (var session = store.OpenSession())
                {
                    session.Store(new User 
                    {
                        Id = "users/1"
                    });
                    session.SaveChanges();
                    WaitUntilOutput("passed_foralldocuments");

                    session.Store(new Company
                    {
                        Id = "companies/1"
                    });
                    session.SaveChanges();
                    WaitUntilOutput("passed_forfordocumentsstartingwith");

                    session.Store(new Post
                    {
                        Id = "posts/1"
                    });
                    session.SaveChanges();
                    WaitUntilOutput("passed_ForDocumentsInCollection");

                    session.Store(new Camera
                    {
                        Id = "cameras/1"
                    });
                    session.SaveChanges();
                    WaitUntilOutput("passed_ForDocumentsOfType");

                    session.Delete("companies/1");
                    session.SaveChanges();
                    WaitUntilOutput("passed_fordocumentdelete");
                }
            }
        }

        private void WaitUntilOutput(string expected)
        {
            Assert.True(SpinWait.SpinUntil(() => output.Contains(expected), 5000));
        }

        private void WaitUntilOutput2(string expected)
        {
            Assert.True(SpinWait.SpinUntil(() => output2.Contains(expected), 5000));
        }

        [Fact]
        public void CanSubscribeToIndexChanges()
        {
            using (var store = GetDocumentStore())
            {
                store.Changes().Task.Result
                    .ForAllIndexes().Task.Result
                    .Subscribe(change => 
                    {
                        Console.WriteLine(JsonConvert.SerializeObject(change));
                        if (change.Type == IndexChangeTypes.IndexAdded)
                        {
                            output.Add("passed_forallindexesadded");
                        }
                    });

                new Companies_CompanyByType().Execute(store);
                WaitForIndexing(store);
                WaitUntilOutput("passed_forallindexesadded");

                var usersByName = new Users_ByName();
                usersByName.Execute(store);
                WaitForIndexing(store);
                store.Changes().Task.Result
                    .ForIndex(usersByName.IndexName).Task.Result
                    .Subscribe(change =>
                    {
                        Console.WriteLine(JsonConvert.SerializeObject(change));
                        if (change.Type == IndexChangeTypes.MapCompleted)
                        {
                            output .Add("passed_forindexmapcompleted");
                        }
                    });

                var companiesSompanyByType = new Companies_CompanyByType();
                companiesSompanyByType.Execute(store);
                WaitForIndexing(store);
                store.Changes().Task.Result
                    .ForIndex(companiesSompanyByType.IndexName).Task.Result
                    .Subscribe(change =>
                    {
                        Console.WriteLine(JsonConvert.SerializeObject(change));
                        if (change.Type == IndexChangeTypes.RemoveFromIndex)
                        {
                            output2.Add("passed_forindexremovecompleted");
                        }
                        if (change.Type == IndexChangeTypes.ReduceCompleted)
                        {
                            output.Add("passed_forindexreducecompleted");
                        }
                    });

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "user", LastName = "user" });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    WaitUntilOutput("passed_forindexmapcompleted");

                    session.Store(new Company { Id = "companies/1", Name = "company", Type = Company.CompanyType.Public });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    WaitUntilOutput("passed_forindexreducecompleted");

                    session.Delete("companies/1");
                    session.SaveChanges();
                    WaitForIndexing(store);
                    WaitUntilOutput2("passed_forindexremovecompleted");
                }


                store.Changes().Task.Result
                    .ForAllIndexes().Task.Result
                    .Subscribe(change =>
                    {
                        if (change.Type == IndexChangeTypes.IndexRemoved)
                        {
                            output.Add("passed_forallindexesremoved");
                        }
                    });
                store.DatabaseCommands.DeleteIndex("Companies/CompanyByType");
                WaitForIndexing(store);
                Assert.Contains("passed_forallindexesremoved", output);
            }
        }

        [Fact]
        public void CanSubscribeToReplicationConflicts()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                source.DatabaseCommands.Put("docs/1", null, new RavenJObject() { { "Key", "Value" } }, new RavenJObject());
                destination.DatabaseCommands.Put("docs/1", null, new RavenJObject() { { "Key", "Value" } }, new RavenJObject());

                var eTag = source.DatabaseCommands.Get("docs/1").Etag;

                destination.Changes().Task.Result
                    .ForAllReplicationConflicts().Task.Result
                    .Subscribe(conflict =>
                    {
                        output.Add("conflict");
                    });

                SetupReplication(source, destinations: destination);
                source.Replication.WaitAsync(eTag, replicas: 1).Wait();

                WaitUntilOutput("conflict");
            }
        }

        [Fact]
        public void CanSubscribeToBulkInsert()
        {
            using (var store = GetDocumentStore())
            {
	            using (var bulkInsert = store.BulkInsert())
	            {
		            store.Changes().Task.Result
			            .ForBulkInsert(bulkInsert.OperationId).Task.Result
			            .Subscribe(changes =>
			            {
				            output.Add("passed_bulkInsert");
			            });

		            bulkInsert.Store(new User
		            {
			            Name = "User"
		            });

	            }

				// perform the check after dispose of bulk insert operation to make sure that we already flushed everything to the server to the notification should already arrive
				WaitUntilOutput("passed_bulkInsert");
            }
        }

		[Fact]
		public void CanSubscribeToAnyBulkInsert()
		{
			using (var store = GetDocumentStore())
			{
				var bulkEndedCount = 0;
				var bulkStartedCount = 0;

				store.Changes().Task.Result
						.ForBulkInsert().Task.Result
						.Subscribe(changes =>
						{
							if(changes.Type == DocumentChangeTypes.BulkInsertEnded)
								Interlocked.Increment(ref bulkEndedCount);
							else if (changes.Type == DocumentChangeTypes.BulkInsertStarted)
								Interlocked.Increment(ref bulkStartedCount);
						});

				using (var bulkInsert = store.BulkInsert())
				{
					bulkInsert.Store(new User
					{
						Name = "User"
					});
				}

				using (var bulkInsert = store.BulkInsert())
				{
					bulkInsert.Store(new User
					{
						Name = "User"
					});
				}

				// perform the check after dispose of bulk insert operation to make sure that we already flushed everything to the server to the notification should already arrive
				Assert.True(SpinWait.SpinUntil(() => bulkStartedCount == 2 && bulkEndedCount == 2, TimeSpan.FromSeconds(20)));
			}
		}

        [Fact]
        public void CanSubscribeToAllTransformers()
        {
            using (var store = GetDocumentStore())
            {
                store.Changes().Task.Result
                    .ForAllTransformers().Task.Result
                    .Subscribe(changes => 
                    {
                        if (changes.Type == TransformerChangeTypes.TransformerAdded)
                        {
                            output.Add("passed_CanSubscribeToAllTransformers_TransformerAdded");
                        }
                        if (changes.Type == TransformerChangeTypes.TransformerRemoved)
                        {
                            output.Add("passed_CanSubscribeToAllTransformers_TransformerRemoved");
                        }
                    });

                var transformer = new CompanyFullAddressTransformer();
                transformer.Execute(store);
                WaitUntilOutput("passed_CanSubscribeToAllTransformers_TransformerAdded");

                store.DatabaseCommands.DeleteTransformer(transformer.TransformerName);
                WaitUntilOutput("passed_CanSubscribeToAllTransformers_TransformerRemoved");
            }
        }

		[Fact]
		public void CanSubscribeToAllDataSubscriptions()
		{
			using (var store = GetDocumentStore())
			{
				store.Changes().Task.Result
					.ForAllDataSubscriptions().Task.Result
					.Subscribe(changes =>
					{
						if (changes.Type == DataSubscriptionChangeTypes.SubscriptionOpened)
						{
							output.Add("passed_CanSubscribeToAllDataSubscriptions_SubscriptionOpened");
						}
						if (changes.Type == DataSubscriptionChangeTypes.SubscriptionReleased)
						{
							output.Add("passed_CanSubscribeToAllDataSubscriptions_SubscriptionReleased");
						}
					});

				var id = store.Subscriptions.Create(new SubscriptionCriteria());
				var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions());

				WaitUntilOutput("passed_CanSubscribeToAllDataSubscriptions_SubscriptionOpened");

				subscription.Dispose();

				WaitUntilOutput("passed_CanSubscribeToAllDataSubscriptions_SubscriptionReleased");
			}
		}

		[Fact]
		public void CanSubscribeToSpecificDataSubscriptionChanges()
		{
			using (var store = GetDocumentStore())
			{
				var id = store.Subscriptions.Create(new SubscriptionCriteria());

				var id2 = store.Subscriptions.Create(new SubscriptionCriteria());

				store.Changes().Task.Result
					.ForDataSubscription(1).Task.Result
					.Subscribe(changes =>
					{
						if (changes.Type == DataSubscriptionChangeTypes.SubscriptionOpened)
						{
							output.Add("passed_CanSubscribeToAllDataSubscriptions_SubscriptionOpened_" + changes.Id);
						}
						if (changes.Type == DataSubscriptionChangeTypes.SubscriptionReleased)
						{
						    output.Add("passed_CanSubscribeToAllDataSubscriptions_SubscriptionReleased_" + changes.Id);
						}
					});

				var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions());
				var subscription2 = store.Subscriptions.Open(id2, new SubscriptionConnectionOptions());

				WaitUntilOutput("passed_CanSubscribeToAllDataSubscriptions_SubscriptionOpened_" + id);

				subscription.Dispose();
				subscription2.Dispose();

				WaitUntilOutput("passed_CanSubscribeToAllDataSubscriptions_SubscriptionReleased_" + id);
			}
		}
    }
}
