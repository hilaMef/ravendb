﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Core.BulkInsert;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Core.ChangesApi
{
    public class WebsocketsTests : RavenTestBase
    {
        [Fact]
        public async Task Can_connect_via_websockets_and_receive_heartbeat()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var clientWebSocket = TryCreateClientWebSocket())
                {
                    var url = store.Url.Replace("http:", "ws:");
                    url = url + "/changes/websocket?id=" + Guid.NewGuid();
                    await clientWebSocket.ConnectAsync(new Uri(url), CancellationToken.None);

                    var buffer = new byte[1024];
                    var result = await clientWebSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                    var message = Encoding.UTF8.GetString(buffer, 0, result.Count);

                    Assert.Contains("Heartbeat", message);
                }
            }
        }

        public class Node
        {
            public string Name { get; set; }
        }

        [Fact]
        public void AreWebsocketsDestroyedAfterGC()
        {
            var counter = new ConcurrentQueue<BulkInsertChangeNotification>();

            using (var store = NewRemoteDocumentStore())
            {
                Stopwatch testTimer;
                var mre = new ManualResetEventSlim();
                using (var bulkInsert = store.BulkInsert(store.DefaultDatabase))
                {
                    store.Changes().ForBulkInsert(bulkInsert.OperationId).Subscribe(x =>
                    {
                        counter.Enqueue(x);
                        mre.Set();
                    });

                    do
                    {
                        bulkInsert.Store(new ChunkedBulkInsert.Node
                        {
                            Name = "Parent"
                        });
                    } while (!mre.IsSet);

                    testTimer = Stopwatch.StartNew();

                    IssueGCRequest(store);

                    bulkInsert.Store(new ChunkedBulkInsert.Node
                    {
                        Name = "Parent"
                    });

                    const int maxMillisecondsToWaitUntilConnectionRestores = 1000;
                    //wait until connection restores
                    IEnumerable<RavenJToken> response;
                    var sw = Stopwatch.StartNew();
                    do
                    {
                        response = IssueGetChangesRequest(store);
                    } while (response == null ||
                             !response.Any() ||
                             sw.ElapsedMilliseconds <= maxMillisecondsToWaitUntilConnectionRestores);

                    //sanity check, if the test fails here, then something is wrong
                    response.Should().NotBeEmpty("if it is null or empty then it means the connection did not restore after 1 second by itself. Should be investigated.");

                    var connectionAge = TimeSpan.Parse(response.First().Value<string>("Age"));
                    var timeSinceTestStarted = TimeSpan.FromMilliseconds(testTimer.ElapsedMilliseconds);
                    connectionAge.Should().BeLessThan(timeSinceTestStarted);
                }
            }
        }

        private static DateTime GetLastForcedGCDateTimeRequest(DocumentStore store)
        {
            var request = store.JsonRequestFactory.CreateHttpJsonRequest(
                new CreateHttpJsonRequestParams(null,
                    store.Url + "/debug/gc-info",
                    "GET",
                    store.DatabaseCommands.PrimaryCredentials,
                    store.Conventions));

            var response = request.ReadResponseJson();
            return response.Value<DateTime>("LastForcedGCTime");

        }

        private static IEnumerable<RavenJToken> IssueGetChangesRequest(DocumentStore store)
        {
            var getChangesRequest = store
                .JsonRequestFactory
                .CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
                    store.Url.ForDatabase(store.DefaultDatabase) + "/debug/changes",
                    "GET",
                    store.DatabaseCommands.PrimaryCredentials,
                    store.Conventions));

            var getChangesResponse = (RavenJArray)getChangesRequest.ReadResponseJson();
            return getChangesResponse;
        }

        private static void IssueGCRequest(DocumentStore store)
        {
            var gcRequest = store
                .JsonRequestFactory
                .CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
                    store.Url.ForDatabase(null) + "/admin/gc",
                    "GET",
                    store.DatabaseCommands.PrimaryCredentials,
                    store.Conventions));
            var gcResponse = gcRequest.ReadResponseBytesAsync();
            gcResponse.Wait();
        }

        private static ClientWebSocket TryCreateClientWebSocket()
        {
            try
            {
                return new ClientWebSocket();
            }
            catch (PlatformNotSupportedException)
            {
                throw new SkipException("Cannot run this test on this platform");
            }
        }
    }
}