﻿// -----------------------------------------------------------------------
//  <copyright file="Subscription.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Implementation;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Json.Linq;
using Sparrow.Collections;

namespace Raven.Client.Document
{
	public delegate void BeforeBatch();

	public delegate void AfterBatch(int documentsProcessed);

	public delegate bool BeforeAcknowledgment();

	public delegate void AfterAcknowledgment(Etag lastProcessedEtag);

	public class Subscription<T> : IObservable<T>, IDisposableAsync, IDisposable where T : class 
	{
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		private readonly AutoResetEvent newDocuments = new AutoResetEvent(false);
		private readonly ManualResetEvent anySubscriber = new ManualResetEvent(false);
		private readonly IAsyncDatabaseCommands commands;
		private readonly IDatabaseChanges changes;
		private readonly DocumentConvention conventions;
		private readonly Func<Task> ensureOpenSubscription;
		private readonly ConcurrentSet<IObserver<T>> subscribers = new ConcurrentSet<IObserver<T>>();
		private readonly SubscriptionConnectionOptions options;
		private readonly CancellationTokenSource cts = new CancellationTokenSource();
		private readonly GenerateEntityIdOnTheClient generateEntityIdOnTheClient;
		private readonly bool isStronglyTyped;
		private readonly long id;
		private Task pullingTask;
		private Task startPullingTask;
		private IDisposable putDocumentsObserver;
		private IDisposable endedBulkInsertsObserver;
		private IDisposable dataSubscriptionReleasedObserver;
		private bool completed;
		private bool disposed;
		private bool firstConnection = true;

		public event BeforeBatch BeforeBatch = delegate { };
		public event AfterBatch AfterBatch = delegate { };
		public event BeforeAcknowledgment BeforeAcknowledgment = () => true;
		public event AfterAcknowledgment AfterAcknowledgment = delegate { };

		internal Subscription(long id, string database, SubscriptionConnectionOptions options, IAsyncDatabaseCommands commands, IDatabaseChanges changes, DocumentConvention conventions, bool open, Func<Task> ensureOpenSubscription)
		{
			this.id = id;
			this.options = options;
			this.commands = commands;
			this.changes = changes;
			this.conventions = conventions;
			this.ensureOpenSubscription = ensureOpenSubscription;

			if (typeof (T) != typeof (RavenJObject))
			{
				isStronglyTyped = true;
				generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(conventions, entity => AsyncHelpers.RunSync(() => conventions.GenerateDocumentKeyAsync(database, commands, entity)));
			}

			if (open)
				Start();
			else
			{
				if (options.Strategy != SubscriptionOpeningStrategy.WaitForFree)
					throw new InvalidOperationException("Subscription isn't open while its opening strategy is: " + options.Strategy);
			}

			if (options.Strategy == SubscriptionOpeningStrategy.WaitForFree)
				WaitForSubscriptionReleased();
		}

		private void Start()
		{
			StartWatchingDocs();
			startPullingTask = StartPullingDocs();
		}

		private Task PullDocuments()
		{
			return Task.Run(async () =>
			{
				try
				{
                    Etag lastProcessedEtagOnClient = null;

					while (true)
					{
						anySubscriber.WaitOne();

						cts.Token.ThrowIfCancellationRequested();

						var pulledDocs = false;

						Etag lastProcessedEtagOnServer = null;
						int processedDocs = 0;

						using (var subscriptionRequest = CreatePullingRequest())
						using (var response = await subscriptionRequest.ExecuteRawResponseAsync().ConfigureAwait(false))
						{
							await response.AssertNotFailingResponse().ConfigureAwait(false);

							using (var responseStream = await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false))
							{
								cts.Token.ThrowIfCancellationRequested();

								using (var streamedDocs = new AsyncServerClient.YieldStreamResults(subscriptionRequest, responseStream, customizedEndResult: reader =>
								{
									if (Equals("LastProcessedEtag", reader.Value) == false)
										return false;

									lastProcessedEtagOnServer = Etag.Parse(AsyncHelpers.RunSync(reader.ReadAsString));

									return true;
								}))
								{
									while (await streamedDocs.MoveNextAsync().ConfigureAwait(false))
									{
										if (pulledDocs == false) // first doc in batch
											BeforeBatch();

										pulledDocs = true;

										cts.Token.ThrowIfCancellationRequested();

										var jsonDoc = streamedDocs.Current;

										T instance = null;

										foreach (var subscriber in subscribers)
										{
											try
											{
												if (isStronglyTyped)
												{
													if (instance == null)
													{
														instance = jsonDoc.Deserialize<T>(conventions);

														var docId = jsonDoc[Constants.Metadata].Value<string>("@id");

														if (string.IsNullOrEmpty(docId) == false)
															generateEntityIdOnTheClient.TrySetIdentity(instance, docId);
													}
												
													subscriber.OnNext(instance);
												}
												else
												{
													subscriber.OnNext((T) (object) jsonDoc);
												}
											}
											catch (Exception ex)
											{
												logger.WarnException("Subscriber threw an exception", ex);

												if (options.IgnoreSubscribersErrors == false)
												{
													IsErroredBecauseOfSubscriber = true;
													LastSubscriberException = ex;

													try
													{
														subscriber.OnError(ex);
													}
													catch (Exception)
													{
														// can happen if a subscriber doesn't have an onError handler - just ignore it
													}
													break;
												}
											}
										}

										processedDocs++;

										if (IsErroredBecauseOfSubscriber)
											break;
									}
								}
							}
						}

						if (IsErroredBecauseOfSubscriber)
							break;

                        if (lastProcessedEtagOnServer != null)
						{
                            if (pulledDocs)
                            {
                                // This is an acknowledge when the server returns documents to the subscriber.
                                if (BeforeAcknowledgment())
                                {
                                    AcknowledgeBatchToServer(lastProcessedEtagOnServer);

                                    AfterAcknowledgment(lastProcessedEtagOnServer);
                                }

                                AfterBatch(processedDocs);

                                continue; // try to pull more documents from subscription
                            }
                            else
                            {
                                if (lastProcessedEtagOnClient != lastProcessedEtagOnServer)
                                {
                                    // This is a silent acknowledge, this can happen because there was no documents in range
                                    // to be accessible in the time available. This condition can happen when documents must match
                                    // a set of conditions to be eligible.
                                    AcknowledgeBatchToServer(lastProcessedEtagOnServer);

                                    lastProcessedEtagOnClient = lastProcessedEtagOnServer;
                                    
                                    continue; // try to pull more documents from subscription
                                }
                            }							
						}

						while (newDocuments.WaitOne(options.ClientAliveNotificationInterval) == false)
						{
							using (var clientAliveRequest = CreateClientAliveRequest())
							{
								clientAliveRequest.ExecuteRequest();
							}
						}
					}
				}
				catch (ErrorResponseException e)
				{
					SubscriptionException subscriptionException;
					if (AsyncDocumentSubscriptions.TryGetSubscriptionException(e, out subscriptionException))
						throw subscriptionException;

					throw;
				}
			});
		}

        private void AcknowledgeBatchToServer(Etag lastProcessedEtagOnServer)
        {
            using (var acknowledgmentRequest = CreateAcknowledgmentRequest(lastProcessedEtagOnServer))
            {
                try
                {
                    acknowledgmentRequest.ExecuteRequest();
                }
                catch (Exception)
                {
                    if (acknowledgmentRequest.ResponseStatusCode != HttpStatusCode.RequestTimeout) // ignore acknowledgment timeouts
                        throw;
                }
            }
        }


		/// <summary>
		/// It indicates if the subscription is in errored state because one of subscribers threw an exception.
		/// </summary>
		public bool IsErroredBecauseOfSubscriber { get; private set; }

		/// <summary>
		/// The last exception thrown by one of subscribers.
		/// </summary>
		public Exception LastSubscriberException { get; private set; }

		/// <summary>
		/// The last subscription connection exception.
		/// </summary>
		public Exception SubscriptionConnectionException { get; private set; }

		/// <summary>
		/// It determines if the subscription connection is closed.
		/// </summary>
		public bool IsConnectionClosed { get; private set; }

		private async Task StartPullingDocs()
		{
			SubscriptionConnectionException = null;

			pullingTask = PullDocuments().ObserveException();

			try
			{
				await pullingTask.ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (cts.Token.IsCancellationRequested)
					return;

				if (TryHandleRejectedConnection(ex))
					return;

				RestartPullingTask().ConfigureAwait(false);
			}

			if (IsErroredBecauseOfSubscriber)
			{
				try
				{
					startPullingTask = null; // prevent from calling Wait() on this in Dispose because we are already inside this task
					await DisposeAsync().ConfigureAwait(false);
				}
				catch (Exception e)
				{
					logger.WarnException("Exception happened during an attempt to close subscription after it had become faulted", e);
				}
			}
		}

		private async Task RestartPullingTask()
		{
			await Time.Delay(options.TimeToWaitBeforeConnectionRetry).ConfigureAwait(false);
			try
			{
				await ensureOpenSubscription().ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				if (TryHandleRejectedConnection(ex))
					return;

				RestartPullingTask().ConfigureAwait(false);
				return;
			}

			startPullingTask = StartPullingDocs().ObserveException();
		}

		private bool TryHandleRejectedConnection(Exception ex)
		{
			SubscriptionConnectionException = ex;

			if (ex is SubscriptionInUseException || // another client has connected to the subscription
				ex is SubscriptionDoesNotExistException ||  // subscription has been deleted meanwhile
			    ex is SubscriptionClosedException) // someone forced us to drop the connection by calling Subscriptions.Release
			{
				IsConnectionClosed = true;

				startPullingTask = null; // prevent from calling Wait() on this in Dispose because we can be already inside this task
				pullingTask = null; // prevent from calling Wait() on this in Dispose because we can be already inside this task

				Dispose();

				return true;
			}

			return false;
		}

		private void StartWatchingDocs()
		{
			changes.ConnectionStatusChanged += ChangesApiConnectionChanged;

			var allDocsObservable = changes.ForAllDocuments();

			putDocumentsObserver = allDocsObservable.Subscribe(notification =>
			{
				if (notification.Type == DocumentChangeTypes.Put && notification.Id.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase) == false)
				{
					newDocuments.Set();
				}
			});

			var bulkInsertObservable = changes.ForBulkInsert();
			endedBulkInsertsObserver = bulkInsertObservable.Subscribe(notification =>
			{
				if (notification.Type == DocumentChangeTypes.BulkInsertEnded)
				{
					newDocuments.Set();
				}
			});

			Task.WaitAll(new Task[]
			{
				allDocsObservable.Task, bulkInsertObservable.Task
			});
		}

		private void WaitForSubscriptionReleased()
		{
			var dataSubscriptionObservable = changes.ForDataSubscription(id);

			dataSubscriptionReleasedObserver = dataSubscriptionObservable.Subscribe(notification =>
			{
				if (notification.Type == DataSubscriptionChangeTypes.SubscriptionReleased)
				{
					try
					{
						ensureOpenSubscription().Wait();
					}
					catch (Exception)
					{
						return;
					}

					// succeeded in opening the subscription
					
					// no longer need to be notified about subscription status changes
					dataSubscriptionReleasedObserver.Dispose();
					dataSubscriptionReleasedObserver = null;

					// start standard stuff
					Start();
				}
			});

			dataSubscriptionObservable.Task.Wait();
		}

		private void ChangesApiConnectionChanged(object sender, EventArgs e)
		{
			if (firstConnection)
			{
				firstConnection = false;
				return;
			}

			var changesApi = (RemoteDatabaseChanges) sender;

			if (changesApi.Connected)
				newDocuments.Set();
		}

		public IDisposable Subscribe(IObserver<T> observer)
		{
			if(IsErroredBecauseOfSubscriber)
				throw new InvalidOperationException("Subscription encountered errors and stopped. Cannot add any subscriber.");

			if (subscribers.TryAdd(observer))
			{
				if (subscribers.Count == 1)
					anySubscriber.Set();
			}

			return new DisposableAction(() =>
			{
				subscribers.TryRemove(observer);
				if (subscribers.Count == 0)
					anySubscriber.Reset();
			});
		}

		private HttpJsonRequest CreateAcknowledgmentRequest(Etag lastProcessedEtag)
		{
			return commands.CreateRequest(string.Format("/subscriptions/acknowledgeBatch?id={0}&lastEtag={1}&connection={2}", id, lastProcessedEtag, options.ConnectionId), "POST");
		}

		private HttpJsonRequest CreatePullingRequest()
		{
			return commands.CreateRequest(string.Format("/subscriptions/pull?id={0}&connection={1}", id, options.ConnectionId), "GET");
		}

		private HttpJsonRequest CreateClientAliveRequest()
		{
			return commands.CreateRequest(string.Format("/subscriptions/client-alive?id={0}&connection={1}", id, options.ConnectionId), "PATCH");
		}

		private HttpJsonRequest CreateCloseRequest()
		{
			return commands.CreateRequest(string.Format("/subscriptions/close?id={0}&connection={1}", id, options.ConnectionId), "POST");
		}

		private void OnCompletedNotification()
		{
			if(completed)
				return;

			foreach (var subscriber in subscribers)
			{
				subscriber.OnCompleted();
			}

			completed = true;
		}

		public void Dispose()
		{
			if (disposed)
				return;

			DisposeAsync().Wait();
		}

		public Task DisposeAsync()
		{
			if (disposed)
				return new CompletedTask();

			disposed = true;

			OnCompletedNotification();

			subscribers.Clear();

			if (putDocumentsObserver != null)
				putDocumentsObserver.Dispose();

			if(endedBulkInsertsObserver != null)
				endedBulkInsertsObserver.Dispose();

			if(dataSubscriptionReleasedObserver != null)
				dataSubscriptionReleasedObserver.Dispose();

			cts.Cancel();

			newDocuments.Set();
			anySubscriber.Set();

			changes.ConnectionStatusChanged -= ChangesApiConnectionChanged;

			foreach (var task in new []{pullingTask, startPullingTask})
			{
				if (task == null) 
					continue;
				
				switch (task.Status)
				{
					case TaskStatus.RanToCompletion:
					case TaskStatus.Canceled:
						break;
					default:
						try
						{
							task.Wait();
						}
						catch (AggregateException ae)
						{
							if (ae.InnerException is OperationCanceledException == false)
							{
								throw;
							}
						}

						break;
				}
			}

			if (IsConnectionClosed)
				return new CompletedTask();

			return CloseSubscription();
		}

		private async Task CloseSubscription()
		{
			using (var closeRequest = CreateCloseRequest())
			{
				await closeRequest.ExecuteRequestAsync().ConfigureAwait(false);
				IsConnectionClosed = true;
			}
		}
	}
}