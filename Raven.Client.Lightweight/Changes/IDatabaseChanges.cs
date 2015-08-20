using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Client.Changes
{
    public interface IDatabaseChanges : IConnectableChanges<IDatabaseChanges>
	{
		/// <summary>
		/// Subscribe to changes for specified index only.
		/// </summary>
		IObservableWithTask<IndexChangeNotification> ForIndex(string indexName);

		/// <summary>
		/// Subscribe to changes for specified document only.
		/// </summary>
		IObservableWithTask<DocumentChangeNotification> ForDocument(string docId);

		/// <summary>
		/// Subscribe to changes for all documents.
		/// </summary>
		IObservableWithTask<DocumentChangeNotification> ForAllDocuments();

		/// <summary>
		/// Subscribe to changes for all indexes.
		/// </summary>
		IObservableWithTask<IndexChangeNotification> ForAllIndexes();

		/// <summary>
		/// Subscribe to changes for all transformers.
		/// </summary>
	    IObservableWithTask<TransformerChangeNotification> ForAllTransformers();

		/// <summary>
		/// Subscribe to changes for all documents that Id starts with given prefix.
		/// </summary>
        IObservableWithTask<DocumentChangeNotification> ForDocumentsStartingWith(string docIdPrefix);

		/// <summary>
		/// Subscribe to changes for all documents that belong to specified collection (Raven-Entity-Name).
		/// </summary>
		IObservableWithTask<DocumentChangeNotification> ForDocumentsInCollection(string collectionName);

		/// <summary>
		/// Subscribe to changes for all documents that belong to specified collection (Raven-Entity-Name).
		/// </summary>
		IObservableWithTask<DocumentChangeNotification> ForDocumentsInCollection<TEntity>();

		/// <summary>
		/// Subscribe to changes for all documents that belong to specified type (Raven-Clr-Type).
		/// </summary>
		IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType(string typeName);

		/// <summary>
		/// Subscribe to changes for all documents that belong to specified type (Raven-Clr-Type).
		/// </summary>
		IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType(Type type);

		/// <summary>
		/// Subscribe to changes for all documents that belong to specified type (Raven-Clr-Type).
		/// </summary>
		IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType<TEntity>();

		/// <summary>
		/// Subscribe to all replication conflicts.
		/// </summary>
        IObservableWithTask<ReplicationConflictNotification> ForAllReplicationConflicts();

        /// <summary>
		/// Subscribe to all bulk insert operation changes that belong to a operation with given Id.
		/// </summary>
		IObservableWithTask<BulkInsertChangeNotification> ForBulkInsert(Guid? operationId = null);

		/// <summary>
		/// Subscribe to changes for all data subscriptions.
		/// </summary>
		IObservableWithTask<DataSubscriptionChangeNotification> ForAllDataSubscriptions();

		/// <summary>
		/// Subscribe to changes for a specified data subscription.
		/// </summary>
		IObservableWithTask<DataSubscriptionChangeNotification> ForDataSubscription(long id);

	}
}