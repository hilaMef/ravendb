//-----------------------------------------------------------------------
// <copyright file="ITransactionalStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Impl.DTC;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.Storage
{
    public interface ITransactionalStorage : IDisposable
    {
        /// <summary>
        /// This is used mostly for replication
        /// </summary>
        Guid Id { get; }

        IDocumentCacher DocumentCacher { get; }

        IDisposable DisableBatchNesting();

        IStorageActionsAccessor CreateAccessor();
        bool SkipConsistencyCheck { get;}
        void Batch(Action<IStorageActionsAccessor> action);
        void ExecuteImmediatelyOrRegisterForSynchronization(Action action);
        void Initialize(IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs, Action<string> putResourceMarker = null);
        void StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory, bool incrementalBackup, DatabaseDocument documentDatabase);
        void Restore(DatabaseRestoreRequest restoreRequest, Action<string> output);
        DatabaseSizeInformation GetDatabaseSize();
        long GetDatabaseCacheSizeInBytes();
        long GetDatabaseTransactionVersionSizeInBytes();
        StorageStats GetStorageStats();

        string FriendlyName { get; }
        bool HandleException(Exception exception);

        bool IsAlreadyInBatch { get; }
        bool SupportsDtc { get; }

        void Compact(InMemoryRavenConfiguration configuration, Action<string> output);
        Guid ChangeId();
        void ClearCaches();
        void DumpAllStorageTables();
        InFlightTransactionalState InitializeInFlightTransactionalState(DocumentDatabase self, Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> put, Func<string, Etag, TransactionInformation, bool> delete);
        IList<string> ComputeDetailedStorageInformation(bool computeExactSizes = false);
        List<TransactionContextData> GetPreparedTransactions();

        object GetInFlightTransactionsInternalStateForDebugOnly();

        ConcurrentDictionary<int, RemainingReductionPerLevel> GetScheduledReductionsPerViewAndLevel();
        /// <summary>
        /// Scheduled reduction tracking is a memory living entity it will get corrupted on a reset.
        /// The reset must occur while there are no map/reduce indexing activity going on.
        /// </summary>
        void ResetScheduledReductionsTracking();

        void RegisterTransactionalStorageNotificationHandler(ITransactionalStorageNotificationHandler handler);
    }

    public interface ITransactionalStorageNotificationHandler
    {
        void HandleTransactionalStorageNotification();
    }
}
