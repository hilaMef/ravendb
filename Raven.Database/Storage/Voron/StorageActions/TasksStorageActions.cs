﻿// -----------------------------------------------------------------------
//  <copyright file="TasksStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Diagnostics;
using System.Threading.Tasks;
using Lucene.Net.Search;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Storage.Voron.StorageActions.StructureSchemas;
using Raven.Database.Util.Streams;

namespace Raven.Database.Storage.Voron.StorageActions
{
	using System;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Logging;
	using Raven.Database.Impl;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Database.Tasks;
	using Raven.Json.Linq;

	using global::Voron;
	using global::Voron.Impl;

	internal class TasksStorageActions : StorageActionsBase, ITasksStorageActions
	{
		private static readonly ILog Logger = LogManager.GetCurrentClassLogger();

		private readonly TableStorage tableStorage;

		private readonly IUuidGenerator generator;

		private readonly Reference<WriteBatch> writeBatch;

        public TasksStorageActions(TableStorage tableStorage, IUuidGenerator generator, Reference<SnapshotReader> snapshot, Reference<WriteBatch> writeBatch, IBufferPool bufferPool)
			: base(snapshot, bufferPool)
		{
			this.tableStorage = tableStorage;
			this.generator = generator;
			this.writeBatch = writeBatch;
		}

		public void AddTask(DatabaseTask task, DateTime addedAt)
		{
			var tasksByType = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByType);
			var tasksByIndex = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndex);
			var tasksByIndexAndType = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndexAndType);

			var type = task.GetType().FullName;
			var index = task.Index;
			var id = generator.CreateSequentialUuid(UuidType.Tasks);
            var idAsString = (Slice)id.ToString();

			var taskStructure = new Structure<TaskFields>(tableStorage.Tasks.Schema)
				.Set(TaskFields.IndexId, index)
				.Set(TaskFields.TaskId, id.ToByteArray())
				.Set(TaskFields.AddedAt, addedAt.ToBinary())
				.Set(TaskFields.Type, type)
				.Set(TaskFields.SerializedTask, task.AsBytes());

			tableStorage.Tasks.AddStruct(writeBatch.Value, idAsString, taskStructure, 0);

            var indexKey = CreateKey(index);

            tasksByType.MultiAdd(writeBatch.Value, (Slice)CreateKey(type), idAsString);
            tasksByIndex.MultiAdd(writeBatch.Value, (Slice)indexKey, idAsString);
            tasksByIndexAndType.MultiAdd(writeBatch.Value, (Slice)AppendToKey(indexKey, type), idAsString);
		}

		public bool HasTasks
		{
			get { return ApproximateTaskCount > 0; }
		}

		public long ApproximateTaskCount
		{
			get
			{
				return tableStorage.GetEntriesCount(tableStorage.Tasks);
			}
		}

		public T GetMergedTask<T>() where T : DatabaseTask
		{
			var type = CreateKey(typeof(T).FullName);
			var tasksByType = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByType);

            using (var iterator = tasksByType.MultiRead(Snapshot, (Slice)type))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return null;

				do
				{
					ushort version;
					var value = LoadStruct(tableStorage.Tasks, iterator.CurrentKey, writeBatch.Value, out version);
					if (value == null)
						continue;
					DatabaseTask task;
					try
					{
						task = DatabaseTask.ToTask(value.ReadString(TaskFields.Type), value.ReadBytes(TaskFields.SerializedTask));
					}
					catch (Exception e)
					{
						Logger.ErrorException(
							string.Format("Could not create instance of a task: {0}", value),
							e);
						continue;
					}

					MergeSimilarTasks(task, value.ReadBytes(TaskFields.TaskId));
					RemoveTask(iterator.CurrentKey, task.Index, type);

					return (T)task;
				}
				while (iterator.MoveNext());
			}

			return null;
		}

		private void MergeSimilarTasks(DatabaseTask task, byte[] taskId)
		{
			var id = Etag.Parse(taskId);
			var type = task.GetType().FullName;
			var tasksByIndexAndType = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndexAndType);

			using (var iterator = tasksByIndexAndType.MultiRead(Snapshot, (Slice)CreateKey(task.Index, type)))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				int totalTaskCount = 0;

				do
				{
					var currentId = Etag.Parse(iterator.CurrentKey.ToString());
					if (currentId == id)
						continue;

					ushort version;
					var value = LoadStruct(tableStorage.Tasks, iterator.CurrentKey, writeBatch.Value, out version);
					if (value == null)
						continue;
					DatabaseTask existingTask;
					try
					{
						existingTask = DatabaseTask.ToTask(value.ReadString(TaskFields.Type), value.ReadBytes(TaskFields.SerializedTask));
					}
					catch (Exception e)
					{
						Logger.ErrorException(
							string.Format("Could not create instance of a task: {0}", value),
							e);

						RemoveTask(iterator.CurrentKey, task.Index, type);
						continue;
					}

					task.Merge(existingTask);
					RemoveTask(iterator.CurrentKey, task.Index, type);

					if (totalTaskCount++ > 1024)
						break;
				}
				while (iterator.MoveNext());
			}
		}

		private void RemoveTask(Slice taskId, int index, string type)
		{
			var tasksByType = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByType);
			var tasksByIndex = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndex);
			var tasksByIndexAndType = tableStorage.Tasks.GetIndex(Tables.Tasks.Indices.ByIndexAndType);

			tableStorage.Tasks.Delete(writeBatch.Value, taskId);

            var indexKey = CreateKey(index);

            tasksByType.MultiDelete(writeBatch.Value, (Slice)CreateKey(type), taskId);
            tasksByIndex.MultiDelete(writeBatch.Value, (Slice)indexKey, taskId);
            tasksByIndexAndType.MultiDelete(writeBatch.Value, (Slice)AppendToKey(indexKey, type), taskId);
		}


		public System.Collections.Generic.IEnumerable<TaskMetadata> GetPendingTasksForDebug()
		{
			if(!HasTasks)
				yield break;

			using (var taskIterator = tableStorage.Tasks.Iterate(Snapshot, writeBatch.Value))
			{
				if(!taskIterator.Seek(Slice.BeforeAllKeys))
					yield break;

				do
				{
					ushort version;
					var taskData = LoadStruct(tableStorage.Tasks, taskIterator.CurrentKey, writeBatch.Value, out version); 
					if (taskData == null) 
							throw new InvalidOperationException("Retrieved a pending task object, but was unable to parse it. This is probably a data corruption or a bug.");

						TaskMetadata pendingTasksForDebug;
						try
						{
							pendingTasksForDebug = new TaskMetadata
							{
								Id = Etag.Parse(taskData.ReadBytes(TaskFields.TaskId)),
								AddedTime = DateTime.FromBinary(taskData.ReadLong(TaskFields.AddedAt)),
								Type = taskData.ReadString(TaskFields.Type),
								IndexId = taskData.ReadInt(TaskFields.IndexId)
							};
						}
						catch (Exception e)
						{
							throw new InvalidOperationException("The pending task record was parsed, but contained invalid values. See more details at inner exception.",e);
						}

						yield return pendingTasksForDebug;
				} while (taskIterator.MoveNext());
			}
			

		}
	}
}