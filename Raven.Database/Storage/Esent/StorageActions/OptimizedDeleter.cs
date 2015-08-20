﻿// -----------------------------------------------------------------------
//  <copyright file="OptimizedDeleter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Util;
using System.Linq;
using Raven.Database.Storage;
using Sparrow.Collections;

namespace Raven.Storage.Esent.StorageActions
{
	public class OptimizedDeleter
	{
        private static int bookmarkMost = SystemParameters.BookmarkMost;

		private class Key : IComparable
		{
			public readonly byte[] Buffer;
			public readonly int BufferLen;

			public Key(byte[] buffer, int bufferLen)
			{
				Buffer = buffer;
				BufferLen = bufferLen;
			}

			public override bool Equals(object obj)
			{
				var other = ((Key) obj);
				if (BufferLen != other.BufferLen)
					return false;
				for (int i = 0; i < BufferLen; i++)
				{
					if (Buffer[i] != other.Buffer[i])
						return false;
				}
				return true;
			}

			public override int GetHashCode()
			{
				var hash = BufferLen;
				for (int i = 0; i < BufferLen; i++)
				{
					hash = hash*397 ^ Buffer[i];
				}
				return hash;
			}

			public int CompareTo(object obj)
			{
				var other = (Key) obj;
				for (int i = 0; i < Math.Min(BufferLen, other.BufferLen); i++)
				{
					var val = Buffer[i] - other.Buffer[i];
					if (val != 0)
						return val;
				}
				return BufferLen - other.BufferLen;
			}
		}

		private readonly ConcurrentSet<Key> itemsToDelete = new ConcurrentSet<Key>(); 
		private readonly RemainingReductionPerLevel itemsToDeletePerViewAndLevel =
			new  RemainingReductionPerLevel();
		public RemainingReductionPerLevel ItemsToDeletePerViewAndLevel { get { return itemsToDeletePerViewAndLevel; } }

		public int IndexId { get; set; }

		public bool Add(JET_SESID session, JET_TABLEID table, int level)
		{
			byte[] buffer;
			int actualBookmarkSize;

            var largeBuffer = IndexReaderBuffers.Buffers.TakeBuffer(bookmarkMost);
			try
			{
				Api.JetGetBookmark(session, table, largeBuffer,
								   largeBuffer.Length, out actualBookmarkSize);

				buffer = new byte[actualBookmarkSize];
				Buffer.BlockCopy(largeBuffer, 0, buffer, 0, actualBookmarkSize);
			}
			finally
			{
				IndexReaderBuffers.Buffers.ReturnBuffer(largeBuffer);
			}
			var res = itemsToDelete.TryAdd(new Key(buffer, actualBookmarkSize));
			if (res)
			{
				itemsToDeletePerViewAndLevel.DecrementPerLevelCounters(level);
			}
			return res;
		}

		public IEnumerable<Tuple<byte[],int>> GetSortedBookmarks()
		{
			return itemsToDelete.OrderBy(x => x).Select(x => Tuple.Create(x.Buffer, x.BufferLen));
		}
	}
}