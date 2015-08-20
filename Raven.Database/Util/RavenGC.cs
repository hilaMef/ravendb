﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;

namespace Raven.Database.Util
{
	public static class RavenGC
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		private static long memoryBeforeLastForcedGC;
		private static long memoryAfterLastForcedGC;

		private static DateTime lastForcedGCTime;

		private static int delayBetweenGCInMinutes;
		private const int DefaultDelayBetweenGCInMinutes = 1;
		private const int MaxDelayBetweenGCInMinutes = 60;
		static RavenGC()
		{
			ResetHistory();
		}

		public static ReaderWriterLockSlim GcCollectLock = new ReaderWriterLockSlim();

		public static void ResetHistory()
		{
			lastForcedGCTime = DateTime.MinValue;
			memoryAfterLastForcedGC = 0;
			memoryBeforeLastForcedGC = 0;
			delayBetweenGCInMinutes = DefaultDelayBetweenGCInMinutes;
		}

		public static long MemoryBeforeLastForcedGC
		{
			get { return memoryBeforeLastForcedGC; }
		}
		public static long MemoryAfterLastForcedGC
		{
			get { return memoryAfterLastForcedGC; }
		}
		public static DateTime LastForcedGCTime
		{
			get { return lastForcedGCTime; }
		}

		private static long lastTimeMemoryReleasedBeforeGC = 0;
		private static readonly long fiveSecondsInTicks = Stopwatch.Frequency * 5;
		private static void ReleaseMemoryBeforeGC()
		{

			if (MemoryStatistics.AvailableMemoryInMb < ((double)MemoryStatistics.TotalPhysicalMemory - MemoryStatistics.AvailableMemoryInMb)/10)
			{
				if (Environment.TickCount - lastTimeMemoryReleasedBeforeGC < fiveSecondsInTicks)
					return;

				lastTimeMemoryReleasedBeforeGC = Environment.TickCount;

				MemoryStatistics.SimulateLowMemoryNotification();
			}
			else
			{
				MemoryStatistics.InitiateSoftMemoryRelease();
			}
		}

		public static bool CollectGarbage(int generation, GCCollectionMode collectionMode = GCCollectionMode.Default, bool forceByUser = false)
		{
			return CollectGarbageInternal(false, null, forceByUser, generation, collectionMode, false);
		}

		public static bool CollectGarbage(bool compactLoh, Action afterCollect, bool forceByUser = false)
		{
			return CollectGarbageInternal(compactLoh, afterCollect, forceByUser, GC.MaxGeneration, GCCollectionMode.Forced, true);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		private static bool CollectGarbageInternal(bool compactLoh, Action afterCollect, bool forceByUser, int maxGeneration, GCCollectionMode gcCollectionMode, bool waitForPendingFinalizers)
		{
			if (!ShouldCollectNow() && !forceByUser)
				return false;

			GcCollectLock.EnterWriteLock();
			try
			{
				ReleaseMemoryBeforeGC();
				memoryBeforeLastForcedGC = GC.GetTotalMemory(false);

				if (compactLoh)
					SetCompactLog.Value();

				GC.Collect(maxGeneration, gcCollectionMode);
				if (afterCollect != null)
					afterCollect();

				if (waitForPendingFinalizers)
					GC.WaitForPendingFinalizers();
			}
			finally
			{
				GcCollectLock.ExitWriteLock();
			}

			memoryAfterLastForcedGC = GC.GetTotalMemory(false);
			memoryDifferenceLastGc = DifferenceAsDecimalPercents(MemoryBeforeLastForcedGC, MemoryAfterLastForcedGC);

			log.Info("Finished GC, before was {0:#,#}kb, after is {1:#,#}kb", MemoryBeforeLastForcedGC, MemoryAfterLastForcedGC);

			// -> reset last time, increase delay threshold and disallow GC (too early!)
			lastForcedGCTime = SystemTime.UtcNow;

			var old = delayBetweenGCInMinutes;
			if (memoryDifferenceLastGc < 0.1)
			{
				delayBetweenGCInMinutes = Math.Max(5 + delayBetweenGCInMinutes, MaxDelayBetweenGCInMinutes);

				if (old != delayBetweenGCInMinutes)
				{
					log.Debug("Increasing delay for forced GC (not enough memory released, so we need to back off). " +
							  "New interval between GCs will be {0} minutes", delayBetweenGCInMinutes);
				}
			}
			else
			{
				if (old != delayBetweenGCInMinutes)
				{
					log.Debug("Resetting delay for forced GC (enough memory was released to make it useful, so we don't need to back off). " +
							  "New interval between GCs will be {0} minutes", delayBetweenGCInMinutes);
				}
				delayBetweenGCInMinutes = DefaultDelayBetweenGCInMinutes;
			}

			return true;
		}

		private static bool ShouldCollectNow()
		{
			var nowTime = SystemTime.UtcNow;
			if (MemoryAfterLastForcedGC == 0 || MemoryBeforeLastForcedGC == 0) //running for the first time
			{
				log.Debug("GCing for the first time...");
				return true;
			}

			//if last time was freed enough memory (more than 10%) allow the GC and store last GC time
			if (memoryDifferenceLastGc >= 0.1)
			{
				log.Debug("Allowing GC because difference of memory before and after GC equals or more than 10% - last time was released {0}kbs.", Math.Abs(MemoryAfterLastForcedGC - MemoryBeforeLastForcedGC) / 1024);
				delayBetweenGCInMinutes = DefaultDelayBetweenGCInMinutes;

				return true;
			}

			//if last time not enough memory was freed, but enough time passed since last allowed GC,
			//reset delay and allow GC
			if ((nowTime - LastForcedGCTime).TotalMinutes >= delayBetweenGCInMinutes)
			{
				log.Debug("Allowing GC because more than {1} minutes passed since last GC - last time was released {0}kbs.", Math.Abs(MemoryAfterLastForcedGC - MemoryBeforeLastForcedGC) / 1024, (nowTime - LastForcedGCTime).TotalMinutes);
				return true;
			}

			//not enough memory was freed the last time, and not enough time passed
			return false;
		}

		private static double DifferenceAsDecimalPercents(long v1, long v2)
		{
			double x1 = v1;
			double x2 = v2;

			if (x1 > 0.0 && x2 > 0.0)
				return Math.Abs(x1 - x2) / x2;

			return 0.0;
		}

		// this is just the code below, but we have to run on 4.5, not just 4.5.1
		// GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
		private static readonly Lazy<Action> SetCompactLog = new Lazy<Action>(() =>
		{
			var prop = typeof(GCSettings).GetProperty("LargeObjectHeapCompactionMode");
			if (prop == null)
				return (() => { });
			var enumType = Type.GetType("System.Runtime.GCLargeObjectHeapCompactionMode, mscorlib");
			var value = Enum.Parse(enumType, "CompactOnce");
			var lambda = Expression.Lambda<Action>(Expression.Assign(Expression.MakeMemberAccess(null, prop), Expression.Constant(value)));
			return lambda.Compile();
		});
		private static double memoryDifferenceLastGc;

	    public static void ConsiderRunningGC()
	    {
	        var availableMemoryInMb = MemoryStatistics.AvailableMemoryInMb;
	        if (availableMemoryInMb >= 1536 ||
                availableMemoryInMb > (MemoryStatistics.TotalPhysicalMemory*0.2))
	        {
                // there is no point in even running this if we have more than 1.5GB of memory or more than 20% of 
                // memory free, it is better to let the system run in, then inducing GC manually
	            return;
	        }
            CollectGarbage(1, GCCollectionMode.Optimized);
        }
	}
}
