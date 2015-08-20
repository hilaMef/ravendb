//-----------------------------------------------------------------------
// <copyright file="RobustEnumerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Database.Linq;
using Raven.Database.Util;

namespace Raven.Database.Indexing
{
	public class RobustEnumerator
	{        
        private readonly Action BeforeMoveNext = () => { };
        private readonly Action CancelMoveNext = () => { };
        private readonly Action<Exception, object> OnError = (_,__) => { };

		private readonly CancellationToken cancellationToken;
		private readonly int numberOfConsecutiveErrors;

        public Stopwatch MoveNextDuration = null;

        public RobustEnumerator(CancellationToken cancellationToken, int numberOfConsecutiveErrors, Action beforeMoveNext = null, Action cancelMoveNext = null, Action<Exception,object> onError = null )
        {
            this.cancellationToken = cancellationToken;
            this.numberOfConsecutiveErrors = numberOfConsecutiveErrors;

            if (beforeMoveNext != null)
                BeforeMoveNext = beforeMoveNext;
            if (cancelMoveNext != null)
                CancelMoveNext = cancelMoveNext;
            if (onError != null)
                OnError = onError;
        }

		public IEnumerable<object> RobustEnumeration(IEnumerator<object> input, IndexingFunc func)
		{
            using (var wrapped = new StatefulEnumerableWrapper<dynamic>(input))
		    {
                IEnumerator<dynamic> en;
                using (en = func(wrapped).GetEnumerator())
                {
	                int maxNumberOfConsecutiveErrors = numberOfConsecutiveErrors;
	                do
	                {
		                cancellationToken.ThrowIfCancellationRequested();
		                var moveSuccessful = MoveNext(en, wrapped);
		                if (moveSuccessful == false)
			                break;
		                if (moveSuccessful == true)
		                {
			                maxNumberOfConsecutiveErrors = numberOfConsecutiveErrors;

			                yield return en.Current;
		                }
		                else
		                {
			                // we explicitly do not dispose the enumerator, since that would not allow us 
			                // to continue on with the next item in the list.
			                // Not actually a problem, because we are iterating only over in memory data
			                // en.Dispose();

			                en = func(wrapped).GetEnumerator();
			                maxNumberOfConsecutiveErrors--;
		                }
	                } while (maxNumberOfConsecutiveErrors > 0);
                }
			}
		}

        public IEnumerable<object> RobustEnumeration(IEnumerator<object> input, List<IndexingFunc> funcs)
        {
            if (funcs.Count == 1)
            {
                foreach (var item in RobustEnumeration(input, funcs[0]))
                    yield return item;                    

                yield break;
            }

            var onlyIterateOverEnumableOnce = new List<object>();
            try
            {
                while (input.MoveNext())
                {
                    onlyIterateOverEnumableOnce.Add(input.Current);
                }
            }
            catch (Exception e)
            {
                OnError(e, null);
                yield break;
            }

            foreach (var func in funcs)
            {
                foreach (var item in RobustEnumeration(onlyIterateOverEnumableOnce.GetEnumerator(), func))
                {
                    yield return item;
                }
            }
        }

		private bool? MoveNext(IEnumerator en, StatefulEnumerableWrapper<object> innerEnumerator)
		{
			using (StopwatchScope.For(MoveNextDuration))
			{
				try
				{
					BeforeMoveNext();
					var moveNext = en.MoveNext();
					if (moveNext == false)
						CancelMoveNext();

					return moveNext;
				}
				catch (Exception e)
				{
					OnError(e, innerEnumerator.Current);
				}
			}

			return null;
		}
	}
}