﻿// -----------------------------------------------------------------------
//  <copyright file="BadQuery.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class BadQuery : RavenTestBase
	{
		[Serializable]
		public class Entity
		{
			public string Id { get; set; }
			public string Number { get; set; }
		}
		[Fact]
		public void ShouldNotNull()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Entity { Number = "0373100117415000026" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var res = session.Query<Entity>().Customize(x => x.WaitForNonStaleResults()).Count();
					Assert.Equal(1, res);
				}
				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var item = session.Query<Entity>()
							.Customize(x => x.WaitForNonStaleResults())
							.Statistics(out stats)
							.FirstOrDefault(x => x.Number == "0373100117415000026");
					// fail
					Assert.NotNull(item);
				}
			}
		}
	}

}