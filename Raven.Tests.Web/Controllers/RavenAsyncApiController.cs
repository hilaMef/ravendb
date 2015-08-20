﻿// -----------------------------------------------------------------------
//  <copyright file="RavenSyncApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http.Controllers;

using Raven.Client;

namespace Raven.Tests.Web.Controllers
{
	public abstract class RavenAsyncApiController : RavenApiController
	{
		protected IAsyncDocumentSession Session { get; private set; }

		public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
		{
			using (Session = DocumentStore.OpenAsyncSession())
			{
				var response = await base.ExecuteAsync(controllerContext, cancellationToken);
				await Session.SaveChangesAsync();

				return response;
			}
		}
	}
}