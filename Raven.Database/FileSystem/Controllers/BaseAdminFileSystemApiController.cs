﻿// -----------------------------------------------------------------------
//  <copyright file="BaseAdminFileSystemApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Web.Http.Controllers;

using Raven.Database.Common;
using Raven.Database.Server.Controllers.Admin;

namespace Raven.Database.FileSystem.Controllers
{
	public class BaseAdminFileSystemApiController : BaseAdminDatabaseApiController
	{
		public string FileSystemName { get; private set; }

		private RavenFileSystem _fileSystem;
		public RavenFileSystem FileSystem
		{
			get
			{
				if (_fileSystem != null)
					return _fileSystem;

				var resource = FileSystemsLandlord.GetResourceInternal(FileSystemName);
				if (resource == null)
				{
					throw new InvalidOperationException("Could not find a file system named: " + FileSystemName);
				}

				return _fileSystem = resource.Result;
			}
		}

		protected override void InnerInitialization(HttpControllerContext controllerContext)
		{
			base.InnerInitialization(controllerContext);

			FileSystemName = GetResourceName(controllerContext, ResourceType.FileSystem);
		}

		public override string TenantName
		{
			get { return BaseFileSystemApiController.TenantNamePrefix + FileSystemName; }
		}

		public override void MarkRequestDuration(long duration)
		{
			if (FileSystem == null)
				return;
			FileSystem.MetricsCounters.RequestDurationMetric.Update(duration);
			FileSystem.MetricsCounters.RequestDurationLastMinute.AddRecord(duration);
		}
	}
}