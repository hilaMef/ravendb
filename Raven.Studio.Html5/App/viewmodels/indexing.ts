import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

import status = require("viewmodels/status");

class requests extends viewModelBase {
    router: DurandalRouter;
    currentRouteTitle: KnockoutComputed<string>;

    constructor() {
        super();

        this.router = status.statusRouter.createChildRouter()
            .map([
                { route: 'databases/status/indexing',             moduleId: 'viewmodels/indexPerformance', title: 'Indexing performance',  tooltip: "Shows details about indexing peformance", nav: true, hash: appUrl.forCurrentDatabase().indexPerformance },
                { route: 'databases/status/indexing/stats',       moduleId: 'viewmodels/indexStats',       title: 'Index stats',           tooltip: "Show details about indexing in/out counts", nav: true, hash: appUrl.forCurrentDatabase().indexStats },
                { route: 'databases/status/indexing/batchSize',   moduleId: 'viewmodels/indexBatchSize',   title: 'Index batch size',      tooltip: "Index batch sizes", nav: true, hash: appUrl.forCurrentDatabase().indexBatchSize },
                { route: 'databases/status/indexing/prefetches',  moduleId: 'viewmodels/indexPrefetches',  title: 'Prefetches',            tooltip: "Prefetches", nav: true, hash: appUrl.forCurrentDatabase().indexPrefetches },
            ])
            .buildNavigationModel();
       
        appUrl.mapUnknownRoutes(this.router);

        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = requests;    