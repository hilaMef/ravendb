import pagedResultSet = require("common/pagedResultSet");
import commandBase = require("commands/commandBase");
import database = require("models/database");

class getStudioConfig extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<singleAuthToken> {
        var url = "/studio-tasks/config";
        var getTask = this.query(url, null, this.db);
        return getTask;
    }
}

export = getStudioConfig;