import commandBase = require("commands/commandBase");
import database = require("models/database");
import zip = require('jszip/jszip');
import zipUtils = require('jszip/jszip-utils.min');
import appUrl = require('common/appUrl');
import d3 = require("d3/d3");
import getSingleAuthTokenCommand = require("commands/getSingleAuthTokenCommand");

class getInfoPackage extends commandBase {

    /**
	* @param ownerDb The database the collections will belong to.
	*/
    constructor(private db: database, private withStackTrace: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {
        var task = $.Deferred();

        var getTokenTask = new getSingleAuthTokenCommand(this.db).execute();
        getTokenTask
            .done((tokenObject: singleAuthToken) => {
                var token = tokenObject.Token;
                var url = appUrl.forResourceQuery(this.db)  + (this.db.isSystem ? '/admin/debug/info-package' : '/debug/info-package');
                url += '?singleUseAuthToken=' + token;
                if (this.withStackTrace && this.db.isSystem) {
                    url += "&stacktrace=true";
                }

                var now = d3.time.format("%Y-%m-%d_%H:%M:%S")(new Date());
                var filename = this.db.isSystem ? "Admin-Debug-Info-" + now + ".zip" : "Debug-Info-" + this.db.name + "-" + now + ".zip";

                zipUtils.getBinaryContent(url, function (err, data) {
                    if (err) {
                        task.reject(err);
                    } else {

                        task.resolve(data, filename);
                    }            
                });
            })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get token!", response.responseText, response.statusText);
                task.reject(response);
            });

        return task.promise();
    }
}

export = getInfoPackage;