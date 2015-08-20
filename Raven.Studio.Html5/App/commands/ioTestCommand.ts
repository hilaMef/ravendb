import commandBase = require("commands/commandBase");
import database = require("models/database");
import appUrl = require("common/appUrl");
import getOperationStatusCommand = require('commands/getOperationStatusCommand');

class ioTestCommand extends commandBase {

    operationIdTask = $.Deferred();

    constructor(private db: database, private testParameters: performanceTestRequestDto, private onStatus: (string) => void) {
        super();
    }

    execute(): JQueryPromise<any> {
        var promise = $.Deferred();
        var url = '/admin/ioTest';
        this.post(url, ko.toJSON(this.testParameters), null)
            .done((result: operationIdDto) => {
                this.operationIdTask.resolve(result.OperationId);
                this.monitorIoTest(promise, result.OperationId);
                })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to start disk IO test!", response.responseText, response.statusText);
                promise.reject();
            });
        return promise;
    }

    private monitorIoTest(parentPromise: JQueryDeferred<any>, operationId: number) {
        new getOperationStatusCommand(appUrl.getSystemDatabase(), operationId)
            .execute()
            .done((result: operationStatusDto) => {
                if (result.Completed) {
                    if (result.Faulted) {
                        this.reportError("Failed to perform disk IO test!", result.State.Error);
                        parentPromise.reject();
                    } else {
                        this.reportSuccess("Disk IO test completed");   
                        parentPromise.resolve();
                    }
                } else {
	                this.onStatus(result.State);
                    setTimeout(() => this.monitorIoTest(parentPromise, operationId), 500);
                }
            });
    }
}

export = ioTestCommand;