/// <reference path="../../Scripts/typings/ace/ace.d.ts" />
import InMethodFilter = require("viewmodels/inMethodFilter");
import fieldStringFilter = require("viewmodels/fieldStringFilter");
import fieldRangeFilter = require("viewmodels/fieldRangeFilter");
import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getCollectionsCommand = require("commands/getCollectionsCommand");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import pagedList = require("common/pagedList");
import pagedResultSet = require("common/pagedResultSet");
import queryIndexCommand = require("commands/queryIndexCommand");
import database = require("models/database");
import querySort = require("models/querySort");
import collection = require("models/collection");
import getTransformersCommand = require("commands/getTransformersCommand");
import deleteDocumentsMatchingQueryConfirm = require("viewmodels/deleteDocumentsMatchingQueryConfirm");
import document = require("models/document");
import customColumnParams = require("models/customColumnParams");
import customColumns = require("models/customColumns");
import selectColumns = require("viewmodels/selectColumns");
import getCustomColumnsCommand = require("commands/getCustomColumnsCommand");
import getDocumentsByEntityNameCommand = require("commands/getDocumentsByEntityNameCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/getDocumentsMetadataByIDPrefixCommand");
import getIndexTermsCommand = require("commands/getIndexTermsCommand");
import queryStatsDialog = require("viewmodels/queryStatsDialog");
import customFunctions = require("models/customFunctions");
import getCustomFunctionsCommand = require("commands/getCustomFunctionsCommand");
import transformerType = require("models/transformer");
import transformerQueryType = require("models/transformerQuery");
import getIndexSuggestionsCommand = require("commands/getIndexSuggestionsCommand");
import recentQueriesStorage = require("common/recentQueriesStorage");
import getSingleAuthTokenCommand = require("commands/getSingleAuthTokenCommand");
import virtualTable = require("widgets/virtualTable/viewModel");
import queryUtil = require("common/queryUtil");

class query extends viewModelBase {
    isTestIndex = ko.observable<boolean>(false);
    isStaticIndexSelected: KnockoutComputed<boolean>;
    selectedIndex = ko.observable<string>();
    indexes = ko.observableArray<indexDataDto>();
    indexesExceptCurrent: KnockoutComputed<indexDataDto[]>;
    editIndexUrl: KnockoutComputed<string>;
    visualizerUrl: KnockoutComputed<string>;
    indexPerfStatsUrl: KnockoutComputed<string>;
    termsUrl: KnockoutComputed<string>;
    statsUrl: KnockoutComputed<string>;
    hasSelectedIndex: KnockoutComputed<boolean>;
    hasEditableIndex: KnockoutComputed<boolean>;
    queryText = ko.observable("");
    queryResults = ko.observable<pagedList>();
    selectedResultIndices = ko.observableArray<number>();
    queryStats = ko.observable<indexQueryResultsDto>();
    selectedIndexEditUrl: KnockoutComputed<string>;
    sortBys = ko.observableArray<querySort>();
    indexFields = ko.observableArray<string>();
    transformer = ko.observable<transformerType>();
    allTransformers = ko.observableArray<transformerDto>();
    isDefaultOperatorOr = ko.observable(true);
    showFields = ko.observable(false);
    indexEntries = ko.observable(false);
    recentQueries = ko.observableArray<storedQueryDto>();
    rawJsonUrl = ko.observable<string>();
    exportUrl: KnockoutComputed<string>;
    collections = ko.observableArray<collection>([]);
    collectionNames = ko.observableArray<string>();
    collectionNamesExceptCurrent: KnockoutComputed<string[]>;
    selectedIndexLabel: KnockoutComputed<string>;
    appUrls: computedAppUrls;
    isIndexMapReduce: KnockoutComputed<boolean>;
	isDynamicIndex: KnockoutComputed<boolean>;
    isLoading = ko.observable<boolean>(false);
    isCacheDisable = ko.observable<boolean>(false);
	enableDeleteButton: KnockoutComputed<boolean>;
    token = ko.observable<singleAuthToken>();
    warningText = ko.observable<string>();
    isWarning = ko.observable<boolean>(false);
    containsAsterixQuery: KnockoutComputed<boolean>;
    searchField = ko.observable<string>("Select field");
    contextName = ko.observable<string>();
    didDynamicChangeIndex: KnockoutComputed<boolean>;
    dynamicPrefix = "dynamic/";

    currentColumnsParams = ko.observable<customColumns>(customColumns.empty());
    currentCustomFunctions = ko.observable<customFunctions>(customFunctions.empty());

    indexSuggestions = ko.observableArray<indexSuggestion>([]);
    showSuggestions: KnockoutComputed<boolean>;

    csvUrl = ko.observable<string>();

    static containerSelector = "#queryContainer";
    static queryGridSelector = "#queryResultsGrid";

    constructor() {
        super();
        this.appUrls = appUrl.forCurrentDatabase();
        this.editIndexUrl = ko.computed(() => this.selectedIndex() ? appUrl.forEditIndex(this.selectedIndex(), this.activeDatabase()) : null);
        this.visualizerUrl = ko.computed(() => this.selectedIndex() ? appUrl.forVisualizer(this.activeDatabase(), this.selectedIndex()) : null);
        this.indexPerfStatsUrl = ko.computed(() => this.selectedIndex() ? appUrl.forIndexPerformance(this.activeDatabase()) : null);
        this.termsUrl = ko.computed(() => this.selectedIndex() ? appUrl.forTerms(this.selectedIndex(), this.activeDatabase()) : null);
        this.statsUrl = ko.computed(() => appUrl.forStatus(this.activeDatabase()));
        this.hasSelectedIndex = ko.computed(() => this.selectedIndex() != null);
        this.hasEditableIndex = ko.computed(() => this.selectedIndex() != null && this.selectedIndex().indexOf("dynamic/") !== 0);
        this.rawJsonUrl.subscribe((value: string) => ko.postbox.publish("SetRawJSONUrl", value));
        this.selectedIndexLabel = ko.computed(() => this.selectedIndex() === "dynamic" ? "All Documents" : this.selectedIndex());
        this.containsAsterixQuery = ko.computed(() => this.queryText().contains("*.*"));
        this.isStaticIndexSelected = ko.computed(() => this.selectedIndex() == null || this.selectedIndex().indexOf(this.dynamicPrefix) == -1);
        this.selectedIndexEditUrl = ko.computed(() => {
            if (this.queryStats()) {
                var index = this.queryStats().IndexName;
                if (!!index && index.indexOf(this.dynamicPrefix) !== 0) {
                    return appUrl.forEditIndex(index, this.activeDatabase());
                }
            }

            return "";
        });

        this.indexesExceptCurrent = ko.computed(() => {
            var allIndexes = this.indexes();
            var selectedIndex = this.selectedIndex();

	        var allIndexesCopy: Array<indexDataDto>;

            if (!!selectedIndex && selectedIndex.indexOf(this.dynamicPrefix) === -1) {
	            allIndexesCopy = allIndexes.filter((indexDto: indexDataDto) => indexDto.name !== selectedIndex);
            } else {
	            allIndexesCopy = allIndexes.slice(0); // make copy as sort works in situ
            }
			allIndexesCopy.sort((l: indexDataDto, r: indexDataDto) => l.name.toLowerCase() > r.name.toLowerCase() ? 1 : -1);
            return allIndexesCopy;
        });

        this.collectionNamesExceptCurrent = ko.computed(() => {
            var allCollectionNames = this.collectionNames();
            var selectedIndex = this.selectedIndex();

            if (!!selectedIndex && selectedIndex.indexOf(this.dynamicPrefix) == 0) {
                return allCollectionNames.filter((collectionName: string) => this.dynamicPrefix + collectionName != selectedIndex);
            }
            return allCollectionNames;

        });

        this.showSuggestions = ko.computed<boolean>(() => {
            return this.indexSuggestions().length > 0;
        });

        this.didDynamicChangeIndex = ko.computed(() => {
            if (this.queryStats()) {
                var recievedIndex = this.queryStats().IndexName;
                var selectedIndex = this.selectedIndex();
                return selectedIndex.indexOf(this.dynamicPrefix) === 0 && this.indexes()[0].name !== recievedIndex;
            } else {
                return false;
            }
        });

        this.isIndexMapReduce = ko.computed(() => {
            var currentIndex = this.indexes.first(i=> i.name == this.selectedIndex());
            return !!currentIndex && currentIndex.hasReduce;
        });

	    this.isDynamicIndex = ko.computed(() => {
			var currentIndex = this.indexes.first(i=> i.name == this.selectedIndex());
		    if (currentIndex) {
			    console.log(currentIndex.name);
		    }
		    return !!currentIndex && currentIndex.name.startsWith("Auto/");
		});

		this.enableDeleteButton = ko.computed(() => {
			var currentIndex = this.indexes.first(i=> i.name == this.selectedIndex());
			var isMapReduce = this.isIndexMapReduce();
			var isDynamic = this.isDynamicIndex();
			return !!currentIndex && !isMapReduce && !isDynamic;
	    });

        aceEditorBindingHandler.install();

        // Refetch the index fields whenever the selected index name changes.
        this.selectedIndex
            .where(indexName => indexName != null)
            .subscribe(indexName => this.fetchIndexFields(indexName));
    }

    openQueryStats() {
        var viewModel = new queryStatsDialog(this.queryStats(), this.selectedIndexEditUrl(), this.didDynamicChangeIndex(), this.rawJsonUrl());
        app.showDialog(viewModel);
    }

    canActivate(args: any): any {
        super.canActivate(args);
        var deferred = $.Deferred();

        var db = this.activeDatabase();
        if (!!db) {
            this.fetchRecentQueries();
            $.when(this.fetchCustomFunctions(db), this.fetchAllTransformers(db))
                .done(() => deferred.resolve({ can: true }));
        } else {
            deferred.resolve({ redirect: "#resources" });
        }

        return deferred;
    }

    activate(indexNameOrRecentQueryHash?: string) {
        super.activate(indexNameOrRecentQueryHash);

        this.updateHelpLink('KCIMJK');
        this.updateAuthToken();
        this.selectedIndex.subscribe(index => this.onIndexChanged(index));
        var db = this.activeDatabase();
        return $.when(this.fetchAllCollections(db), this.fetchAllIndexes(db))
            .done(() => this.selectInitialQuery(indexNameOrRecentQueryHash));
    }

    detached() {
        super.detached();
        aceEditorBindingHandler.detached();
    }

    updateAuthToken() {
        new getSingleAuthTokenCommand(this.activeDatabase())
            .execute()
            .done(token => this.token(token));
    }

    attached() {
	    super.attached();
        this.createKeyboardShortcut("F2", () => this.editSelectedIndex(), query.containerSelector);
        this.createKeyboardShortcut("ctrl+enter", () => this.runQuery(), query.containerSelector);
        this.createKeyboardShortcut("alt+c", () => this.focusOnQuery(), query.containerSelector);
        this.createKeyboardShortcut("alt+r", () => this.runQuery(), query.containerSelector); // Using keyboard shortcut here, rather than HTML's accesskey, so that we don't steal focus from the editor.

        $("#indexQueryLabel").popover({
            html: true,
            trigger: "hover",
            container: ".form-horizontal",
            content: 'Queries use Lucene syntax. Examples:<pre><span class="code-keyword">Name</span>: Hi?berna*<br/><span class="code-keyword">Count</span>: [0 TO 10]<br/><span class="code-keyword">Title</span>: "RavenDb Queries 1010" AND <span class="code-keyword">Price</span>: [10.99 TO *]</pre>',
        });
        ko.postbox.publish("SetRawJSONUrl", appUrl.forIndexQueryRawData(this.activeDatabase(), this.selectedIndex()));

        this.focusOnQuery();

        var self = this;
        $(window).bind('storage', () => {
            self.fetchRecentQueries();
        });

        this.isLoading.extend({ rateLimit: 100 });
    }

    

    private fetchRecentQueries() {
        this.recentQueries(recentQueriesStorage.getRecentQueries(this.activeDatabase()));
    }

    private fetchCustomFunctions(db: database): JQueryPromise<any> {
        var deferred = $.Deferred();

        var task = new getCustomFunctionsCommand(db).execute();
        task.done((cf: customFunctions) => this.currentCustomFunctions(cf))
            .always(() => deferred.resolve());

        return deferred;
    }
    
    private fetchAllTransformers(db: database): JQueryPromise<any> {
        var deferred = $.Deferred();

        new getTransformersCommand(db)
            .execute()
            .done((results: transformerDto[]) => {
                this.allTransformers(results);
                deferred.resolve();
            });

        return deferred;
    }

    private fetchAllCollections(db: database): JQueryPromise<any> {
        var deferred = $.Deferred();

        new getCollectionsCommand(db)
            .execute()
            .done((results: collection[]) => {
                this.collections(results);
                this.collectionNames(results.map(c => c.name));
                deferred.resolve();
            });

        return deferred;
    }

    private fetchAllIndexes(db: database): JQueryPromise<any> {
        var deferred = $.Deferred();

        new getDatabaseStatsCommand(db)
            .execute()
            .done((results: databaseStatisticsDto) => {
                this.indexes(results.Indexes.map(i => {
                    return {
                        name: i.Name,
                        hasReduce: !!i.LastReducedTimestamp
                    };
                }));
                deferred.resolve();
            });

        return deferred;
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            ko.postbox.subscribe("EditItem", (itemNumber: number) => {
                //(itemNumber: number, res: resource, index: string, query?: string, sort?:string)
                var queriess = this.recentQueries();
                var recentq = this.recentQueries()[0];
                var sorts = recentq.Sorts
                    .join(',');
                //alert(appUrl.forEditQueryItem(itemNumber, this.activeDatabase(), recentq.IndexName,recentq.QueryText,sorts));
                router.navigate(appUrl.forEditQueryItem(itemNumber, this.activeDatabase(), recentq.IndexName, recentq.QueryText, sorts), true);
            })
        ];
    }

    onIndexChanged(newIndexName: string) {
        var command = getCustomColumnsCommand.forIndex(newIndexName, this.activeDatabase());
        this.contextName(command.docName);

        command.execute().done((dto: customColumnsDto) => {
            if (dto) {
                this.currentColumnsParams().columns($.map(dto.Columns, c => new customColumnParams(c)));
                this.currentColumnsParams().customMode(true);
            } else {
                // use default values!
                this.currentColumnsParams().columns.removeAll();
                this.currentColumnsParams().customMode(false);
            }

        });
    }

    selectInitialQuery(indexNameOrRecentQueryHash: string) {
        if (!indexNameOrRecentQueryHash && this.indexes().length > 0) {
            var firstIndexName = this.indexes.first().name;
            this.setSelectedIndex(firstIndexName);
        } else if (this.indexes.first(i => i.name == indexNameOrRecentQueryHash) || indexNameOrRecentQueryHash.indexOf(this.dynamicPrefix) === 0 || indexNameOrRecentQueryHash === "dynamic") {
            this.setSelectedIndex(indexNameOrRecentQueryHash);
        } else if (indexNameOrRecentQueryHash.indexOf("recentquery-") === 0) {
            var hash = parseInt(indexNameOrRecentQueryHash.substr("recentquery-".length), 10);
            var matchingQuery = this.recentQueries.first(q => q.Hash === hash);
            if (matchingQuery) {
	            this.runRecentQuery(matchingQuery);
            } else {
	            this.navigate(appUrl.forQuery(this.activeDatabase()));
            }
        }
    }

    focusOnQuery() {
        var editorElement = $("#queryEditor").length == 1 ? $("#queryEditor")[0] : null;
        if (editorElement) {
            var docEditor = ko.utils.domData.get(editorElement, "aceEditor");
            if (docEditor) {
                docEditor.focus();
            }
        }
    }

    editSelectedIndex() {
        this.navigate(this.editIndexUrl());
    }

    runRecentQuery(query: storedQueryDto) {
        this.selectedIndex(query.IndexName);
        this.queryText(query.QueryText);
        this.showFields(query.ShowFields);
        this.indexEntries(query.IndexEntries);
        this.isDefaultOperatorOr(query.UseAndOperator === false);
        this.sortBys(query.Sorts.map(s => querySort.fromQuerySortString(s)));
        this.selectTransformer(this.findTransformerByName(this.getStoredQueryTransformerName(query)));
        this.applyTransformerParameters(query);
        this.runQuery();
    }

    toggleCacheEnable() {
        this.isCacheDisable(!this.isCacheDisable());
    }

    runQuery(): pagedList {        
        var selectedIndex = this.selectedIndex();
        if (selectedIndex) {
            this.isWarning(false);
            this.isLoading(true);
            this.focusOnQuery();
            var queryText = this.queryText();
            var sorts = this.sortBys().filter(s => s.fieldName() != null);
            var database = this.activeDatabase();
            var showFields = this.showFields();
            var indexEntries = this.indexEntries();

            var transformer: transformerQueryType = null;
            if (this.transformer()) {
                transformer = new transformerQueryType({
                    transformerName: this.transformer().name(),
                    queryParams: []
                });

                var canRunQuery = true;
                $("#transformerParams .transformer_param_flag").each((index: number, element: any) => {
                    $(element).parent().removeClass("has-error");
                    if (element.value) {
                        transformer.addParamByNameAndValue(element.name, element.value);
                    } else if (element.required) {
                        canRunQuery = false;
                        $(element).parent().addClass("has-error");
                    }
                });

                if (!canRunQuery) {
                    return; // Cannot run query without required parameters
                }
            }

            this.currentColumnsParams().enabled(this.showFields() === false && this.indexEntries() === false);

            var useAndOperator = this.isDefaultOperatorOr() === false;

            var queryCommand = new queryIndexCommand(selectedIndex, database, 0, 25, queryText, sorts, transformer, showFields, indexEntries, useAndOperator, this.isCacheDisable());

            this.rawJsonUrl(appUrl.forResourceQuery(database) + queryCommand.getUrl());
            this.csvUrl(queryCommand.getCsvUrl());
            this.exportUrl = ko.computed(() => (appUrl.forResourceQuery(database) + this.csvUrl() + (this.token() ? "&singleUseAuthToken=" + this.token().Token : "")));

            var resultsFetcher = (skip: number, take: number) => {
                var command = new queryIndexCommand(selectedIndex, database, skip, take, queryText, sorts, transformer, showFields, indexEntries, useAndOperator, this.isCacheDisable());
                return command.execute()
                    .always(() => {
                        this.isLoading(false);
                        this.focusOnQuery();
                    })
                    .done((queryResults: pagedResultSet) => {
                        this.queryStats(queryResults.additionalResultInfo);
                        this.indexSuggestions([]);
                        if (queryResults.totalResultCount == 0) {
                            var queryFields = this.extractQueryFields();
                            var alreadyLookedForNull = false;
                            if (this.selectedIndex().indexOf(this.dynamicPrefix) !== 0) {
                                alreadyLookedForNull = true;
                                for (var i = 0; i < queryFields.length; i++) {
                                    this.getIndexSuggestions(selectedIndex, queryFields[i]);  
                                    if (queryFields[i].FieldValue == 'null') {
                                        this.isWarning(true);
                                        this.warningText(<any>("The Query contains '" + queryFields[i].FieldName + ": null', this will check if the field contains the string 'null', is this what you meant?"));
                                    }                                  
                                }
                            }
                            if (!alreadyLookedForNull) {
                                for (var i = 0; i < queryFields.length; i++) {;
                                    if (queryFields[i].FieldValue == 'null') {
                                        this.isWarning(true);
                                        this.warningText(<any>("The Query contains '" + queryFields[i].FieldName + ": null', this will check if the field contains the string 'null', is this what you meant?"));
                                    }
                                }
                            }

                        }
                    })
                    .fail(() => {
                        recentQueriesStorage.removeIndexFromRecentQueries(database, selectedIndex);
                    });
            };
            var resultsList = new pagedList(resultsFetcher);
            this.queryResults(resultsList);
            this.recordQueryRun(selectedIndex, queryText, sorts.map(s => s.toQuerySortString()), transformer, showFields, indexEntries, useAndOperator);

            return resultsList;
        }

        return null;
    }
    queryCompleter(editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) {

        queryUtil.queryCompleter(this.indexFields, this.selectedIndex, this.dynamicPrefix, this.activeDatabase, editor, session, pos, prefix, callback);

    }


    recordQueryRun(indexName: string, queryText: string, sorts: string[], transformerQuery: transformerQueryType, showFields: boolean, indexEntries: boolean, useAndOperator: boolean) {
        var newQuery: storedQueryDto = {
            IndexEntries: indexEntries,
            IndexName: indexName,
            IsPinned: false,
            QueryText: queryText,
            ShowFields: showFields,
            Sorts: sorts,
            TransformerQuery: transformerQuery,
            UseAndOperator: useAndOperator,
            Hash: (indexName + (queryText || "") +
                sorts.reduce((a, b) => a + b, "") +
                (transformerQuery ? transformerQuery.toUrl() : "") +
                showFields +
                indexEntries +
                useAndOperator).hashCode()
        };

        // Put the query into the URL, so that if the user refreshes the page, he's still got this query loaded.
        var queryUrl = appUrl.forQuery(this.activeDatabase(), newQuery.Hash);
        this.updateUrl(queryUrl);

        // Add this query to our recent queries list in the UI, or move it to the top of the list if it's already there.
        var existing = this.recentQueries.first(q => q.Hash === newQuery.Hash);
        if (existing) {
            this.recentQueries.remove(existing);
            this.recentQueries.unshift(existing);
        } else {
            this.recentQueries.unshift(newQuery);
        }

        // Limit us to 15 query recent runs.
        if (this.recentQueries().length > 15) {
            this.recentQueries.remove(this.recentQueries()[15]);
        }

        //save the recent queries to local storage
        recentQueriesStorage.saveRecentQueries(this.activeDatabase(), this.recentQueries());
    }

    getRecentQuerySortText(sorts: string[]) {
        if (sorts.length > 0) {
            return sorts
                .map(s => querySort.fromQuerySortString(s))
                .map(s => s.toHumanizedString())
                .reduce((first, second) => first + ", " + second);
        }

        return "";
    }

    setSelectedIndex(indexName: string) {
        this.sortBys.removeAll();
        this.selectedIndex(indexName);
        this.runQuery();

        // Reflect the new index in the address bar.
        var indexQuery = query.getIndexUrlPartFromIndexName(indexName);
        var url = appUrl.forQuery(this.activeDatabase(), indexQuery);
        this.updateUrl(url);
        NProgress.done();
    }

    static getIndexUrlPartFromIndexName(indexNameOrCollectionName: string) {
        if (indexNameOrCollectionName === "All Documents") {
            return "dynamic";
        }

        return indexNameOrCollectionName;
    }

    addSortBy() {
        var sort = new querySort();
        sort.fieldName.subscribe(() => this.runQuery());
        sort.sortDirection.subscribe(() => this.runQuery());
        this.sortBys.push(sort);
    }

    removeSortBy(sortBy: querySort) {
        this.sortBys.remove(sortBy);
        this.runQuery();
    }

    addTransformer() {
        this.transformer(new transformerType());
    }

    selectTransformer(dto: transformerDto) {
        if (!!dto) {
            var t = new transformerType();
            t.initFromLoad(dto);
            this.transformer(t);
            this.runQuery();
        } else {
            this.transformer(null);
        }
    }

    removeTransformer() {
        this.transformer(null);
        this.runQuery();
    }

    setOperatorOr() {
        this.isDefaultOperatorOr(true);
        this.runQuery();
    }

    setOperatorAnd() {
        this.isDefaultOperatorOr(false);
        this.runQuery();
    }

    toggleShowFields() {
        this.showFields(!this.showFields());
        this.runQuery();
    }

    toggleIndexEntries() {
        this.indexEntries(!this.indexEntries());
        this.runQuery();
    }

    deleteDocsMatchingQuery() {
        // Run the query so that we have an idea of what we'll be deleting.
        var queryResult = this.runQuery();
        queryResult
            .fetch(0, 1)
            .done((results: pagedResultSet) => {
                if (results.totalResultCount === 0) {
                    app.showMessage("There are no documents matching your query.", "Nothing to do");
                } else {
                    this.promptDeleteDocsMatchingQuery(results.totalResultCount);
                }
            });
    }

    promptDeleteDocsMatchingQuery(resultCount: number) {
        var viewModel = new deleteDocumentsMatchingQueryConfirm(this.selectedIndex(), this.queryText(), resultCount, this.activeDatabase());
        app
            .showDialog(viewModel)
            .done(() => this.runQuery());
    }

    private getQueryGrid(): virtualTable {
        var gridContents = $(query.queryGridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }



    selectColumns() {
           var selectColumnsViewModel: selectColumns = new selectColumns(
            this.currentColumnsParams().clone(),
            this.currentCustomFunctions().clone(),
            this.contextName(),
            this.activeDatabase(),
            this.getQueryGrid().getColumnsNames());

        app.showDialog(selectColumnsViewModel);
        selectColumnsViewModel.onExit().done((cols: customColumns) => {
            this.currentColumnsParams(cols);

            this.runQuery();

        });
    }

    fetchIndexFields(indexName: string) {
        // Fetch the index definition so that we get an updated list of fields to be used as sort by options.
        // Fields don't show for All Documents.
        var self = this;
        var isAllDocumentsDynamicQuery = indexName === "All Documents";
        if (!isAllDocumentsDynamicQuery) {
            //if index is dynamic, get columns using index definition, else get it using first index result
            if (indexName.indexOf(this.dynamicPrefix) === 0) {
                var collectionName = indexName.substring(8);
                new getDocumentsByEntityNameCommand(new collection(collectionName, this.activeDatabase()), 0, 1)
                    .execute()
                    .done((result: pagedResultSet) => {
                        if (!!result && result.totalResultCount > 0 && result.items.length > 0) {
                            var dynamicIndexPattern: document = new document(result.items[0]);
                            if (!!dynamicIndexPattern) {
                                this.indexFields(dynamicIndexPattern.getDocumentPropertyNames());
                            }
                        }
                    });
            } else {
                new getIndexDefinitionCommand(indexName, this.activeDatabase())
                    .execute()
                    .done((result: indexDefinitionContainerDto) => {
                    self.isTestIndex(result.Index.IsTestIndex);
                    self.indexFields(result.Index.Fields);
                    });
            }
        }
    }

    findTransformerByName(transformerName: string): transformerDto {
        try {
            return this.allTransformers().filter((dto: transformerDto) => transformerName === dto.name)[0];
        } catch (e) {
            return null;
        }
    }

    private getStoredQueryTransformerName(query: storedQueryDto): string {
        if (query.TransformerQuery) {
            return query.TransformerQuery.transformerName;
        }
        return "";
    }

    getStoredQueryTransformerParameters(queryParams: Array<transformerParamDto>): string {
        if (queryParams.length > 0) {
            return "(" +
                queryParams
                    .map((param: transformerParamDto) => param.name + "=" + param.value)
                    .join(", ") + ")";
        }

        return "";
    }

    applyTransformerParameters(query: storedQueryDto) {
        if (query.TransformerQuery && query.TransformerQuery.queryParams) {
            query.TransformerQuery.queryParams.forEach((param: transformerParamDto) => {
                $("#transformerParams input[name=" + param.name + "]").val(param.value);
            });
        }
    }

    extractQueryFields(): Array<queryFieldInfo> {
        var query = this.queryText();
        var luceneSimpleFieldRegex = /(\w+):\s*("((?:[^"\\]|\\.)*)"|'((?:[^'\\]|\\.)*)'|(\w+))/g;

        var queryFields: Array<queryFieldInfo> = [];
        var match: RegExpExecArray = null;
        while ((match = luceneSimpleFieldRegex.exec(query))) {
            var value = match[3] || match[4] || match[5];
            queryFields.push({
                FieldName: match[1],
                FieldValue: value,
                Index: match.index
            });
        }
        return queryFields;
    }

    getIndexSuggestions(indexName: string, info: queryFieldInfo) {
        if (this.indexFields().contains(info.FieldName)) {
            var task = new getIndexSuggestionsCommand(this.activeDatabase(), indexName, info.FieldName, info.FieldValue).execute();
            task.done((result: suggestionsDto) => {
                for (var index = 0; index < result.Suggestions.length; index++) {
                    this.indexSuggestions.push({
                        Index: info.Index,
                        FieldName: info.FieldName,
                        FieldValue: info.FieldValue,
                        Suggestion: result.Suggestions[index]
                    });
                }
            });
        }
    }

    applySuggestion(suggestion: indexSuggestion) {
        var value = this.queryText();
        var startIndex = value.indexOf(suggestion.FieldValue, suggestion.Index);
        this.queryText(value.substring(0, startIndex) + suggestion.Suggestion + value.substring(startIndex + suggestion.FieldValue.length));
        this.indexSuggestions([]);
        this.runQuery();
    }

    exportCsv() {
        // schedule token update (to properly handle subseqent downloads)
        setTimeout(() => this.updateAuthToken(), 50);
        return true;
    }

    fieldNameStartsWith() {
        var fieldStartsWithViewModel: fieldStringFilter = new fieldStringFilter("Field sub text");
        fieldStartsWithViewModel
            .applyFilterTask
            .done((input: string, option: string) => {
            if (this.queryText().length !== 0) {
                this.queryText(this.queryText() + " AND ");
            }
            if (option === "Starts with") {
                this.queryText(this.queryText() + this.searchField() + ":" + this.escapeQueryString(input) + "*");
            }
            else if (option === "Ends with") {
                this.queryText(this.queryText() + this.searchField() + ":" + "*" + this.escapeQueryString(input) );
            }
            else if (option === "Contains") {
                this.queryText(this.queryText() + this.searchField() + ":" + "*" + this.escapeQueryString(input) + "*");
            }
            else if (option === "Exact") {
                this.queryText(this.queryText() + this.searchField() + ":" + this.escapeQueryString(input));
            }
        });
        app.showDialog(fieldStartsWithViewModel);
    }

    fieldValueRange() {
        var fieldRangeFilterViewModel: fieldRangeFilter = new fieldRangeFilter("Field range filter");
        fieldRangeFilterViewModel
            .applyFilterTask
            .done((fromStr: string, toStr: string, option: string) => {
            var from = !fromStr || fromStr.length === 0 ? "*" : fromStr;
            var to = !toStr || toStr.length === 0 ? "*" : toStr;
            var fromPrefix = "";
            var toPrefix = "";
            if (this.queryText().length !== 0) {
                this.queryText(this.queryText() + " AND ");
            }
            if (option === "Numeric Double") {
                if (from !== "*") fromPrefix = "Dx";
                if (to !== "*") toPrefix = "Dx";
                this.queryText(this.queryText() + this.searchField() + "_Range:[" + fromPrefix + from + " TO " + toPrefix + to + "]");
            }
            else if (option === "Numeric Int") {
                if (from !== "*") fromPrefix = "Ix";
                if (to !== "*") toPrefix = "Ix";
                this.queryText(this.queryText() + this.searchField() + "_Range:[" + fromPrefix + from + " TO " + toPrefix+ to + "]");
            }
            else if (option === "Alphabetical") {
                this.queryText(this.queryText() + this.searchField() + ":[" + from + " TO " + to + "]");
            }
            else if (option === "Datetime") {
                this.queryText(this.queryText() + this.searchField() + ":[" + from + " TO " + to + "]");
            }

        });
        app.showDialog(fieldRangeFilterViewModel);  
    }

    fieldValueInMethod() {
        var inMethodFilterViewModel: InMethodFilter = new InMethodFilter("Field in method filter");
        inMethodFilterViewModel
            .applyFilterTask
            .done((inputs: string[]) => { 
                if (this.queryText().length !== 0) {
                    this.queryText(this.queryText() + " AND ");
                }
                var escapedStrings = inputs.map((s: string) => this.escapeQueryString(s)).join();
                this.queryText(this.queryText() + "@in<" + this.searchField() + ">:(" + escapedStrings + ")");
            });
        app.showDialog(inMethodFilterViewModel);        
    }

    private escapeQueryString(query: string): string {
        return query.replace(/([ \-\_\.])/g, '\\$1');
    }
}

export = query;
