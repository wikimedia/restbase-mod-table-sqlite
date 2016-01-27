"use strict";

var dbu = require('./dbutils');
var P = require('bluebird');
var TimeUuid = require("cassandra-uuid").TimeUuid;
var SchemaMigrator = require('./SchemaMigrator');
var Wrapper = require('./clientWrapper');
var LRU = require('lru-cache');
var validator = require('restbase-mod-table-spec').validator;
var stringify = require('json-stable-stringify');

function DB(options) {
    this.conf = options.conf;
    this.log = options.log;
    // SQLite client
    this.client = new Wrapper(options);
    this.schemaCache = {};
    this.schemaCache[this.schemaTableName] = this.infoSchemaInfo;
    this.queryCache = new LRU({
        max: 500,
        dispose: function(key, statement) {
            statement.finalize();
        }
    });
}

// Info table schema
DB.prototype.infoSchema = validator.validateAndNormalizeSchema({
    table: 'meta',
    attributes: {
        table: 'string',
        value: 'json',
        tid: 'timeuuid'
    },
    index: [
        { attribute: 'table', type: 'hash' },
        { attribute: 'tid', type: 'range', order: 'desc' }
    ]
});

DB.prototype.infoSchemaInfo = dbu.makeSchemaInfo(DB.prototype.infoSchema, true);
DB.prototype.schemaTableName = 'global_schema';

DB.prototype.getTableSchema = function(domain, bucket) {
    var tableName = domain + '_' + bucket;
    return this._get(this.schemaTableName, {
        attributes: {
            table: tableName
        }
    }, this.infoSchemaInfo)
    .then(function(res) {
        if (res && res.items.length) {
            return {
                status: 200,
                tid: res.items[0].tid,
                schema: JSON.parse(res.items[0].value)
            };
        } else {
            throw new dbu.HTTPError({
                status: 404,
                body: {
                    type: 'notfound',
                    title: 'the requested table schema was not found'
                }
            });
        }
    });
};

DB.prototype._getSchema = function(tableName) {
    var self = this;
    return P.try(function() {
        return self._get(self.schemaTableName, {
            attributes: {
                table: tableName
            }
        }, self.infoSchemaInfo);
    })
    .then(function(res) {
        if (res && res.items.length) {
            var schema = JSON.parse(res.items[0].value);
            schema = validator.validateAndNormalizeSchema(schema);
            return dbu.makeSchemaInfo(schema);
        } else {
            return null;
        }
    });
};

DB.prototype.createTable = function(domain, req) {
    var self = this;
    if (!req.table) {
        throw new Error('Table name required.');
    }

    var tableName = domain + '_' + req.table;

    return this._getSchema(tableName)
    .then(function(currentSchema) {
        // Validate and normalize the schema
        var schema = validator.validateAndNormalizeSchema(req);
        var schemaInfo = dbu.makeSchemaInfo(schema);
        var createOperation;
        if (currentSchema) {
            if (currentSchema.hash !== schemaInfo.hash) {
                var migrator;
                try {
                    migrator = new SchemaMigrator(self, req, tableName, currentSchema, schemaInfo);
                }
                catch (error) {
                    throw new dbu.HTTPError({
                        status: 400,
                        body: {
                            type: 'bad_request',
                            title: 'The table already exists, ' +
                                'and its schema cannot be upgraded ' +
                                'to the requested schema (' + error + ').',
                            tableName: tableName,
                            schema: schemaInfo
                        }
                    });
                }
                createOperation = migrator.migrate()
                .then(function() {
                    self.queryCache.keys().filter(function(key) {
                        return key.indexOf(tableName) === 0;
                    })
                    .forEach(function(key) {
                        self.queryCache.del(key);
                    });
                })
                .catch(function(error) {
                    self.log('error/sqlite/table_update', error);
                    throw error;
                });
            } else {
                return { status: 201 };
            }
        } else {
            createOperation = self._createTable(tableName, schemaInfo);
        }
        return createOperation.then(function() {
            self.schemaCache[tableName] = schemaInfo;
            return self._put(self.schemaTableName, {
                attributes: {
                    table: tableName,
                    value: JSON.stringify(schema)
                }
            });
        });
    });
};

DB.prototype._createTable = function(tableName, schema) {
    var self = this;
    var queries = [];
    queries.push({ sql: dbu.buildTableSql(schema, tableName) });
    queries.push({ sql: dbu.buildStaticsTableSql(schema, tableName) });
    dbu.buildSecondaryIndexTableSql(schema, tableName).forEach(function(sql) {
        queries = queries.concat({ sql: sql });
    });
    return self.client.run(queries);
};

DB.prototype.dropTable = function(domain, bucket) {
    var self = this;
    var tableName = domain + '_' + bucket;
    var deleteRequest = function(schema) {
        var queries = [
            { sql: 'drop table [' + tableName + '_data]' },
            dbu.buildDeleteQuery(self.schemaTableName, { table: tableName })
        ];
        var secondaryIndexNames = Object.keys(schema.secondaryIndexes);
        if (secondaryIndexNames.length > 0) {
            secondaryIndexNames.forEach(function(indexName) {
                queries.push({ sql: 'drop index [' + tableName + '_index_' + indexName + ']' });
            });
            queries.push({ sql: 'drop table [' + tableName + '_secondaryIndex]' });
        }
        if (dbu.staticTableExist(schema)) {
            queries.push({ sql: 'drop table [' + tableName + '_static]' });
        }
        return self.client.run(queries);
    };

    if (!self.schemaCache[tableName]) {
        return this._getSchema(tableName)
        .then(function(schema) {
            return deleteRequest(schema);
        });
    } else {
        var schema = self.schemaCache[tableName];
        delete self.schemaCache[tableName];
        return deleteRequest(schema);
    }
};

DB.prototype.get = function(domain, req) {
    var self = this;

    var tableName = domain + '_' + req.table;

    if (!self.schemaCache[tableName]) {
        return this._getSchema(tableName)
        .then(function(schema) {
            self.schemaCache[tableName] = schema;
            return self._get(tableName, req, schema, {
                includePreparedForDelete: true
            });
        });
    } else {
        return P.try(function() {
            return self._get(tableName, req, self.schemaCache[tableName], {
                includePreparedForDelete: true,
                withTTL: req.withTTL
            });
        });
    }
};

DB.prototype._createGetQuery = function(tableName, req, schema, includePreparedForDelete) {
    var extracted = dbu.extractGetParams(req, schema, includePreparedForDelete);
    var key = tableName + ':' + stringify(req);
    var query = this.queryCache.get(key);
    var getQuery;
    if (query) {
        getQuery = {
            sql: query,
            params: extracted
        };
    } else {
        var newQuery = dbu.buildGetQuery(tableName, req, schema, includePreparedForDelete);
        getQuery = {
            sql: this.client.prepare(newQuery),
            params: extracted
        };
        this.queryCache.set(key, getQuery.sql);
    }
    return getQuery;
};

DB.prototype._get = function(tableName, req, schema, options) {
    var self = this;
    options = options || {};
    validator.validateGetRequest(req, schema);
    var buildResult = self._createGetQuery(tableName, req,
            schema, options.includePreparedForDelete);
    return self.client.all(buildResult.sql, buildResult.params)
    .then(function(result) {
        if (!result) {
            return {
                count: 0,
                items: []
            };
        }
        var rows = [];
        var convertRow = function(row) {
            if (options.withTTL) {
                row._ttl = Math.floor((row._exist_until - new Date().getTime()) / 1000);
            }
            delete row._exist_until;
            Object.keys(row).forEach(function(key) {
                if (schema.attributes[key]) {
                    row[key] = schema.converters[schema.attributes[key]].read(row[key]);
                }
            });
            return row;
        };
        if (result instanceof Array) {
            rows = result.map(convertRow) || [];
        } else {
            rows.push(convertRow(result));
        }
        result = {
            count: rows.length,
            items: rows
        };
        if (req.next || req.limit) {
            result.next = (req.next || 0) + rows.length;
        }
        return result;
    })
    .catch(function(err) {
        if (err instanceof Object && err.cause && err.cause.code === 'SQLITE_ERROR') {
            return {
                count: 0,
                items: []
            };
        } else {
            throw err;
        }
    });
};

DB.prototype.put = function(domain, req) {
    var self = this;
    var tableName = domain + '_' + req.table;
    if (!self.schemaCache[tableName]) {
        return self._getSchema(tableName)
        .then(function(schema) {
            self.schemaCache[tableName] = schema;
            return self._put(tableName, req);
        });
    } else {
        return P.try(function() {
            return self._put(tableName, req);
        });
    }
};

DB.prototype._put = function(tableName, req) {
    var self = this;
    var schema = this.schemaCache[tableName];
    validator.validatePutRequest(req, schema);

    if (!req.attributes[schema.tid]) {
        req.attributes[schema.tid] = TimeUuid.now().toString();
    }

    if (req.attributes._ttl) {
        req.attributes._exist_until = new Date().getTime() + req.attributes._ttl * 1000;
        delete req.attributes._ttl;
    }

    req.timestamp = TimeUuid.fromString(req.attributes[schema.tid].toString()).getDate();
    var queries = dbu.buildPutQuery(req, tableName, schema);
    dbu.buildSecondaryIndexUpdateQuery(req, tableName, schema).forEach(function(query) {
        queries.push(query);
    });
    return self.client.run(queries)
    .then(function() {
        self._revisionPolicyUpdate(tableName, req, schema);
        return { status: 201 };
    });
};

DB.prototype._revisionPolicyUpdate = function(tableName, query, schema) {
    var self = this;

    if (!schema.revisionRetentionPolicy || schema.revisionRetentionPolicy.type === 'all') {
        return P.resolve();
    }

    function createDataQuery(limitTime) {
        var dataQuery = {
            table: query.table,
            attributes: {}
        };
        schema.iKeys.forEach(function(att) {
            if (att !== schema.tid) {
                dataQuery.attributes[att] = query.attributes[att];
            }
        });
        if (limitTime) {
            dataQuery.attributes[schema.tid] = {
                ge: TimeUuid.fromDate(new Date(limitTime)).toString()
            };
        }
        dataQuery.order = {};
        dataQuery.order[schema.tid] = 'desc';
        return dataQuery;
    }

    function setTTLs(result) {
        var expireTime = new Date().getTime() + schema.revisionRetentionPolicy.grace_ttl * 1000;
        if (result.count > schema.revisionRetentionPolicy.count) {
            var extraItems = result.items.slice(schema.revisionRetentionPolicy.count);
            return P.all(extraItems.map(function(item) {
                var updateQuery = {
                    table: query.table,
                    attributes: item
                };
                updateQuery.attributes._exist_until = expireTime;
                return dbu.buildPutQuery(updateQuery, tableName, schema, true);
            }))
            .reduce(function(accumulator, item) { return accumulator.concat(item); }, [])
            .then(function(queries) {
                queries.push(dbu.buildDeleteExpiredQuery(schema, tableName));
                return self.client.run(queries);
            });
        }
    }

    var dataQuery;
    // Step 1: set _exists_until for required rows
    if (schema.revisionRetentionPolicy.type === 'latest') {
        dataQuery = createDataQuery();
    } else if (schema.revisionRetentionPolicy.type === 'interval') {
        // 1. Need to check if there are enough renders more than 'interval' time ago
        var interval = schema.revisionRetentionPolicy.interval * 1000;
        var tidTime = query.timestamp.getTime();
        var intervalLimitTime = tidTime - tidTime % interval;
        dataQuery = createDataQuery(intervalLimitTime);
    }
    return P.try(function() {
        return self._get(tableName, dataQuery, schema);
    })
    .then(setTTLs);
};

module.exports = function(options) {
    var db = new DB(options);
    // Create a table to store schemas
    return db.client.run([
        { sql: dbu.buildTableSql(db.infoSchemaInfo, db.schemaTableName) }
    ])
    .then(function() {
        return db;
    });
};
