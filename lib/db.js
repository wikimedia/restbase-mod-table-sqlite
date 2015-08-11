"use strict";

var dbu = require('./dbutils');
var P = require('bluebird');
var TimeUuid = require("cassandra-uuid").TimeUuid;
var SchemaMigrator = require('./SchemaMigrator');
var Wrapper = require('./clientWrapper');
var validator = require('restbase-mod-table-spec').validator;

function DB(options) {
    this.conf = options.conf;
    this.log = options.log;
    // SQLite client
    this.client = new Wrapper(options);
    this.schemaCache = {};
    this.schemaCache[this.schemaTableName] = this.infoSchemaInfo;
    // Create a table to store schemas
    this.client.run([
        {sql: dbu.buildTableSql(this.infoSchemaInfo, this.schemaTableName)}
    ]);
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
        {attribute: 'table', type: 'hash'},
        {attribute: 'tid', type: 'range', order: 'desc'}
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
    return this._get(this.schemaTableName, {
        attributes: {
            table: tableName
        }
    }, this.infoSchemaInfo)
    .then(function(res) {
        if (res && res.items.length) {
            var schema = JSON.parse(res.items[0].value);
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
                            title: 'The table already exists, and its schema cannot be upgraded to the requested schema (' + error + ').',
                            tableName: tableName,
                            schema: schemaInfo
                        }
                    });
                }
                createOperation = migrator.migrate()
                .catch(function(error) {
                    self.log('error/sqlite/table_update', error);
                    throw error;
                });
            } else {
                return {status: 201};
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
    queries.push({sql: dbu.buildTableSql(schema, tableName)});
    queries.push({sql: dbu.buildStaticsTableSql(schema, tableName)});
    dbu.buildSecondaryIndexTableSql(schema, tableName).forEach(function(sql) {
        queries = queries.concat({sql: sql});
    });
    return self.client.run(queries);
};

DB.prototype.dropTable = function(domain, bucket) {
    var self = this;
    var tableName = domain + '_' + bucket;
    var deleteRequest = function(schema) {
        var queries = [
            {sql: 'drop table [' + tableName + '_data]'},
            dbu.buildDeleteQuery(self.schemaTableName, {'table' : tableName})
        ];
        var secondaryIndexNames = Object.keys(schema.secondaryIndexes);
        if (secondaryIndexNames.length > 0) {
            secondaryIndexNames.forEach(function(indexName) {
                queries.push({sql: 'drop index [' + tableName + '_index_' + indexName + ']'});
            });
            queries.push({sql: 'drop table [' + tableName + '_secondaryIndex]'});
        }
        if (dbu.staticTableExist(schema)) {
            queries.push({sql: 'drop table [' + tableName + '_static]'});
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
            return self._get(tableName, req, schema);
        });
    } else {
        return P.try(function() {
            return self._get(tableName, req, self.schemaCache[tableName]);
        });
    }
};

DB.prototype._get = function(tableName, req, schema, includePreparedForDelete) {
    var self = this;
    var buildResult;

    validator.validateGetRequest(req, schema);

    buildResult = dbu.buildGetQuery(tableName, req, schema, includePreparedForDelete);
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
            delete row._exist_until;
            Object.keys(row).forEach(function(key) {
                row[key] = schema.converters[schema.attributes[key]].read(row[key]);
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

    req.timestamp = TimeUuid.fromString(req.attributes[schema.tid].toString()).getDate();
    var query = dbu.buildPutQuery(req, tableName, schema);
    var queries = [ query.data ];
    if (query.static) {
        queries.push(query.static);
    }
    dbu.buildSecondaryIndexUpdateQuery(req, tableName, schema).forEach(function(query) {
        queries.push(query);
    });
    return self.client.run(queries)
    .then(function() {
        self._revisionPolicyUpdate(tableName, req, schema);
        return {status: 201};
    });
};

DB.prototype._revisionPolicyUpdate = function(tableName, query, schema) {
    var self = this;
    // Step 1: set _exists_until for required rows
    if (schema.revisionRetentionPolicy.type === 'latest') {
        var dataQuery = {
            table: query.table,
            attributes: {}
        };
        var expireTime = new Date().getTime() + schema.revisionRetentionPolicy.grace_ttl * 1000;
        schema.iKeys.forEach(function(att) {
            if (att !== schema.tid) {
                dataQuery.attributes[att] = query.attributes[att];
            }
        });
        dataQuery.order = {};
        dataQuery.order[schema.tid] = 'asc';
        return self._get(tableName, dataQuery, schema, false)
        .then(function(result) {
            if (result.count > schema.revisionRetentionPolicy.count) {
                var extraItems = result.items.slice(0, result.count - schema.revisionRetentionPolicy.count);
                return P.all(extraItems.map(function(item) {
                    var updateQuery = {
                        table: query.table,
                        attributes: item
                    };
                    updateQuery.attributes._exist_until = expireTime;
                    return dbu.buildPutQuery(updateQuery, tableName, schema).data;
                }))
                .then(function(queries) {
                    queries.push(dbu.buildDeleteExpiredQuery(schema, tableName));
                    return self.client.run(queries);
                });
            }
        });
    }
};

module.exports = DB;
