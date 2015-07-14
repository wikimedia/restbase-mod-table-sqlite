"use strict";

var dbu = require('./dbutils');
var P = require('bluebird');
var TimeUuid = require("cassandra-uuid").TimeUuid;

function DB(client, options) {
    this.conf = options.conf;
    this.log = options.log;
    // SQLite client
    this.client = client;
    this.schemaCache = {};
}

// Info table schema
DB.prototype.infoSchema = dbu.validateAndNormalizeSchema({
    table: 'meta',
    attributes: {
        key: 'string',
        value: 'json',
        tid: 'timeuuid'
    },
    index: [
        {attribute: 'key', type: 'hash'},
        {attribute: 'tid', type: 'range', order: 'desc'}
    ]
});

DB.prototype.infoSchemaInfo = dbu.makeSchemaInfo(DB.prototype.infoSchema, true);

DB.prototype._getSchema = function(keyspace) {
    return this._get(keyspace, {}, 'meta', this.infoSchemaInfo)
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

    var keyspace = dbu.keyspaceName(domain, req.table);

    return this._getSchema(keyspace)
    .then(function(currentSchema) {
        // Validate and normalize the schema
        var schema = dbu.validateAndNormalizeSchema(req);
        var schemaInfo = dbu.makeSchemaInfo(schema);
        if (currentSchema) {
            if (currentSchema.hash !== schemaInfo.hash) {
                // TODO: Schema migration support
                throw new dbu.HTTPError({
                    status: 400,
                    body: {
                        type: 'bad_request',
                        title: 'The table already exists, and its schema cannot be upgraded to the requested schema.',
                        keyspace: keyspace,
                        schema: schema
                    }
                });
            } else {
                return {status: 201};
            }
        }
        return self._createTable(keyspace, schemaInfo)
        .then(function() {
            self.schemaCache[keyspace] = schemaInfo;
            return self._put(keyspace, {
                attributes: {
                    key: 'schema',
                    value: JSON.stringify(schema)
                }
            }, 'meta');
        });
    });
};

DB.prototype._createTable = function(keyspace, schema) {
    var self = this;
    if (!schema.attributes) {
        throw new Error('No attribute definitions for table ' + keyspace);
    }

    var tableSql = dbu.buildTableSql(schema, keyspace, 'data');
    var metaSql = dbu.buildTableSql(self.infoSchemaInfo, keyspace, 'meta');
    var staticSql = dbu.buildStaticsTableSql(schema, keyspace);
    var operation = P.try(function() {
        return self.client.run_p('begin transaction');
    })
    .then(function() {
        return P.all([
            self.client.run_p(tableSql),
            self.client.run_p(metaSql)
        ]);
    });
    if (staticSql) {
        operation = operation.then(self.client.run_p.bind(self.client, staticSql));
    }
    operation.then(function() {
        return self.client.run_p('commit transaction');
    })
    .catch(function(e) {
        return self.client.run_p('rollback transaction');
        throw e;
    });
    return operation;
};

DB.prototype.dropTable = function(domain, table) {
    var self = this;
    var keyspace = dbu.keyspaceName(domain, table);
    return P.all([
        self.client.run_p('drop table ' + keyspace + '_meta', []),
        self.client.run_p('drop table ' + keyspace + '_data', [])
    ]);
};

DB.prototype.get = function(domain, req) {
    var self = this;

    var keyspace = dbu.keyspaceName(domain, req.table);
    if (!self.schemaCache[keyspace]) {
        return this._getSchema(keyspace)
        .then(function(schema) {
            self.schemaCache[keyspace] = schema;
            return self._get(keyspace, req, 'data', schema);
        });
    } else {
        return self._get(keyspace, req, 'data', self.schemaCache[keyspace]);
    }
};

DB.prototype._get = function(keyspace, req, table, schema) {
    var self = this;
    if (!table) table = 'data';

    if (!schema) {
        throw new Error('restbase-sqlite3: No schema for ' + keyspace + '_' + table);
    }
    var buildResult = dbu.buildGetQuery(keyspace, req, table, schema);
    // TODO: utilize get_p as it should be faster for singles
    return self.client.all_p(buildResult.sql, buildResult.params)
    .then(function(result) {
        if (!result) return [];
        var rows = [];
        var convertRow = function(row) {
            Object.keys(row).forEach(function(key) {
                row[key] = dbu.convertFromSQLite(row[key], schema.attributes[key]);
            });
            return row;
        };
        if (result instanceof Array) {
            rows = result.map(convertRow);
        } else {
            rows.push(convertRow(result));
        }
        return {
            count: rows.length,
            items: rows
        };
    })
    .catch(function(err) {
        if (err instanceof Object && err.cause && err.cause.code === 'SQLITE_ERROR') {
            return null;
        } else {
            throw err;
        }
    });
};

DB.prototype.put = function(domain, req) {
    var self = this;
    var keyspace = dbu.keyspaceName(domain, req.table);
    if (!self.schemaCache[keyspace]) {
        return self._getSchema(keyspace)
        .then(function(schema) {
            self.schemaCache[keyspace] = schema;
            return self._put(keyspace, req);
        });
    } else {
        return self._put(keyspace, req);
    }
};

DB.prototype._put = function(keyspace, req, table) {
    var self = this;
    if (!table) table = 'data';

    var schema;
    if (table === 'meta') {
        schema = this.infoSchemaInfo;
    } else if (table === "data") {
        schema = this.schemaCache[keyspace];
    }

    if (!schema) {
        throw new Error('Table not found!');
    }

    if (!req.attributes[schema.tid]) {
        req.attributes[schema.tid] = TimeUuid.now().toString();
    }

    req.timestamp = TimeUuid.fromString(req.attributes[schema.tid].toString()).getDate();
    var query = dbu.buildPutQuery(req, keyspace, table, schema);
    var operation = P.try(function() {
        return self.client.run_p('begin transaction');
    })
    .then(function() {
        return self.client.run_p(query.data.sql, query.data.params);
    });
    if (query.static) {
        operation = operation.then(function() {
            return self.client.run_p(query.static.sql, query.static.params);
        });
    }
    return operation.catch(function(err) {
        self.client.run_p('rollback transaction');
        throw err;
    })
    .then(function() {
        self.client.run_p('commit transaction');
        return {
            status: 201
        };
    });
};

module.exports = DB;
