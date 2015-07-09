"use strict";

var dbu = require('./dbutils');
var assert = require('assert');
var P = require('bluebird');
var TimeUuid = require("cassandra-uuid").TimeUuid;

function DB (client, options) {
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
        { attribute: 'key', type: 'hash' },
        { attribute: 'tid', type: 'range', order: 'desc' }
    ]
});

DB.prototype.infoSchemaInfo = dbu.makeSchemaInfo(DB.prototype.infoSchema, true);

DB.prototype._getSchema = function (keyspace) {
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

DB.prototype.createTable = function (domain, req) {
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
                // TODO: Schema migration
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
                return { status: 201 };
            }
        }
        return P.all([
            self._createTable(keyspace, schemaInfo, 'data'),
            self._createTable(keyspace, self.infoSchemaInfo, 'meta')
        ])
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

DB.prototype._createTable = function (keyspace, schema, tableName) {
    if (!schema.attributes) {
        throw new Error('No attribute definitions for table ' + tableName);
    }

    var tasks = [];

    var sql = 'create table if not exists ' + keyspace + '_' + tableName + ' (';
    sql += Object.keys(schema.attributes).filter(function(attr) {
        // TODO: handle sets
        return !(schema.attributes[attr].indexOf('set') == 0);
    })
    .map(function(attr) {
        var type = schema.attributes[attr];
        return dbu.fieldName(attr) + ' ' + type;
    })
    .join(', ');
    sql += ')';

    tasks.push(this.client.run_p(sql, []));
    return P.all(tasks);
};

DB.prototype.dropTable = function (domain, table) {
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

DB.prototype._get = function (keyspace, req, table, schema) {
    var self = this;
    if (!table) table = 'data';

    if (!schema) {
        throw new Error('restbase-sqlite3: No schema for ' + keyspace + '_' + table);
    }

    var buildResult = dbu.buildGetQuery(keyspace, req, table, schema);
    return self.client.get_p(buildResult.sql, buildResult.params)
    .then(function(result){
        if (!result) return null;
        var rows = [];
        if (result instanceof Array) {
            result.filter(function(row) { return !row._del} )
            .forEach(function(row) {
                delete row._del;
                rows.push(row);
            });
        } else {
            if (!result._del) {
                delete result._del;
                rows.push(result);
            }
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
            console.log(err);
            console.log(err.stack);
            throw err;
        }
    });
};

DB.prototype.put = function (domain, req) {
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

DB.prototype._put = function(keyspace, req, table, action) {
    var self = this;
    if (!table) table = 'data';

    if (!action) action = 'insert';

    var schema;
    if (table === 'meta') {
        schema = this.infoSchemaInfo;
    } else if ( table === "data" ) {
        schema = this.schemaCache[keyspace];
    }

    if (!schema) {
        throw new Error('Table not found!');
    }

    if (!req.attributes[schema.tid]) {
        req.attributes[schema.tid] = TimeUuid.now().toString();
    }

    req.timestamp = TimeUuid.fromString(req.attributes[schema.tid].toString()).getDate();
    var query = dbu.buildPutQuery(req, keyspace, table, schema, action);
    return this.client.run_p(query.sql, query.params)
    .catch(function(err) {
        // Donot throw error if table doesnot exist yet.
        if (err instanceof Object && err.cause.code === 'SQLITE_CONSTRAINT' ) {
            self._put(keyspace, req, table, "update");
        } else {
            throw err;
        }
    })
    .then(function() {
        return {
            status: 201
        };
    });
};

module.exports = DB;
