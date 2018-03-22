"use strict";

const dbu = require('./dbutils');
const P = require('bluebird');
const SchemaMigrator = require('./SchemaMigrator');
const Wrapper = require('./clientWrapper');
const LRU = require('lru-cache');
const validator = require('restbase-mod-table-spec').validator;
const stringify = require('json-stable-stringify');

class DB {
    constructor(options) {
        this.conf = options.conf;
        this.log = options.log;
        // SQLite client
        this.client = new Wrapper(options);
        this.schemaCache = {};
        this.schemaCache[this.schemaTableName] = this.infoSchemaInfo;
        this.queryCache = new LRU({
            max: 500,
            dispose(key, statement) {
                statement.finalize();
            }
        });
    }

    getTableSchema(domain, bucket) {
        const tableName = `${domain}_${bucket}`;
        return this._get(this.schemaTableName, {
            attributes: {
                table: tableName
            }
        }, this.infoSchemaInfo)
        .then((res) => {
            if (res && res.items.length) {
                return {
                    status: 200,
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
    }

    _getSchema(tableName) {
        return P.try(() => this._get(this.schemaTableName, {
            attributes: {
                table: tableName
            }
        }, this.infoSchemaInfo))
        .then((res) => {
            if (res && res.items.length) {
                let schema = JSON.parse(res.items[0].value);
                schema = validator.validateAndNormalizeSchema(schema);
                return dbu.makeSchemaInfo(schema);
            } else {
                return null;
            }
        });
    }

    createTable(domain, req) {
        if (!req.table) {
            throw new Error('Table name required.');
        }

        const tableName = `${domain}_${req.table}`;

        return this._getSchema(tableName)
        .then((currentSchema) => {
            // Validate and normalize the schema
            const schema = validator.validateAndNormalizeSchema(req);
            const schemaInfo = dbu.makeSchemaInfo(schema);
            let createOperation;
            if (currentSchema) {
                if (currentSchema.hash !== schemaInfo.hash) {
                    let migrator;
                    try {
                        migrator = new SchemaMigrator(this, req, tableName,
                            currentSchema, schemaInfo);
                    } catch (error) {
                        throw new dbu.HTTPError({
                            status: 400,
                            body: {
                                type: 'bad_request',
                                title: `The table already exists`
                                    + `, and its schema cannot be upgraded `
                                    + `to the requested schema (${error}).`,
                                tableName,
                                schema: schemaInfo
                            }
                        });
                    }
                    createOperation = migrator.migrate()
                    .then(() => {
                        this.queryCache.keys().filter(key => key.indexOf(tableName) === 0)
                        .forEach((key) => {
                            this.queryCache.del(key);
                        });
                    })
                    .catch((error) => {
                        this.log('error/sqlite/table_update', error);
                        throw error;
                    });
                } else {
                    return { status: 201 };
                }
            } else {
                createOperation = this._createTable(tableName, schemaInfo);
            }
            return createOperation.then(() => {
                this.schemaCache[tableName] = schemaInfo;
                return this._put(this.schemaTableName, {
                    attributes: {
                        table: tableName,
                        value: JSON.stringify(schema)
                    }
                });
            });
        });
    }

    _createTable(tableName, schema) {
        return this.client.run([
            { sql: dbu.buildTableSql(schema, tableName) },
            { sql: dbu.buildStaticsTableSql(schema, tableName) }
        ]);
    }

    dropTable(domain, bucket) {
        const tableName = `${domain}_${bucket}`;
        const deleteRequest = (schema) => {
            const queries = [
                { sql: `drop table [${tableName}_data]` },
                {
                    sql: `delete from [${this.schemaTableName}_data] where "table" = ?`,
                    params: [ tableName ]
                }
            ];
            if (dbu.staticTableExist(schema)) {
                queries.push({ sql: `drop table [${tableName}_static]` });
            }
            return this.client.run(queries);
        };

        if (!this.schemaCache[tableName]) {
            return this._getSchema(tableName)
            .then(schema => deleteRequest(schema));
        } else {
            const schema = this.schemaCache[tableName];
            delete this.schemaCache[tableName];
            return deleteRequest(schema);
        }
    }

    get(domain, req) {
        const tableName = `${domain}_${req.table}`;

        if (!this.schemaCache[tableName]) {
            return this._getSchema(tableName)
            .then((schema) => {
                this.schemaCache[tableName] = schema;
                return this._get(tableName, req, schema, {
                    includePreparedForDelete: true
                });
            });
        } else {
            return P.try(() => this._get(tableName, req, this.schemaCache[tableName], {
                includePreparedForDelete: true,
                withTTL: req.withTTL
            }));
        }
    }

    _createGetQuery(tableName, req, schema, includePreparedForDelete) {
        const extracted = dbu.extractGetParams(req, schema, includePreparedForDelete);
        const key = `${tableName}:${stringify(req)}`;
        const query = this.queryCache.get(key);
        let getQuery;
        if (query) {
            getQuery = {
                sql: query,
                params: extracted
            };
        } else {
            const newQuery = dbu.buildGetQuery(tableName, req, schema, includePreparedForDelete);
            getQuery = {
                sql: this.client.prepare(newQuery),
                params: extracted
            };
            this.queryCache.set(key, getQuery.sql);
        }
        return getQuery;
    }

    _get(tableName, req, schema, options) {
        options = options || {};
        validator.validateGetRequest(req, schema);
        const buildResult = this._createGetQuery(tableName, req,
            schema, options.includePreparedForDelete);
        return this.client.all(buildResult.sql, buildResult.params)
        .then((result) => {
            if (!result) {
                return {
                    count: 0,
                    items: []
                };
            }
            let rows = [];
            const convertRow = (row) => {
                if (options.withTTL) {
                    row._ttl = Math.floor((row._exist_until - new Date().getTime()) / 1000);
                }
                delete row._exist_until;
                Object.keys(row).forEach((key) => {
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
        .catch((err) => {
            if (err instanceof Object && err.cause && err.cause.code === 'SQLITE_ERROR') {
                return {
                    count: 0,
                    items: []
                };
            } else {
                throw err;
            }
        });
    }

    put(domain, req) {
        const tableName = `${domain}_${req.table}`;
        if (!this.schemaCache[tableName]) {
            return this._getSchema(tableName)
            .then((schema) => {
                this.schemaCache[tableName] = schema;
                return this._put(tableName, req);
            });
        } else {
            return P.try(() => this._put(tableName, req));
        }
    }

    _put(tableName, req) {
        const schema = this.schemaCache[tableName];
        // TODO: Resurrect validation
        // validator.validatePutRequest(req, schema);

        if (req.attributes._ttl) {
            req.attributes._exist_until = new Date().getTime() + req.attributes._ttl * 1000;
            delete req.attributes._ttl;
        }

        const queries = dbu.buildPutQuery(req, tableName, schema);
        return this.client.run(queries).thenReturn({ status: 201 });
    }

    delete(domain, req) {
        const tableName = `${domain}_${req.table}`;
        if (!this.schemaCache[tableName]) {
            return this._getSchema(tableName)
            .then((schema) => {
                this.schemaCache[tableName] = schema;
                return this._delete(tableName, req);
            });
        } else {
            return P.try(() => this._delete(tableName, req));
        }
    }

    _delete(tableName, req) {
        const schema = this.schemaCache[tableName];
        return this.client.run([ dbu.buildDeleteQuery(req, tableName, schema) ])
        .thenReturn({ status: 204 });
    }
}

// Info table schema
DB.prototype.infoSchema = validator.validateAndNormalizeSchema({
    table: 'meta',
    attributes: {
        table: 'string',
        value: 'json'
    },
    index: [
        { attribute: 'table', type: 'hash' }
    ]
});

DB.prototype.infoSchemaInfo = dbu.makeSchemaInfo(DB.prototype.infoSchema, true);
DB.prototype.schemaTableName = 'global_schema';

module.exports = (options) => {
    const db = new DB(options);
    // Create a table to store schemas
    return db.client.run([
        { sql: dbu.buildTableSql(db.infoSchemaInfo, db.schemaTableName) }
    ])
    .then(() => db);
};
