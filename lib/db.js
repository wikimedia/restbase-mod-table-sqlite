'use strict';

const dbu = require('./dbutils');
const P = require('bluebird');
const SchemaMigrator = require('./SchemaMigrator');
const Wrapper = require('./clientWrapper');
const LRU = require('lru-cache');
const validator = require('restbase-mod-table-spec').validator;
const stringify = require('fast-json-stable-stringify');
const extend = require('extend');

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
        /* Process the array of storage groups declared in the config */
        this._storageGroups = this._buildStorageGroups(this.conf.storage_groups);
        /* The cache holding the already-resolved domain-to-group mappings */
        this._storageGroupsCache = new Map();
    }

    /**
     * Process the storage group configuration.
     * @param {Array} groups the array of group objects to read, each must contain
     *                at least the name and domains keys
     * @return {Array} Array of storage group objects
     */
    _buildStorageGroups(groups) {
        const storageGroups = [];
        if (!Array.isArray(groups)) {
            return storageGroups;
        }
        groups.forEach((group) => {
            const grp = extend(true, {}, group);
            if (!Array.isArray(grp.domains)) {
                grp.domains = [grp.domains];
            }
            grp.domains = grp.domains.map((domain) => {
                if (/^\/.*\/$/.test(domain)) {
                    return new RegExp(domain.slice(1, -1));
                }
                return domain;
            });
            storageGroups.push(grp);
        });
        return storageGroups;
    }

    /**
     * Finds the storage group for a given domain.
     * @param  {string} domain  the domain's name
     * @return {Object}         the group object matching the domain
     */
    _resolveStorageGroup(domain) {
        if (this._storageGroupsCache.has(domain)) {
            return this._storageGroupsCache.get(domain);
        }
        let idx;
        let group;

        // not found in cache, find it
        for (idx = 0; idx < this._storageGroups.length; idx++) {
            const curr = this._storageGroups[idx];
            let domIdx;
            for (domIdx = 0; domIdx < curr.domains.length; domIdx++) {
                const dom = curr.domains[domIdx];
                if (((dom instanceof RegExp) && dom.test(domain)) ||
                    (typeof dom === 'string' && dom === domain)) {
                    group = curr;
                    break;
                }
            }
            if (group) {
                break;
            }
        }
        if (!group) {
            throw new Error(`No storage group configured for ${domain}`);
        }
        // save it in the cache
        this._storageGroupsCache.set(domain, group);
        return group;
    }

    _tableName(domain, table) {
        const name = this._resolveStorageGroup(domain).name;
        return this._tableNameForStorageGroup(name, table);
    }

    _tableNameForStorageGroup(groupName, table) {
        return `${groupName}_${table}`;
    }

    getTableSchema(domain, bucket) {
        const tableName = this._tableName(domain, bucket);
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

        if (domain === '*') {
            return P.each(this._storageGroups, (group) =>
                this._createTable(this._tableNameForStorageGroup(group.name, req.table), req));
        } else {
            return this._createTable(this._tableName(domain, req.table), req);
        }
    }

    _createTable(tableName, req) {
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
                                title: 'The table already exists' +
                                    ', and its schema cannot be upgraded ' +
                                    `to the requested schema (${error}).`,
                                tableName,
                                schema: schemaInfo
                            }
                        });
                    }
                    createOperation = migrator.migrate()
                    .then(() => {
                        this.queryCache.keys().filter((key) => key.indexOf(tableName) === 0)
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
                createOperation = this.client.run([
                    { sql: dbu.buildTableSql(schemaInfo, tableName) },
                    { sql: dbu.buildStaticsTableSql(schemaInfo, tableName) }
                ]);
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

    dropTable(domain, bucket) {
        if (domain === '*') {
            return P.each(this._storageGroups, (group) =>
                this._dropTable(this._tableNameForStorageGroup(group.name, bucket)));
        } else {
            return this._dropTable(this._tableName(domain, bucket));
        }
    }

    _dropTable(tableName) {
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
            .then((schema) => {
                if (!schema) {
                    return;
                }
                deleteRequest(schema);
            });
        } else {
            const schema = this.schemaCache[tableName];
            delete this.schemaCache[tableName];
            return deleteRequest(schema);
        }
    }

    get(domain, req) {
        const tableName = this._tableName(domain, req.table);
        req.attributes._domain = domain;
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
                delete row._domain;
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
        const tableName = this._tableName(domain, req.table);
        req.attributes._domain = domain;
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
        const tableName = this._tableName(domain, req.table);
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
