var assert = require('assert');
var sqlite3 = require('sqlite3').verbose();
var uuid = require('node-uuid');
var dbu = require('./dbutils');
var extend = require('extend');
var P = require('bluebird');

function DB (options) {
    this.conf = options.conf;
    this.log = options.log;

    // cache database -> schema
    this.schemaCache = {};

    // cache db connections
    this.dblists = {}
}

/**
 * Wrap common internal request state
 */
function InternalRequest (opts) {
    this.domain = opts.domain;
    this.table = opts.table;
    this.database = opts.database || dbu.databaseName(opts.domain, opts.table);
    this.query = opts.query || null;
    this.schema = opts.schema || null;
    this.sqlTable = opts.sqlTable || 'data';
    this.dbClient = opts.dbClient || null;
}

/**
 * Construct a new InternalRequest based on an existing one, optionally
 * overriding existing properties.
 */
InternalRequest.prototype.extend = function(opts) {
    var req = new InternalRequest(this);
    Object.keys(opts).forEach(function(key) {
        req[key] = opts[key];
    });
    return req;
};

DB.prototype._makeInternalRequest = function (domain, table, query) {
    var self = this;

    var req = new InternalRequest({
        domain: domain,
        table: table,
        query: query,
        sqlTable: 'data'
    });
    var schemaCacheKey = JSON.stringify([req.database, domain]);
    req.schema = this.schemaCache[schemaCacheKey];

    req.dbClient = this.dblists[req.database];

    if (req.schema && req.dbClient) {
        return P.resolve(req);
    } else {
        if (!req.dbClient) {
            var client = new sqlite3.Database(req.database);
            this.dblists[req.database] = client;
            req.dbClient = client;
        }
        var schemaQuery = {
            attributes: {
                key: 'schema'
            },
            limit: 1
        };
        var schemaReq = req.extend({
            query: schemaQuery,
            sqlTable: 'meta',
            schema: this.infoSchemaInfo
        });
        return this._get(schemaReq)
        .then(function(res) {
            if (res.items.length) {
                // Need to parse the JSON manually here as we are using the
                // internal _get(), which doesn't apply transforms.
                var schema = res.items[0].value;
                self.schemaCache[schemaCacheKey] = req.schema = dbu.makeSchemaInfo(schema);
            }
            return req;
        }, function(err) {
            // Check if the database & meta column family exists
            return req.dbClient.all_p('SELECT name FROM '
                + 'sqlite_master WHERE type=\'table\'=? ', ['meta'])
            .then(function (res) {
                if (res && res.length === 0) {
                    // meta column family doesn't exist yet
                    return req;
                } else {
                    // re-throw error
                    throw err;
                }
            });
        });
    }
};


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
    ],
    secondaryIndexes: {}
});

DB.prototype.infoSchemaInfo = dbu.makeSchemaInfo(DB.prototype.infoSchema);

DB.prototype.get = function (domain, query) {
    var self = this;
    return this._makeInternalRequest(domain, query.table, query)
    .then(function(req) {
        return self._get(req);
    });
};

DB.prototype._get = function (req) {
    var self = this;
    if (!req.schema) {
        throw new Error("restbase-sqlite: No schema for " + req.database + ', table: ' + req.sqlTable);
    }

    if (!req.schema.iKeyMap) {
        self.log('error/sqlite/no_iKeyMap', req.schema);
    }

    var buildResult = dbu.buildGetQuery(req);
    
    //var maxLimit = 250;
    //if (req.pageSize || req.limit > maxLimit) {
    //    var rows = []
    //    var options = {fetchSize: req.pageSize? req.pageSize : maxLimit};
    //    if (req.next) {
    //        var token = dbu.hashKey(this.conf.salt_key);
    //        token = req.next.substring(0,req.next.indexOf(token)).replace(/_/g,'/').replace(/-/g,'+');
    //        options.pageState = new Buffer(token, 'base64');
    //    }
    //    return new P(function(resolve, reject) {
    //        try {
    //            self.client.each(buildResult.query, buildResult.params, options,
    //                function(n, result){
    //                    dbu.convertRow(result, schema);
    //                    if (!result._del) {
    //                        rows.push(result);
    //                    }
    //               }, function(err, result){
    //                    if (err) {
    //                        reject(err);
    //                    } else {
    //                        var token = null;
    //                        if (result.meta.pageState) {
    //                            token = result.meta.pageState.toString('base64').replace(/\//g,'_').replace(/\+/g,'-') +
    //                                    dbu.hashKey(self.conf.salt_key);
    //                        }
    //                        resolve({
    //                            count: rows.length,
    //                            items: rows,
    //                            next: token
    //                        });
    //                   }
    //                }
    //            );
    //        } catch (e) {
    //            reject (e);
    //        }
    //    });
    //}
    return req.dbClient.all_p(buildResult.sql, buildResult.params)
    .then(function(result){
        var rows = [];
        if (result instanceof Array) {
            result.forEach(function(row) {
                 // Apply value conversions
                row = dbu.convertRow(row, req.schema);
                // Filter rows that don't match any more
                // XXX: Refine this for queries in the past:
                // - compare to query time for index entries
                // - compare to tid for main data table entries, or use tids there
                //   as well
                if (!row._del) {
                    rows.push(row);
                }
            });
        } else {
            if (!result._del) {
                rows.push(result);
            }
        }
        return {
            count: rows.length,
            items: rows
        };
    });
};

DB.prototype.put = function (domain, query) {
    return this._makeInternalRequest(domain, query.table, query)
    .bind(this)
    .then(this._put);
};

DB.prototype._put = function(req, action) {
    var self = this;

    if (!req.schema) {
        throw new Error('Table not found!');
    }

    if (!action) {
        action = 'insert';
    }

    var schema = req.schema;
    var query = req.query;

    var tid = query.attributes[schema.tid];
    if (!tid) {
        query.attributes[schema.tid] = uuid.v1();
    } else if (tid.constructor === String) {
        query.attributes[schema.tid] = uuid.parse(tid);
    }

    var batch = [];
    batch = dbu.buildPutQuery(req, action);
    //console.log(batch, schema);
    var mainUpdate;
    if (batch.length === 1) {
        // Single query only (no secondary indexes): no need for a batch.
        var queryInfo = batch[0];
        mainUpdate = req.dbClient.run_p(queryInfo.sql, queryInfo.params);
    } else {
        mainUpdate = new P(function(resolve,reject) {
            req.dbClient.serialize(function() {
                req.dbClient.run('begin transaction;');
                req.dbClient.run(batch[0].sql, batch[0].params, function(err){
                    if (err instanceof Object && err.code === 'SQLITE_CONSTRAINT') {
                        return self._put(req, "update");
                    }
                });
                req.dbClient.run(batch[1].sql, batch[1].params, function(err){
                    if (err instanceof Object && err.code === 'SQLITE_CONSTRAINT') {
                        return self._put(req, "update");
                    }
                });
                req.dbClient.run('end transaction;');
                resolve();
            });
        });
    }
    return mainUpdate
    .catch(function(err) {
        // Donot throw error if table doesnot exist yet.
        if (err instanceof Object && err.cause.code === 'SQLITE_CONSTRAINT' ) {
            return self._put(req, "update");
        } else {
            throw err;
        }
    })
    .then(function(result) {
        return {
            // XXX: check if condition failed!
            status: 201
        };
    });
};

DB.prototype.createTable = function (domain, query) {
    var self = this;
    if (!query.table) {
        throw new Error('Table name required.');
    }
 
    return this._makeInternalRequest(domain, query.table, query)
    .catch(function(err) {
        self.log('error/sqlite/table_creation', err);
        throw err;
    })
    .then(function(req) {
        var currentSchemaInfo = req.schema;
        // Validate and normalize the schema
        var newSchema = dbu.validateAndNormalizeSchema(req.query);

        var newSchemaInfo = dbu.makeSchemaInfo(newSchema);

        //console.log(currentSchemaInfo, newSchemaInfo);
        if (currentSchemaInfo) {
            // Table already exists
            // Use JSON.stringify to avoid object equality on functions

            if (JSON.stringify(currentSchemaInfo) === JSON.stringify(newSchemaInfo)) {
                // all good & nothing to do.
                return {
                    status: 201
                };
            } else {
                throw new dbu.HTTPError({
                    status: 400,
                    body: {
                        type: 'bad_request',
                        title: 'The table already exists, and its schema cannot be upgraded to the requested schema.',
                        database: req.database,
                        schema: newSchema
                    }
                });
            }
        }

        return self._createTable(req, newSchemaInfo, 'data')
        .then(function() {
            return self._createTable(req, self.infoSchemaInfo, 'meta');
        })
        .then(function() {
             // Only store the schema after everything else was created
            var putReq = req.extend({
                sqlTable: 'meta',
                schema: self.infoSchemaInfo,
                query: {
                    attributes: {
                        key: 'schema',
                        value: newSchema
                    }
                }
            });
            return self._put(putReq)
            .then(function() {
                return {
                    status: 201
                };
            });
        })
        .catch(function(e) {
            self.log('error/sqlite3/table_creation', e);
            throw e;
        });

    })
};

DB.prototype._createTable = function (req, schema, tableName) {
    var self = this;
    if (!schema.attributes) {
        throw new Error('No attribute definitions for table ' + tableName);
    }

    var statics = {}, staticSQL;
    schema.index.forEach(function(elem) {
        if (elem.type === 'static') {
            statics[elem.attribute] = true;
        }
    });

    // Create a "static" table to support static columns in sqlite 
    if (Object.keys(statics).length) {
        staticSQL = 'create table if not exists '
        + dbu.sqlID('static') + ' (';
    }

    var hashBits = [];
    var rangeBits = [];
    schema.index.forEach(function(elem) {
        var attr = dbu.sqlID(elem.attribute);
        if (elem.type === 'hash') {
            hashBits.push(attr);
            statics[elem.attribute] = true;
        } else if (elem.type === 'range') {
            if (elem.order) {
                rangeBits.push(attr + ' ' + elem.order);
            } else {
                rangeBits.push(attr);
            }
        }
    });

    // Finally, create the main data table
    var mainsql = 'create table if not exists '
        + dbu.sqlID(tableName) + ' (';
    for (var attr in schema.attributes) {
        var type = schema.attributes[attr];
        if (staticSQL && statics[attr]) {
            staticSQL += dbu.sqlID(attr) + ' ' + dbu.schemaTypeToSQLType(type) + ', ';
        }
        if (schema.iKeyMap[attr] && schema.iKeyMap[attr].type === 'static') {
            continue;
        }
        mainsql += dbu.sqlID(attr) + ' ' + dbu.schemaTypeToSQLType(type);
        mainsql += ', ';
    }

    if (schema.secondaryIndexes) {
        mainsql += ' "_is_latest" timeuuid, ';
    }

    mainsql += 'primary key (';
    mainsql += [hashBits.join(',')].concat(rangeBits).join(',') + '))';

    // Add a forign key to the static table, if static columns exists
    // foreign key constraints will guarantee that the referenced rows exist
    var sql;
    if (staticSQL) {
        staticSQL += 'primary key (';
        staticSQL += [hashBits.join(',')].join(',') + ')';
        staticSQL += ' FOREIGN KEY( '+ hashBits.join(', ') +' ) REFERENCES '+ dbu.sqlID(tableName);
        staticSQL += ' (' + hashBits.join(',') + ')) ';

        sql = 'begin transaction; ' + mainsql + ' WITHOUT ROWID; '+ staticSQL +' WITHOUT ROWID;  end transaction;'
    } else {
        sql = mainsql + ' WITHOUT ROWID;';
    }

    // Execute the table creation query
    return req.dbClient.exec_p(sql)
    .catch(function(e){console.log(e);})
    .then(function(){
        tasks = [];
        if (schema.secondaryIndexes) {
            // Create secondary indexes
            for (var idx in schema.secondaryIndexes) {
                var indexSchema = schema.secondaryIndexes[idx];
                tasks.push(self._createIndex(req, indexSchema, 'idx_' + idx +"_ever"));
            }
        }
        return P.all(tasks);
    });
};

DB.prototype._createIndex = function (req, schema, tableName) {
    var hashBits = [];
    schema.index.forEach(function(elem) {
        if (elem.type === 'hash') {
            hashBits.push(dbu.sqlID(elem.attribute));
        }
    });
    hashBits.push('"_is_latest"');
    var sql = 'create index if not exists ' + tableName + ' on "' + 'data" ';
    sql += '('+  [hashBits.join(',')].join(',') +')';
    return req.dbClient.run_p(sql);
};

DB.prototype.delete = function (domain, query) {
    return this._makeInternalRequest(domain, query.table, query)
    .bind(this)
    .then(this._delete);
};

DB.prototype._delete = function (req) {
    var self = this;
    var hasStatic = req.schema.index.some(function(elem) {
     if (elem.type === 'static') {
         return true
     }
    });

    // Mark _del with current timestamp and update the row.
    req.query.attributes._del = uuid.v1();

    return this._put(req, 'update')
    .then(function(res){
        // perform delete in static table as well
        // since static attributes share hash keys we need to
        // check if other rows with same hash key exist before deleting.

        if (res.status === 201 && hasStatic) {
            var sql = 'delete from static where not exists ( ';
            
            var hashKVMap = {};
            Object.keys(req.query.attributes).forEach(function(item){
                if (req.schema.iKeyMap[item] && req.schema.iKeyMap[item].type === "hash") {
                    hashKVMap[item] = req.query.attributes[item];
                }
            })
            var condRes = dbu.buildCondition(hashKVMap, req.schema);
            var selectSQl = 'select * from data where ' + condRes.sql;
            sql += selectSQl + ' and _del IS NULL) and ' + condRes.sql;
            var params = condRes.params.concat(condRes.params);
            return req.dbClient.all_p(sql, params)
        }
    });
    return res;
};

DB.prototype.dropTable = function (domain, table, req) {
    return this._makeInternalRequest(domain, table, req)
    .bind(this)
    .then(this._dropTable);
};

DB.prototype._dropTable = function (req) {
    if (!req.schema) {
        throw new Error('Table name found.');
    }
    var hasStatic = req.schema.index.some(function(elem) {
        if (elem.type === 'static') {
            return true
        }
    });
    if (hasStatic) {
        return req.dbClient.exec_p('drop table ' + dbu.sqlID("data") + 
                                      "; drop table" + dbu.sqlID("meta") +
                                      "; drop table" + dbu.sqlID(+"static"));
    }
    return req.dbClient.exec_p('drop table ' + dbu.sqlID("data") + "; drop table" + dbu.sqlID("meta"));
}

module.exports = DB;