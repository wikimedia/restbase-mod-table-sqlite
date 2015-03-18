var assert = require('assert');
var sqlite3 = require('sqlite3').verbose();
var uuid = require('node-uuid');
var dbu = require('./dbutils');
var extend = require('extend');
var P = require('bluebird');

function DB (client, options) {
    this.conf = options.conf;
    this.log = options.log;
    this.client = client;

    // cache keyspace -> schema
    this.schemaCache = {};
}

/**
 * Wrap common internal request state
 */
function InternalRequest (opts) {
    this.domain = opts.domain;
    this.table = opts.table;
    this.keyspace = opts.keyspace || dbu.keyspaceName(opts.domain, opts.table);
    this.query = opts.query || null;
    this.schema = opts.schema || null;
    this.columnfamily = opts.columnfamily || 'data';
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
    /*if (query.consistency && query.consistency in {all:1, localQuorum:1}) {
        consistency = cass.types.consistencies[query.consistency];
    }*/
    var req = new InternalRequest({
        domain: domain,
        table: table,
        query: query,
        columnfamily: 'data'
    });
    var schemaCacheKey = JSON.stringify([req.keyspace, domain]);
    req.schema = this.schemaCache[schemaCacheKey];

    if (req.schema) {
        return P.resolve(req);
    } else {
        var schemaQuery = {
            attributes: {
                key: 'schema'
            },
            limit: 1
        };
        var schemaReq = req.extend({
            query: schemaQuery,
            columnfamily: 'meta',
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
            // Check if the keyspace & meta column family exists
            return self.client.exec_p('SELECT name FROM '
                + 'sqlite_master WHERE type=\'table\'=? ', [req.keyspace+'_meta'])
            .then(function (res) {
                if (res && res.rows.length === 0) {
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
        throw new Error("restbase-sqlite: No schema for " + req.keyspace + ', table: ' + req.columnfamily);
    }

    if (!req.schema.iKeyMap) {
        self.log('error/cassandra/no_iKeyMap', req.schema);
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
    //                    if (row._is_latest) {
    //                        delete row._is_latest;
    //                    }
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
    return self.client.all_p(buildResult.cql, buildResult.params)
    .catch(function(err) {
        // Donot throw error if table doesnot exist yet.
        if (err instanceof Object && err.cause && err.cause.code === 'SQLITE_ERROR') {
            return [];
        } else {
            throw err;
        }
    })
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
                if (row._is_latest  || row._is_latest===null) {
                    delete row._is_latest;
                }
                if (!row._del) {
                    rows.push(row);
                }
            });
        } else {
            if (!result._del) {
                rows.push(result);
            }
            if (row._is_latest) {
                delete row._is_latest;
            }
        }
        return {
            count: rows.length,
            items: rows
        };
    })
    .catch(function(err) {
        // Donot throw error if table doesnot exist yet.
        if (err instanceof Object && err.cause && err.cause.code === 'SQLITE_ERROR') {
            return null;
        } else {
            throw err;
        }
    })
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

    query.timestamp = uuid.v1time(query.attributes[schema.tid]);

    var batch = [];
    batch = dbu.buildPutQuery(req, action);
    //console.log(batch, schema);
    var mainUpdate;
    if (batch.length === 1) {
        // Single query only (no secondary indexes): no need for a batch.
        var queryInfo = batch[0];
        mainUpdate = this.client.run_p(queryInfo.cql, queryInfo.params);
    } else {
        mainUpdate = new P(function(resolve,reject) {
            self.client.serialize(function() {
                self.client.run('begin transaction;');
                self.client.run(batch[0].cql, batch[0].params, function(err){
                    if (err instanceof Object && err.code === 'SQLITE_CONSTRAINT') {
                        return self._put(req, "update");
                    }
                });
                self.client.run(batch[1].cql, batch[1].params, function(err){
                    if (err instanceof Object && err.code === 'SQLITE_CONSTRAINT') {
                        return self._put(req, "update");
                    }
                });
                self.client.run('end transaction;');
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

DB.prototype.delete = function (domain, query) {
    return this._makeInternalRequest(domain, query.table, query)
    .bind(this)
    .then(this._delete);
};

DB.prototype._delete = function (req) {

    // Mark _del with current timestamp and update the row.
    req.query.attributes._del = uuid.v1();

    return this._put(req);
};

DB.prototype.createTable = function (domain, query) {
    var self = this;
    if (!query.table) {
        throw new Error('Table name required.');
    }
 
    return this._makeInternalRequest(domain, query.table, query)
    .catch(function(err) {
        self.log('error/cassandra/table_creation', err);
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
                        keyspace: req.keyspace,
                        schema: newSchema
                    }
                });
            }
        }

        /*if (req.query.options) {
            if (req.query.options.durability === 'low') {
                replicationOptions = "{ 'class': 'NetworkTopologyStrategy', '" + localDc + "': 1 }";
            }
        }*/

        // Cassandra does not like concurrent keyspace creation. This is
        // especially significant on the first restbase startup, when many workers
        // compete to create the system tables. It is also relevant for complex
        // bucket creation, which also often involves the concurrent creation of
        // several sub-buckets backed by keyspaces and tables.
        //
        // The typical issue is getting errors like this:
        // org.apache.cassandra.exceptions.ConfigurationException: Column family
        // ID mismatch
        //
        // See https://issues.apache.org/jira/browse/CASSANDRA-8387 for
        // background.
        //
        // So, our work-around is to retry a few times before giving up.  Our
        // table creation code is idempotent, which makes this a safe thing to do.
        var retries = 100; // We try really hard.
        var delay = 100; // Start with a 1ms delay
        function doCreateTables() {
            return self._createTable(req, newSchemaInfo, 'data')
            .then(function() {
                return self._createTable(req, self.infoSchemaInfo, 'meta');
            })
            .then(function() {
                 // Only store the schema after everything else was created
                var putReq = req.extend({
                    columnfamily: 'meta',
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
                // TODO: proper error reporting:
                console.log(e);
                if (retries--) {
                    //console.error('Retrying..', retries, e);
                    // Increase the delay by a factor of 2 on average
                    delay = delay * (1.5 + Math.random());
                    return P.delay(delay).then(doCreateTables);
                } else {
                    self.log('error/sqlite3/table_creation', e);
                    throw e;
                }
            });

        }

        return doCreateTables();
    })
};

DB.prototype._createTable = function (req, schema, tableName) {
    var self = this;
    if (!schema.attributes) {
        throw new Error('No attribute definitions for table ' + tableName);
    }

    var statics = {}, sql2;;
    schema.index.forEach(function(elem) {
        if (elem.type === 'static') {
            statics[elem.attribute] = true;
        }
    });

    // Create a "_static" table if needed 
    if (Object.keys(statics).length) {
        sql2 = 'create table if not exists '
        + dbu.cassID(req.keyspace + '_' + tableName + '_static') + ' (';
    }

    var hashBits = [];
    var rangeBits = [];
    var orderBits = [];
    schema.index.forEach(function(elem) {
        var cassName = dbu.cassID(elem.attribute);
        if (elem.type === 'hash') {
            hashBits.push(cassName);
            statics[elem.attribute] = true;
        } else if (elem.type === 'range') {
            rangeBits.push(cassName);
            orderBits.push(cassName + ' ' + elem.order);
        }
    });

    // Finally, create the main data table
    var sql1 = 'create table if not exists '
        + dbu.cassID(req.keyspace + '_' + tableName) + ' (';
    for (var attr in schema.attributes) {
        var type = schema.attributes[attr];
        if (sql2 && statics[attr]) {
            sql2 += dbu.cassID(attr) + ' ' + dbu.schemaTypeToSQLType(type) + ', ';
        }
        if (schema.iKeyMap[attr] && schema.iKeyMap[attr].type === 'static') {
            continue;
        }
        sql1 += dbu.cassID(attr) + ' ' + dbu.schemaTypeToSQLType(type);
        sql1 += ', ';
    }

    if (schema.secondaryIndexes) {
        sql1 += ' "_is_latest" timeuuid, ';
    }

    sql1 += 'primary key (';
    sql1 += [hashBits.join(',')].concat(rangeBits).join(',') + '))';
    var sql;
    if (sql2) {
        sql2 += 'primary key (';
        sql2 += [hashBits.join(',')].join(',') + ')';
        sql2 += ' FOREIGN KEY( '+ hashBits.join(', ') +' ) REFERENCES '+ dbu.cassID(req.keyspace + '_' + tableName);
        sql2 += ' (' + hashBits.join(',') + ')) ';

        sql = 'begin transaction; ' + sql1 + ' WITHOUT ROWID; '+ sql2 +' WITHOUT ROWID;  end transaction;'
    } else {
        sql = sql1 + ' WITHOUT ROWID;';
    }
    // Execute the table creation query
    return this.client.exec_p(sql)
    .catch(function(e){console.log(e);})
    .then(function(){
        tasks = [];
        if (schema.secondaryIndexes) {
            // Create secondary indexes
            for (var idx in schema.secondaryIndexes) {
                var indexSchema = schema.secondaryIndexes[idx];
                tasks.push(self._createIndex(req.keyspace, indexSchema, 'idx_' + idx +"_ever"));
            }
        }
        return P.all(tasks);
    });
};

DB.prototype._createIndex = function (keyspace, schema, tableName) {
    var hashBits = [];
    var rangeBits = [];
    var orderBits = [];
    schema.index.forEach(function(elem) {
        var cassName = dbu.cassID(elem.attribute);
        if (elem.type === 'hash') {
            hashBits.push(cassName);
        } else if (elem.type === 'range') {
            //hashBits.push(cassName);
        }
    });
    //hashBits.push('"'+schema.tid+'"');
    hashBits.push('"_is_latest"');
    hashBits.push('"_domain"');
    var sql = 'create index if not exists ' + tableName + ' on "' + keyspace + '_data" ';
    sql += '('+  [hashBits.join(',')].join(',') +')';
    return P.all([this.client.run_p(sql)]);
};

DB.prototype.dropTable = function (domain, table) {
    var keyspace = dbu.keyspaceName(domain, table);
    return this.client.exec_p('drop table ' + cassID(domain+"_"+table));
};

module.exports = DB;