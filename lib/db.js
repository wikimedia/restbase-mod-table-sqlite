var sqlite3 = require('sqlite3').verbose();
var uuid = require('node-uuid');
var dbu = require('./dbutils');
var extend = require('extend');

function DB (client, options) {
    //this.conf = options.conf;
    //this.log = options.log;
    this.client = client;

    // cache keyspace -> schema
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
    ],
    secondaryIndexes: {}
});

DB.prototype.infoSchemaInfo = dbu.makeSchemaInfo(DB.prototype.infoSchema);

DB.prototype._getSchema = function (keyspace) {
    var query = {
        attributes: {
            key: 'schema'
        },
        limit: 1
    };
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

DB.prototype._get = function (keyspace, req, table, schema) {
    var self = this;

    if (!table) {
        table = 'data';
    }

    if (!schema) {
        throw new Error("restbase-sqlite3: No schema for " + keyspace
                + ', table: ' + table);
    }

    if (!schema.iKeyMap) {
        self.log('error/sqlite3/no_iKeyMap', schema);
    }
    var buildResult = dbu.buildGetQuery(keyspace, req, table, schema);
    //if (req.index) {
    //    return this._getSecondaryIndex(keyspace, req, consistency, table, buildResult);
    //}

    var maxLimit = 250;
    if (req.pageSize || req.limit > maxLimit) {
        var rows = [];
        var options = {fetchSize: req.pageSize? req.pageSize : maxLimit};
        if (req.next) {
            var token = dbu.hashKey(this.conf.salt_key);
            token = req.next.substring(0,req.next.indexOf(token)).replace(/_/g,'/').replace(/-/g,'+');
            options.pageState = new Buffer(token, 'base64');
        }
        return new Promise(function(resolve, reject) {
            try {
                self.client.eachRow(buildResult.query, buildResult.params, options,
                    function(n, result){
                        dbu.convertRow(result, schema);
                        if (!result._del) {
                            rows.push(result);
                        }
                    }, function(err, result){
                        if (err) {
                            reject(err);
                        } else {
                            var token = null;
                            if (result.meta.pageState) {
                                token = result.meta.pageState.toString('base64').replace(/\//g,'_').replace(/\+/g,'-') +
                                        dbu.hashKey(self.conf.salt_key);
                            }
                            resolve({
                                count: rows.length,
                                items: rows,
                                next: token
                            });
                       }
                    }
                );
            } catch (e) {
                reject (e);
            }
        });
    }
    return self.client.get_p(buildResult.query, buildResult.params)
    .then(function(result){
        var rows = [];
        if (result instanceof Array) {
            result.forEach(function(rowno) {
                // Apply value conversions
                //dbu.convertRow(row, schema);
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

DB.prototype.put = function (reverseDomain, req) {
    var keyspace = dbu.keyspaceName(reverseDomain, req.table);

    // Get the type info for the table & verify types & ops per index
    var self = this;
    if (!this.schemaCache[keyspace]) {
        return this._getSchema(keyspace)
        .then(function(schema) {
            self.schemaCache[keyspace] = schema;
            return self._put(keyspace, req);
        });
    } else {
        return this._put(keyspace, req);
    }
};


DB.prototype._put = function(keyspace, req, table ) {
    var self = this;

    if (!table) {
        table = 'data';
    }

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
        req.attributes[schema.tid] = uuid.v1();
    }

    req.timestamp = uuid.v1time(req.attributes[schema.tid]);

    // insert into secondary Indexes first
    var batch = [];
    if (schema.secondaryIndexes) {
        for ( var idx in schema.secondaryIndexes) {
            var secondarySchema = schema.secondaryIndexes[idx];
            if (!secondarySchema) {
                throw new Error('Table not found!');
            }
            //if (req.attributes.uri) { console.log(req.attributes.uri, req.timestamp); }
            batch.push(dbu.buildPutQuery(req, keyspace, dbu.idxTable(idx), secondarySchema));
        }
    }
    batch.push(dbu.buildPutQuery(req, keyspace, table, schema));
    //console.log(batch, schema);
    var mainUpdate;
    if (batch.length === 1) {
        // Single query only (no secondary indexes): no need for a batch.
        var query = batch[0];
        mainUpdate = this.client.run_p(query.query, query.params);
    } else {
        mainUpdate = this.client.run_p(batch);
    }
    return mainUpdate
    .then(function(result) {
        // Kick off asynchronous local index rebuild
        if (schema.secondaryIndexes) {
            self._rebuildIndexes(keyspace, req, schema, 3)
            .catch(function(err) {
                self.log('error/cassandra/rebuildIndexes', err);
            });
        }
        // But don't wait for it. Return success straight away.
        return {
            // XXX: check if condition failed!
            status: 201
        };
    });
};

/*
 * Index update algorithm
 *
 * look at sibling revisions to update the index with values that no longer match
 *   - select sibling revisions
 *   - walk results in ascending order and diff each row vs. preceding row
 *      - if diff: for each index affected by that diff, update _deleted for old value
 *        using that revision's TIMESTAMP.
 * @param {string} keyspace
 * @param {object} req, the original update request; pass in empty attributes
 *        to match / rebuild all entries
 * @param {object} schema, the table schema
 * @param {array} (optional) indexes, an array of index names to update;
 *        default: all indexes in the schema
 */
DB.prototype._rebuildIndexes = function (keyspace, req, schema, limit, indexes) {
    var self = this;
    if (!indexes) {
        indexes = Object.keys(schema.secondaryIndexes);
    }
    if (indexes.length) {
        var tidKey = schema.tid;

        // Build a new request for the main data table
        var dataReq = {
            table: req.table,
            attributes: {},
            proj: []
        };

        // Narrow down the update to the original request's primary key. If
        // that's empty, the entire index (within the numerical limits) will be updated.
        schema.iKeys.forEach(function(att) {
            if (att !== tidKey) {
                dataReq.attributes[att] = req.attributes[att];
                dataReq.proj.push(att);
            }
        });

        // Select indexed attributes for all indexes to rebuild
        var secondaryKeySet = {};
        indexes.forEach(function(idx) {
            // console.log(idx, JSON.stringify(schema.secondaryIndexes));
            Object.keys(schema.attributeIndexes).forEach(function(att) {
                if (!schema.iKeyMap[att] && !secondaryKeySet[att]) {
                    dataReq.proj.push(att);
                    secondaryKeySet[att] = true;
                }
            });
        });
        var secondaryKeys = Object.keys(secondaryKeySet);
        // Include the data table's _del column, so that we can deal with
        // deleted rows there
        dataReq.proj.push('_del');
        if (!secondaryKeySet[tidKey]) {
            dataReq.proj.push(tidKey);
        }

        // XXX: handle the case where reqTid is not defined!
        var reqTid = req.attributes[schema.tid];
        var reqTime = uuid.v1time(reqTid);

        // Clone the query, and create le & gt variants
        var newerDataReq = extend(true, {}, dataReq);
        // 1) select one newer index entry
        newerDataReq.attributes[schema.tid] = { 'ge': reqTid };
        newerDataReq.order = {};
        newerDataReq.order[schema.tid] = 'asc'; // select sibling entries
        newerDataReq.limit = 2; // data entry + newer entry
        var newerRebuild = self._get(keyspace, newerDataReq, 'data', schema)
        .then(function(res) {
            var newerRebuilder = new secIndexes.IndexRebuilder(self, keyspace,
                    schema, secondaryKeys, reqTime);
            // XXX: handle the case where reqTid is not defined?
            for (var i = res.items.length - 1; i >= 0; i--) {
                // Process rows in reverse chronological order
                var row = res.items[i];
                newerRebuilder.handleRow(null, row, true);
            }
        });
        var mainRebuild = new Promise(function(resolve, reject) {
            try {
                dataReq.attributes[schema.tid] = {'le': reqTid};
                dataReq.limit = limit; // typically something around 3, or unlimited
                var reqOptions = {
                    prepare : true,
                    fetchSize : 1000,
                    autoPage: true
                };
                // Traverse the bulk of the data, in timestamp descending order
                // (reverse chronological)
                var dataQuery = dbu.buildGetQuery(keyspace, dataReq, consistency, 'data', schema);
                var mainRebuilder = new secIndexes.IndexRebuilder(self, keyspace,
                        schema, secondaryKeys, reqTime);
                self.client.eachRow(dataQuery.query, dataQuery.params, reqOptions,
                    // row callback
                    mainRebuilder.handleRow.bind(mainRebuilder),
                    // end callback
                    function (err, result) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve();
                        }
                    }
                );
            } catch (e) {
                reject (e);
            }
        });

        return Promise.all([newerRebuild, mainRebuild]);
    } else {
        return Promise.resolve();
    }
};



DB.prototype.createTable = function (reverseDomain, req) {

    var self = this;
    if (!req.table) {
        throw new Error('Table name required.');
    }

    var keyspace = dbu.keyspaceName(reverseDomain, req.table);
 
    return this._getSchema(keyspace)
    .then(function(gSchema) {
        return gSchema;
    })
    .then(function(currentSchema) {
        // Validate and normalize the schema
        var schema = dbu.validateAndNormalizeSchema(req);

        var schemaInfo = dbu.makeSchemaInfo(schema);
        if (currentSchema) {
            try {
                assert.deepEqual(currentSchema, schemaInfo);
            } catch(e) {
                return {
                    status: 400,
                    body: {
                        type: 'bad_request',
                        title: 'The table already exists, and its schema cannot be upgraded to the requested schema.',
                    } 
                };
            }
        }
        // console.log(JSON.stringify(internalSchema, null, 2));

        // TODO:2014-11-09:gwicke use info from system.{peers,local} to
        // automatically set up DC replication
        //
        // Always use NetworkTopologyStrategy with default 'datacenter1' for easy
        // extension to cross-DC replication later.
        var replicationOptions = "{ 'class': 'NetworkTopologyStrategy', 'datacenter1': 3 }";

        if (req.options) {
            if (req.options.durability === 'low') {
                replicationOptions = "{ 'class': 'NetworkTopologyStrategy', 'datacenter1': 1 }";
            }
        }


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

            return Promise.all([
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
            })
            .catch(function(e) {
                // TODO: proper error reporting:
                console.log(e);
                if (retries--) {
                    //console.error('Retrying..', retries, e);
                    // Increase the delay by a factor of 2 on average
                    delay = delay * (1.5 + Math.random());
                    return Promise.delay(delay).then(doCreateTables);
                } else {
                    self.log('error/sqlite3/table_creation', e);
                    throw e;
                }
            });

        }

        return doCreateTables();
    })
    .catch(function(e){console.log(e);});
};

DB.prototype._createTable = function (keyspace, schema, tableName) {
    var self = this;
    if (!schema.attributes) {
        throw new Error('No attribute definitions for table ' + tableName);
    }

    var tasks = [];
    if (schema.secondaryIndexes) {
        // Create secondary indexes
        for (var idx in schema.secondaryIndexes) {
            var indexSchema = schema.secondaryIndexes[idx];
            tasks.push(this._createTable(keyspace, indexSchema, 'idx_' + idx +"_ever"));
        }
    }

    var statics = {};
    schema.index.forEach(function(elem) {
        if (elem.type === 'static') {
            statics[elem.attribute] = true;
        }
    });

    // Finally, create the main data table
    var cql = 'create table if not exists '
        + dbu.cassID(keyspace + '_' + tableName) + ' (';
    for (var attr in schema.attributes) {
        var type = schema.attributes[attr];
        cql += dbu.cassID(attr) + ' ';
        switch (type) {
        case 'blob': cql += 'blob'; break;
        case 'set<blob>': cql += 'set<blob>'; break;
        case 'decimal': cql += 'decimal'; break;
        case 'set<decimal>': cql += 'set<decimal>'; break;
        case 'double': cql += 'double'; break;
        case 'set<double>': cql += 'set<double>'; break;
        case 'float': cql += 'float'; break;
        case 'set<float>': cql += 'set<float>'; break;
        case 'boolean': cql += 'boolean'; break;
        case 'set<boolean>': cql += 'set<boolean>'; break;
        case 'int': cql += 'int'; break;
        case 'set<int>': cql += 'set<int>'; break;
        case 'varint': cql += 'varint'; break;
        case 'set<varint>': cql += 'set<varint>'; break;
        case 'string': cql += 'text'; break;
        case 'set<string>': cql += 'set<text>'; break;
        case 'timeuuid': cql += 'timeuuid'; break;
        case 'set<timeuuid>': cql += 'set<timeuuid>'; break;
        case 'uuid': cql += 'uuid'; break;
        case 'set<uuid>': cql += 'set<uuid>'; break;
        case 'timestamp': cql += 'timestamp'; break;
        case 'set<timestamp>': cql += 'set<timestamp>'; break;
        case 'json': cql += 'text'; break;
        case 'set<json>': cql += 'set<text>'; break;
        default: throw new Error('Invalid type ' + type
                     + ' for attribute ' + attr);
        }
        if (statics[attr]) {
            cql += ' static';
        }
        cql += ', ';
    }

    var hashBits = [];
    var rangeBits = [];
    var orderBits = [];
    schema.index.forEach(function(elem) {
        var cassName = dbu.cassID(elem.attribute);
        if (elem.type === 'hash') {
            hashBits.push(cassName);
        } else if (elem.type === 'range') {
            rangeBits.push(cassName);
            orderBits.push(cassName + ' ' + elem.order);
        }
    });

    cql += 'primary key (';
    cql += [hashBits.join(',')].concat(rangeBits).join(',') + '))';

    // console.log(cql);

    // Execute the table creation query
    tasks.push(this.client.run_p(cql, []));
    return Promise.all(tasks);
};

module.exports = DB;