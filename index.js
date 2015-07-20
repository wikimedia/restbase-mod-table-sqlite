"use strict";
/*
 * SQLite-backed table storage service
 */

// global includes
var spec = require('restbase-mod-table-spec').spec;
var P    = require('bluebird');

function RBSQLite(options) {
    this.options = options;
    this.conf = options.conf;
    this.log = options.log;
    this.setup = this.setup.bind(this);
    this.store = null;
    this.handler = {
        spec: spec,
        operations: {
            createTable: this.createTable.bind(this),
            dropTable: this.dropTable.bind(this),
            getTableSchema: this.getTableSchema.bind(this),
            get: this.get.bind(this),
            put: this.put.bind(this)
        }
    };
}

RBSQLite.prototype.createTable = function(rb, req) {
    var self = this;
    var store = this.store;

    req.body.table = req.params.table;
    var domain = req.params.domain;

    // check if the domains table exists
    return store.createTable(domain, req.body)
    .then(function() {
        return {
            status: 201, // created
            body: {
                type: 'table_created',
                title: 'Table was created.',
                domain: req.params.domain,
                table: req.params.table
            }
        };
    })
    .catch(function(e) {
        self.log('sqlite/error', e);
        if (e.status >= 400) {
            return {
                status: e.status,
                body: e.body
            };
        }
        return {
            status: 500,
            body: {
                type: 'table_creation_error',
                title: 'Internal error while creating a table within the cassandra storage backend',
                stack: e.stack,
                err: e,
                req: req
            }
        };
    });
};

// Query a table
RBSQLite.prototype.get = function(rb, req) {
    var self = this;
    var rp = req.params;
    if (!rp.rest && !req.body) {
        req.body = {
            table: rp.table,
            limit: 10
        };
    }
    var domain = req.params.domain;
    return this.store.get(domain, req.body)
    .then(function(res) {
        return {
            status: res.items.length ? 200 : 404,
            body: res
        };
    })
    .catch(function(e) {
        self.log('sqlite/error', e);
        return {
            status: 500,
            body: {
                type: 'query_error',
                title: 'Error in Cassandra table storage backend',
                stack: e.stack,
                err: e,
                req: req
            }
        };
    });
};

// Update a table
RBSQLite.prototype.put = function(rb, req) {
    var self = this;
    var domain = req.params.domain;
    return this.store.put(domain, req.body)
    .then(function() {
        return {
            status: 201 // created
        };
    })
    .catch(function(e) {
        self.log('sqlite/error', e);
        return {
            status: 500,
            body: {
                type: 'update_error',
                title: 'Internal error in Cassandra table storage backend',
                stack: e.stack,
                err: e,
                req: req
            }
        };
    });
};

RBSQLite.prototype.dropTable = function(rb, req) {
    var self = this;
    var domain = req.params.domain;
    return this.store.dropTable(domain, req.params.table)
    .then(function() {
        return {
            status: 204 // done
        };
    })
    .catch(function(e) {
        self.log('sqlite/error', e);
        return {
            status: 500,
            body: {
                type: 'delete_error',
                title: 'Internal error in Cassandra table storage backend',
                stack: e.stack,
                err: e,
                req: req
            }
        };
    });
};

RBSQLite.prototype.getTableSchema = function(rb, req) {
    var self = this;
    var domain = req.params.domain;
    return this.store.getTableSchema(domain, req.params.table)
    .then(function(res) {
        return {
            status: 200,
            headers: {etag: res.tid.toString()},
            body: res.schema
        };
    })
    .catch(function(e) {
        self.log('sqlite/error', e);
        return {
            status: 500,
            body: {
                type: 'schema_query_error',
                title: 'Internal error querying table schema in Cassandra storage backend',
                stack: e.stack,
                err: e,
                req: req
            }
        };
    });
};

/*
 * Setup / startup
 *
 * @return {Promise<registry>}
 */
RBSQLite.prototype.setup = function setup() {
    var self = this;
    // Set up storage backend
    var DB = require('./lib/db');
    return P.resolve(new DB(self.options))
    .then(function(store) {
        self.store = store;
        return self.handler;
    });
};


/**
 * Factory
 * @param options
 * @return {Promise<registration>} with registration being the registration
 * object
 */
function makeRBSQLite(options) {
    var rb = new RBSQLite(options);
    return rb.setup();
}

module.exports = makeRBSQLite;

