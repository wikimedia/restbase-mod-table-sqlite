"use strict";
/*
 * Sqlite-backed table storage service
 */

if (!global.Promise) {
    global.Promise = require('bluebird');
}

// global includes
var fs = require('fs');
var yaml = require('js-yaml');
var util = require('util');

// TODO: move to separate package!
var spec = yaml.safeLoad(fs.readFileSync(__dirname + '/table.yaml'));

function reverseDomain (domain) {
    return domain.toLowerCase().split('.').reverse().join('.');
}

function RBSqlite (options) {
    this.setup = this.setup.bind(this);
    this.store = null;
    this.handler = {
        spec: spec,
        operations: {
            createTable: this.createTable.bind(this),
            //dropTable: this.dropTable.bind(this),
            //get: this.get.bind(this),
            //put: this.put.bind(this)
        }
    };
}

RBSqlite.prototype.createTable = function (rb, req) {
    var store = this.store;
    req.body.table = req.params.table;
    var domain = reverseDomain(req.params.domain);

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
                schema: req.body
            }
        };
    });
};

/*
 * Setup / startup
 *
 * @return {Promise<registry>}
 */
RBSqlite.prototype.setup = function setup () {
    var self = this;
    // Set up storage backend
    var backend = require('./lib/index');
    return backend(self.options)
    .then(function(store) {
        self.store = store;
        // console.log('RB setup complete', self.handler);
        return self.handler;
    });
};


/**
 * Factory
 * @param options
 * @return {Promise<registration>} with registration being the registration
 * object
 */
function makeRBSqlite (options) {
    var rb = new RBSqlite(options);
    return rb.setup();
}

module.exports = makeRBSqlite;

