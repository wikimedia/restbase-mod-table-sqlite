"use strict";

var P = require('bluebird');
var sqlite3 = require('sqlite3').verbose();
var poolModule = require('generic-pool');

P.promisifyAll(sqlite3, {suffix: '_p'});

function Wrapper(options) {
    var delay = options.conf.retry_delay || 100;

    this.conf = options.conf;
    this.log = options.log;
    this.retryLimit = options.conf.retry_limit || 5;
    this.randomDelay = function() {
        return Math.ceil(Math.random() * delay);
    };

    this.connectionPool = poolModule.Pool({
        name: 'sqlite',
        create: function(callback) {
            var client = new sqlite3.Database(options.conf.dbname || 'restbase');
            callback(null, client);
        },
        destroy: function(client) {
            client.close();
        },
        max: 1,
        idleTimeoutMillis: options.conf.pool_idle_timeout || 10000,
        log: options.log
    });
    P.promisifyAll(this.connectionPool, {suffix: '_p'});
    this.readerConnection = new sqlite3.Database(options.conf.dbname || 'restbase');
}

/**
 * Run a set of queries within a transaction.
 *
 * @param queries an array of query objects, containing sql field with SQL
 *        and params array with query parameters.
 * @returns {*} operation promise
 */
Wrapper.prototype.run = function(queries) {
    var self = this;
    var retryCount = 0;

    var beginTransaction = function(client) {
        if (self.conf.show_sql) {
            self.log('begin immediate', retryCount);
        }

        return client.run_p('begin immediate')
        .then(function() {
            return client;
        })
        .catch(function(err) {
            if (err && err.cause
                    && err.cause.code === 'SQLITE_BUSY'
                    && retryCount++ < self.retryLimit) {
                return P.delay(self.randomDelay())
                .then(function() {
                    return beginTransaction(client);
                });
            } else {
                self.connectionPool.release(client);
                throw err;
            }
        });
    };

    return self.connectionPool.acquire_p()
    .then(beginTransaction)
    .then(function(client) {
        queries = queries.filter(function(query) {
            return query && query.sql;
        });
        return P.each(queries, function(query) {
            if (self.conf.show_sql) {
                self.log(query.sql);
            }
            return client.run_p(query.sql, query.params);
        })
        .then(function() {
            return client;
        })
        .catch(function(err) {
            if (self.conf.show_sql) {
                self.log('rollback');
            }
            return client.run_p('rollback')
            .finally(function() {
                self.connectionPool.release(client);
                throw err;
            });
        });
    })
    .then(function(client) {
        self.log('commit');
        return client.run_p('commit')
        .finally(function() {
            self.connectionPool.release(client);
        });
    });
};

/**
 * Run read query and return a result promise
 *
 * @param query SQL query to execute
 * @param params query parameters
 * @returns {*} query result promise
 */
Wrapper.prototype.all = function(query, params) {
    var self = this;
    var retryCount = 0;
    if (self.conf.show_sql) {
        self.log(query);
    }
    
    function operation() {
        return self.readerConnection.all_p(query, params)
        .catch(function(err) {
            if (err && err.cause
            && err.cause.code === 'SQLITE_BUSY'
            && retryCount++ < self.retryLimit) {
                return P.delay(self.randomDelay())
                .then(operation);
            } else {
                throw err;
            }
        });
    }

    return operation();
};

module.exports = Wrapper;
