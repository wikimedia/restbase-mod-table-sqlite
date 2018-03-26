"use strict";

const P = require('bluebird');
const sqlite3 = require('sqlite3').verbose();
const genericPool = require('generic-pool');

P.promisifyAll(sqlite3, { suffix: '_p' });

function expandDBName(options) {
    const dbName = options.conf.dbname || 'sqlite.db';
    return dbName.replace(/^~/, process.env.HOME || process.env.USERPROFILE);
}

class Wrapper {
    constructor(options) {
        const delay = options.conf.retry_delay || 100;

        this.conf = options.conf;
        this.log = options.log;
        this.retryLimit = options.conf.retry_limit || 5;
        this.randomDelay = () => Math.ceil(Math.random() * delay);

        this.connectionPool = genericPool.createPool({
            create() {
                return P.resolve(new sqlite3.Database(expandDBName(options)));
            },
            destroy(client) {
                return P.try(() => client.close());
            }
        },
        {
            max: 1,
            idleTimeoutMillis: options.conf.pool_idle_timeout || 10000,
            log: options.log,
            Promise: P
        });
        this.readerConnection = new sqlite3.Database(expandDBName(options));
    }

    /**
     * Run a set of queries within a transaction.
     * @param {Array} queries an array of query objects, containing sql field with SQL
     *        and params array with query parameters.
     * @return {Promise} operation promise
     */
    run(queries) {
        let retryCount = 0;

        const beginTransaction = (client) => {
            if (this.conf.show_sql) {
                this.log('begin immediate', retryCount);
            }

            return client.run_p('begin immediate')
            .thenReturn(client)
            .catch((err) => {
                if (err && err.cause
                        && err.cause.code === 'SQLITE_BUSY'
                        && retryCount++ < this.retryLimit) {
                    return P.delay(this.randomDelay())
                    .then(() => beginTransaction(client));
                } else {
                    this.connectionPool.release(client);
                    throw err;
                }
            });
        };

        return this.connectionPool.acquire()
        .then(beginTransaction)
        .then((client) => {
            queries = queries.filter(query => query && query.sql);
            return P.each(queries, (query) => {
                if (this.conf.show_sql) {
                    this.log(query.sql);
                }
                return client.run_p(query.sql, query.params);
            })
            .then(() => client)
            .catch((err) => {
                if (this.conf.show_sql) {
                    this.log('rollback');
                }
                return client.run_p('rollback')
                .finally(() => this.connectionPool.release(client))
                .thenThrow(err);
            });
        })
        .then((client) => {
            this.log('commit');
            return client.run_p('commit')
            .finally(() => this.connectionPool.release(client));
        });
    }

    /**
     * Run read query and return a result promise
     * @param {Object} query SQL query to execute
     * @param {Object} params query parameters
     * @return {Promise} query result promise
     */
    all(query, params) {
        let retryCount = 0;
        if (this.conf.show_sql) {
            this.log(query.sql, params);
        }

        const operation = () => {
            return query.all_p(params)
            .catch((err) => {
                if (err && err.cause
                && err.cause.code === 'SQLITE_BUSY'
                && retryCount++ < this.retryLimit) {
                    return P.delay(this.randomDelay())
                    .then(operation);
                } else {
                    throw err;
                }
            });
        };

        return operation();
    }

    prepare(query) {
        return this.readerConnection.prepare(query);
    }
}

module.exports = Wrapper;
