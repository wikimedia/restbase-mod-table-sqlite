'use strict';

const P = require('bluebird');
const mysql = require('mysql');

P.promisifyAll(mysql, { suffix: '_p' });

class Wrapper {
    constructor(options) {
        const delay = options.conf.retry_delay || 100;

        this.conf = options.conf;
        this.log = options.log;
        this.retryLimit = options.conf.retry_limit || 5;
        this.randomDelay = () => Math.ceil(Math.random() * delay);

        this.connectionPool = mysql.createPool({
            connectionLimit: this.retryLimit,
            host: options.conf.host,
            user: options.conf.username,
            password: options.conf.password,
            database: options.conf.database,
            charset: 'LATIN1_GENERAL_CI'
        });
        P.promisifyAll(this.connectionPool, { suffix: '_p' });
    }

    /**
     * Run a set of queries within a transaction.
     * @param {Array} queries an array of query objects, containing sql field with SQL
     *        and params array with query parameters.
     * @return {Promise} operation promise
     */
    run(queries) {
        let retryCount = 0;
        let _connection;

        const tryTransaction = (connection) => {
            if (this.conf.show_sql) {
                this.log('begin');
            }
            return connection.beginTransaction_p()
            .then(() => {
                queries = queries.filter((query) => query && query.sql);
                return P.each(queries, (query) => {
                    if (this.conf.show_sql) {
                        this.log(query.sql);
                    }
                    return connection.query_p(query.sql, query.params);
                });
            })
            .thenReturn(connection)
            .then((connection) => {
                if (this.conf.show_sql) {
                    this.log('commit');
                }
                return connection.commit_p();
            });
        };

        return this.connectionPool.getConnection_p()
            .then((connection) => {
                _connection = P.promisifyAll(connection, { suffix: '_p' /* , multiArgs: true */ });
                return _connection;
            })
            .then((connection) => {
                return tryTransaction(connection)
                .thenReturn(connection)
                .catch((err) => {
                    if (err && err.cause &&
                        err.cause.code === 'ER_LOCK_DEADLOCK' &&
                        retryCount++ < this.retryLimit) {
                        if (this.conf.show_sql) {
                            this.log('rollback');
                        }
                        return P.delay(this.randomDelay())
                        .then(() => connection.rollback_p('rollback'))
                        .then(() => tryTransaction(connection));
                    } else {
                        throw err;
                    }
                });
            })
            .finally(() => {
                if (_connection.state !== 'disconnected') {
                    _connection.destroy();
                }
            });
    }

    read(query, params) {
        let _client;
        let connectionPool = mysql.createPool({
            connectionLimit: this.retryLimit,
            host: this.conf.host,
            user: this.conf.username,
            password: this.conf.password,
            database: this.conf.database,
            charset: 'LATIN1_GENERAL_CI'
        });
        P.promisifyAll(connectionPool, { suffix: '_p' });
        return connectionPool.getConnection_p()
            .then((client) => {
                P.promisifyAll(client, { suffix: '_p' });
                _client = client;
                return client.query_p(query, params);
            })
            .then((result) => {
                _client.destroy();
                return result;
            });
    }
}

module.exports = Wrapper;
