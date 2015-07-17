"use strict";

var P = require('bluebird');

function Wrapper(client, options) {
    this.client = client;
    this.conf = options.conf;
    this.log = options.log;
    this.retryLimit = options.conf.retry_limit || 5;
}

Wrapper.prototype.run = function(queries) {
    var self = this;

    return new P(function(resolve, reject) {
        self.client.serialize(function() {
            var retry_count = 0;
            var request = function() {
                self.client.run('begin transaction', [], error_handler);
                queries.filter(function(query) {
                    return query && query.sql;
                })
                .forEach(function(query) {
                    self.client.run(query.sql, query.params, error_handler);
                });
                self.client.run('end');
                resolve();
            };
            var error_handler = function(err) {
                if (err) {
                    self.client.run('rollback');
                    if (err.status === 'SQLITE_BUSY' && retry_count++ < self.retryLimit) {
                        request();
                    } else {
                        reject({cause: err});
                    }
                }
            };
            if (self.conf.show_sql) {
                queries.forEach(self.log.bind(self));
            }
            request();
        });
    });
};

Wrapper.prototype.all = function(query, params) {
    var self = this;

    return new P(function(resolve, reject) {
        self.client.serialize(function() {
            var retry_count = 0;
            var request = function() {
                self.client.all(query, params, function(err, res) {
                    if (err) {
                        // Retrying the query is a recommended way to handle it.
                        if (err.status === 'SQLITE_BUSY' && retry_count++ < self.retryLimit) {
                            request();
                        } else {
                            reject({cause: err});
                        }
                    } else {
                        resolve(res);
                    }
                });
            };
            if (self.conf.show_sql) {
                self.log(query, params);
            }
            request();
        });
    });
};

module.exports = Wrapper;
