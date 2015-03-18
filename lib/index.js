"use strict";

var sqlite3 = require('sqlite3').verbose();
var P = require('bluebird');
var DB = require('./db');

P.promisifyAll(sqlite3, { suffix: '_p' });

function makeClient (options) {
	if (!options) {
		options = {database: 'test_db'};
	}
    var client = new sqlite3.Database(options.database);
    return P.resolve(new DB(client, options));
}

module.exports = makeClient;
