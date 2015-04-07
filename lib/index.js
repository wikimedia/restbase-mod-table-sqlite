"use strict";

var sqlite3 = require('sqlite3').verbose();
var P = require('bluebird');
var DB = require('./db');

P.promisifyAll(sqlite3, { suffix: '_p' });

function makeClient (options) {
	if (!options) {
		options = {};
	}
    return P.resolve(new DB(options));
}

module.exports = makeClient;
