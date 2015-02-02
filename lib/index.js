"use strict";

var sqlite3 = require('sqlite3').verbose();
var DB = require('./db');

Promise.promisifyAll(sqlite3, { suffix: '_p' });

function makeClient (options) {
    var client = new sqlite3.Database(':memory:');
    return Promise.resolve(new DB(client));
}

module.exports = makeClient;
