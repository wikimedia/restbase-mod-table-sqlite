"use strict";

var sqlite3 = require('sqlite3').verbose();
var DB = require('./db');

function makeClient (options) {
    options.log = options.log || function(a, b) {console.log(a, b);};
    var client = new sqlite3.Database(options.conf.dbname || 'restbase');
    return Promise.resolve(new DB(client, options));
}

module.exports = makeClient;
