"use strict";

var DB = require('./db');

function makeClient (options) {
    options.log = options.log || function(a, b) {console.log(a, b);};
    return Promise.resolve(new DB(options));
}

module.exports = makeClient;
