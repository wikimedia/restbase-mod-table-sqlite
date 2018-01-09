"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, it, context, before, beforeEach, after, afterEach */

var yaml = require('js-yaml');
var fs = require("fs");

describe('Functional', function() {
    var conf = yaml.safeLoad(fs.readFileSync(__dirname + '/test_client.conf.yaml'));
    var dbConstructor = require('../index.js');
    require('restbase-mod-table-spec').test(function() {
        return dbConstructor({
            conf: conf,
            log: function() {}
        });
    });
});

// Run jshint as part of normal testing
require('mocha-jshint')();
// Run jscs as part of normal testing
require('mocha-jscs')();
// Run eslint as part of normal testing
require('mocha-eslint')([ 'lib', 'index.js' ]);
