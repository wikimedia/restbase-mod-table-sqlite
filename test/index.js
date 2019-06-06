'use strict';

var yaml = require('js-yaml');
var fs = require('fs');

describe('Functional', function () {
    var conf = yaml.safeLoad(fs.readFileSync(__dirname + '/test_client.conf.yaml'));
    var dbConstructor = require('../index.js');
    require('restbase-mod-table-spec').test(function () {
        return dbConstructor({
            conf: conf,
            log: function () {}
        });
    });
});
