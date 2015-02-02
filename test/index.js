"use strict";

global.Promise = require('bluebird');

var assert = require('assert');

var makeClient = require('../lib/index');
var router = require('../test/test_router.js');

function deepEqual (result, expected) {
    try {
        assert.deepEqual(result, expected);
    } catch (e) {
        console.log('Expected:\n' + JSON.stringify(expected, null, 2));
        console.log('Result:\n' + JSON.stringify(result, null, 2));
        throw e;
    }
}

var DB = require('../lib/db.js');

describe('DB backend', function() {
    before(function() {
        return makeClient()
        .then(function(db) {
            DB = db;
            return router.makeRouter();
        });
    });
    describe('createTable', function() {
        this.timeout(15000);
        it('varint table', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/varintTable',
                method: 'put',
                body: {
                    // keep extra redundant info for primary bucket table reconstruction
                    domain: 'restbase.cassandra.test.local',
                    table: 'varintTable',
                    options: { durability: 'low' },
                    attributes: {
                        key: 'string',
                        rev: 'varint',
                    },
                    index: [
                        { attribute: 'key', type: 'hash' },
                        { attribute: 'rev', type: 'range', order: 'desc' }
                    ]
                }
            })
            .then(function(response) {
                deepEqual(response.status, 201);
            });
        });
    });
});