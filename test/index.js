"use strict";

global.Promise = require('bluebird');

var assert = require('assert');

var makeClient = require('../lib/index');
var router = require('../test/test_router.js');

var dbu = require('../lib/dbutils.js');

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
        it('simple table', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table',
                method: 'put',
                body: {
                    // keep extra redundant info for primary bucket table reconstruction
                    domain: 'restbase.cassandra.test.local',
                    table: 'simple-table',
                    options: { durability: 'low' },
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid',
                        latestTid: 'timeuuid',
                        body: 'blob',
                            'content-type': 'string',
                            'content-length': 'varint',
                                'content-sha256': 'string',
                                // redirect
                                'content-location': 'string',
                                    // 'deleted', 'nomove' etc?
                        //restrictions: 'set<string>',
                    },
                    index: [
                        { attribute: 'key', type: 'hash' },
                        { attribute: 'latestTid', type: 'static' },
                        { attribute: 'tid', type: 'range', order: 'desc' }
                    ]
                }
            })
            .then(function(response) {
                deepEqual(response.status, 201);
            });
        });
        it('table with more than one range keys', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable',
                method: 'put',
                body: {
                    domain: 'restbase.cassandra.test.local',
                    table: 'multiRangeTable',
                    options: { durability: 'low' },
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid',
                        latestTid: 'timeuuid',
                        uri: 'string',
                        body: 'blob',
                            // 'deleted', 'nomove' etc?
                        //restrictions: 'set<string>',
                    },
                    index: [
                    { attribute: 'key', type: 'hash' },
                    { attribute: 'latestTid', type: 'static' },
                    { attribute: 'tid', type: 'range', order: 'desc' },
                        { attribute: 'uri', type: 'range', order: 'desc' }
                    ]
                }
            })
            .then(function(response) {
                deepEqual(response.status, 201);
            });
        });
        it('table with secondary index', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable',
                method: 'put',
                body: {
                    domain: 'restbase.cassandra.test.local',
                    table: 'simpleSecondaryIndexTable',
                    options: { durability: 'low' },
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid',
                        latestTid: 'timeuuid',
                        uri: 'string',
                        body: 'blob',
                            // 'deleted', 'nomove' etc?
                        //restrictions: 'set<string>',
                    },
                    index: [
                    { attribute: 'key', type: 'hash' },
                    { attribute: 'tid', type: 'range', order: 'desc' },
                    ],
                    secondaryIndexes: {
                        by_uri : [
                            { attribute: 'uri', type: 'hash' },
                            { attribute: 'body', type: 'proj' }
                        ]
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.status, 201);
            });
        });
        it('throws Error on updating above table', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table',
                method: 'put',
                body: {
                    // keep extra redundant info for primary bucket table reconstruction
                    domain: 'restbase.cassandra.test.local',
                    table: 'simple-table',
                    options: { durability: 'low' },
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid',
                        latestTid: 'timeuuid',
                        body: 'blob',
                            'content-type': 'string',
                            'content-length': 'varint',
                                'content-sha256': 'string',
                                // redirect
                                'content-location': 'string',
                                    // 'deleted', 'nomove' etc?
                    },
                    index: [
                        { attribute: 'key', type: 'hash' },
                        { attribute: 'latestTid', type: 'static' },
                        { attribute: 'tid', type: 'range', order: 'desc' }
                    ]
                }
            }).then(function(response){
                deepEqual(response.status, 400);
            });
        });
    });
     describe('put', function() {
        it('simple put insert', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    attributes: {
                        key: 'testing',
                        tid: dbu.tidFromDate(new Date('2013-08-08 18:43:58-0700')),
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('simple put insert query on table with more than one range keys', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable/',
                method: 'put',
                body: {
                    table: "multiRangeTable",
                    attributes: {
                        key: 'testing',
                        tid: dbu.tidFromDate(new Date('2013-08-08 18:43:58-0700')),
                        uri: "test"
                    },
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('simple put update', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    attributes: {
                        key: "testing",
                        tid: dbu.tidFromDate(new Date('2013-08-08 18:43:58-0700')),
                        body: new Buffer("<p>Service Oriented Architecture</p>")
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
    });
});