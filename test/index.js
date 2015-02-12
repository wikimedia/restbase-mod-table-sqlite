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

function roundDecimal(item) {
    return Math.round( item * 100) / 100;
}


var DB = require('../lib/db.js');

describe('DB backend', function() {
    before(function() {
        return makeClient({database:"restbase"})
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
        it('table with secondary index and no tid in range', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/unversionedSecondaryIndexTable',
                method: 'put',
                body: {
                    domain: 'restbase.cassandra.test.local',
                    table: 'unversionedSecondaryIndexTable',
                    attributes: {
                        key: 'string',
                        //tid: 'timeuuid',
                        latestTid: 'timeuuid',
                        uri: 'string',
                        body: 'blob',
                            // 'deleted', 'nomove' etc?
                    },
                    index: [
                        { attribute: 'key', type: 'hash' },
                        { attribute: 'uri', type: 'range', order: 'desc' },
                    ],
                    secondaryIndexes: {
                        by_uri : [
                            { attribute: 'uri', type: 'hash' },
                            { attribute: 'key', type: 'range', order: 'desc' },
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
        it('put with if not exists and non index attributes', function() {
            return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'put',
                    body: {
                        table: "simple-table",
                        if : "not exists",
                        attributes: {
                            key: "testing if not exists",
                            tid: dbu.tidFromDate(new Date('2013-08-10 18:43:58-0700')),
                            body: new Buffer("<p>if not exists with non key attr</p>")
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('put with if and non index attributes', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: "simple-table",
                    attributes: {
                        key: "another test",
                        tid: dbu.tidFromDate(new Date('2013-08-11 18:43:58-0700')),
                        body: new Buffer("<p>test<p>")
                    },
                    if: { body: { "eq": new Buffer("<p>Service Oriented Architecture</p>") } }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
    });
    describe('types', function() {
        this.timeout(5000);
        it('create table', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeTable',
                method: 'put',
                body: {
                    domain: 'restbase.cassandra.test.local',
                    table: 'typeTable',
                    options: { durability: 'low' },
                    attributes: {
                        string: 'string',
                        blob: 'blob',
                        set: 'set<string>',
                        'int': 'int',
                        varint: 'varint',
                        decimal: 'decimal',
                        'float': 'float',
                        'double': 'double',
                        'boolean': 'boolean',
                        timeuuid: 'timeuuid',
                        uuid: 'uuid',
                        timestamp: 'timestamp',
                        json: 'json',
                    },
                    index: [
                        { attribute: 'string', type: 'hash' },
                    ]
                }
            }).then(function(response) {
                deepEqual(response.status, 201);
            });
        });
        it('create sets table', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeSetsTable',
                method: 'put',
                body: {
                    domain: 'restbase.cassandra.test.local',
                    table: 'typeSetsTable',
                    attributes: {
                        string: 'string',
                        set: 'set<string>',
                        blob: 'set<blob>',
                        'int': 'set<int>',
                        varint: 'set<varint>',
                        decimal: 'set<decimal>',
                        'float': 'set<float>',
                        'double': 'set<double>',
                        'boolean': 'set<boolean>',
                        timeuuid: 'set<timeuuid>',
                        uuid: 'set<uuid>',
                        timestamp: 'set<timestamp>',
                        json: 'set<json>',
                    },
                    index: [
                        { attribute: 'string', type: 'hash' },
                    ]
                }
            }).then(function(response) {
                deepEqual(response.status, 201);
            });
        }); 
        it('put', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeTable/',
                method: 'put',
                body: {
                    table: "typeTable",
                    attributes: {
                        string: 'string',
                        blob: new Buffer('blob'),
                        set: ['bar','baz','foo'],
                        'int': -1,
                        varint: -4503599627370496,
                        decimal: '1.2',
                        'float': -1.1,
                        'double': 1.2,
                        'boolean': true,
                        timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                        uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                        timestamp: '2014-11-14T19:10:40.912Z',
                        json: {
                            foo: 'bar'
                        },
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it('put 2', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeTable/',
                method: 'put',
                body: {
                    table: "typeTable",
                    attributes: {
                        string: 'string',
                        blob: new Buffer('blob'),
                        set: ['bar','baz','foo'],
                        'int': 1,
                        varint: 1,
                        decimal: '1.4',
                        'float': -3.434,
                        'double': 1.2,
                        'boolean': true,
                        timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                        uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                        timestamp: '2014-11-14T19:10:40.912Z',
                        json: {
                            foo: 'bar'
                        },
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it('put sets', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeSetsTable/',
                method: 'put',
                body: {
                    table: "typeSetsTable",
                    attributes: {
                        string: 'string',
                        blob: [new Buffer('blob')],
                        set: ['bar','baz','foo'],
                        varint: [-4503599627370496,12233232],
                        decimal: ['1.2','1.6'],
                        'float': [1.3, 1.1],
                        'double': [1.2, 1.567],
                        'boolean': [true, false],
                        timeuuid: ['c931ec94-6c31-11e4-b6d0-0f67e29867e0'],
                        uuid: ['d6938370-c996-4def-96fb-6af7ba9b6f72'],
                        timestamp: ['2014-11-14T19:10:40.912Z', '2014-12-14T19:10:40.912Z'],
                        'int': [123456, 2567, 598765],
                        json: [
                            {one: 1, two: 'two'},
                            {foo: 'bar'},
                            {test: [{a: 'b'}, 3]}
                        ]
                    }
                }
            })
            .then(function(response){
                deepEqual(response, {status:201});
            });
        });
        it("get", function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeTable/',
                method: 'get',
                body: {
                    table: "typeTable",
                    proj: ['string','blob','set','int','varint', 'decimal',
                            'float', 'double','boolean','timeuuid','uuid',
                            'timestamp','json']
                }
            })
            .then(function(response){
                response.body.items[0].float = roundDecimal(response.body.items[0].float);
                response.body.items[1].float = roundDecimal(response.body.items[1].float);
                deepEqual(response.body.items, [{
                    string: 'string',
                    blob: new Buffer('blob'),
                    set: ['bar','baz','foo'],
                    'int': -1,
                    varint: -4503599627370496,
                    decimal: '1.2',
                    'float': -1.1,
                    'double': 1.2,
                    'boolean': true,
                    timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                    uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                    timestamp: '2014-11-14T19:10:40.912Z',
                    json: {
                        foo: 'bar'
                    }
                },{
                    string: 'string',
                    blob: new Buffer('blob'),
                    set: ['bar','baz','foo'],
                    'int': 1,
                    varint: 1,
                    decimal: '1.4',
                    'float': -3.43,
                    'double': 1.2,
                    'boolean': true,
                    timeuuid: 'c931ec94-6c31-11e4-b6d0-0f67e29867e0',
                    uuid: 'd6938370-c996-4def-96fb-6af7ba9b6f72',
                    timestamp: '2014-11-14T19:10:40.912Z',
                    json: {
                        foo: 'bar'
                    },
                }]);
            });
        });
        it("get sets", function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/typeSetsTable/',
                method: 'get',
                body: {
                    table: "typeSetsTable",
                    proj: ['string','set','int','varint', 'decimal',
                            'double','boolean','timeuuid','uuid', 'float',
                            'timestamp','json']
                }
            })
            .then(function(response){
                // note: Cassandra orders sets, so the expected rows are
                // slightly different than the original, supplied ones
                //deepEqual([response.body.items[0].blob[0]], [new Buffer('blob')]);
                /*response.body.items[0].float = [roundDecimal(response.body.items[0].float[0]), 
                                                roundDecimal(response.body.items[0].float[1])];
                */
                deepEqual(response.body.items,  
                    [{
                    "string": "string",
                    // TODO: Fix blob types
                    //"blob": [new Buffer('blob')],
                    "set": ["bar", "baz", "foo"],
                    "int": [123456, 2567, 598765],
                    "varint": [-4503599627370496, 12233232],
                    "decimal": ["1.2", "1.6"],
                    "double": [1.2, 1.567],
                    "boolean": [true, false],
                    "timeuuid": ["c931ec94-6c31-11e4-b6d0-0f67e29867e0"],
                    "uuid": ["d6938370-c996-4def-96fb-6af7ba9b6f72"],
                    "float": [1.3, 1.1],
                    "timestamp": ["2014-11-14T19:10:40.912Z", "2014-12-14T19:10:40.912Z"],
                    "json": [
                            {one: 1, two: 'two'},
                            {foo: 'bar'},
                            {test: [{a: 'b'}, 3]}
                    ]
                }]);
            });
        });
    });
});