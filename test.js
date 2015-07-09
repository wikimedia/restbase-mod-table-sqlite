var yaml = require('js-yaml');
var fs = require("fs");
var conf = yaml.safeLoad(fs.readFileSync(__dirname + '/test/test_client.conf.yaml'));
var dbConstructor = require('./index.js');
dbConstructor({
    conf: conf,
    log: function() {
    }
})
.then(function(client) {
    return client.operations.createTable(null, {
        uri: '/restbase.cassandra.test.local/sys/table/simple-table',
        method: 'put',
        body: {
            options: {
                durability: 'low',
                compression: [
                    {
                        algorithm: 'deflate',
                        block_size: 256
                    }
                ]
            },
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
                restrictions: 'set<string>',
            },
            index: [
                {attribute: 'key', type: 'hash'},
                {attribute: 'latestTid', type: 'static'},
                {attribute: 'tid', type: 'range', order: 'desc'}
            ]
        },
        params: {
            domain: 'restbase.cassandra.test.local',
            table: 'simple-table'
        }
    })
    .then(function(res) {
        return client.operations.put(null, {
            uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
            method: 'put',
            body: {
                table: 'simple-table',
                consistency: 'localQuorum',
                attributes: {
                    key: 'testing'
                }
            },
            params: {
                domain: 'restbase.cassandra.test.local'
            }
        })
        .then(function(res) {
            return client.operations.get(null, {
                uri:'/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'get',
                body: {
                    table: "simple-table",
                    attributes: {
                        key: 'testing'
                    }
                },
                params: {
                    domain: 'restbase.cassandra.test.local'
                }
            })
            .then(function(res) {
                console.log(res);
            })
        });
    });
})
.catch(function(err) {
    console.log(err);
});