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
    client.operations.createTable(null, {
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
    });
})
.catch(function(err) {
    console.log(err);
});