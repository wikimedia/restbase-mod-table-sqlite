var extend = require('extend');
var crypto = require('crypto');
var stringify = require('json-stable-stringify');

var dbu = {};

/*
 * Error instance wrapping HTTP error responses
 *
 * Has the same properties as the original response.
 */
function HTTPError(response) {
    Error.call(this);
    Error.captureStackTrace(this, HTTPError);
    this.name = this.constructor.name;
    this.message = JSON.stringify(response);

    for (var key in response) {
        this[key] = response[key];
    }
}

dbu.HTTPError = HTTPError;

dbu.hashKey = function hashKey (key) {
    return crypto.Hash('sha1')
    .update(key)
    .digest()
    .toString('base64')
        // Replace [+/] from base64 with _ (illegal in Cassandra)
    .replace(/[+\/]/g, '_')
        // Remove base64 padding, has no entropy
    .replace(/=+$/, '');
};

dbu.getValidPrefix = function getValidPrefix (key) {
    var prefixMatch = /^[a-zA-Z0-9_]+/.exec(key);
    if (prefixMatch) {
        return prefixMatch[0];
    } else {
        return '';
    }
};

dbu.makeValidKey = function makeValidKey (key, length) {
    var origKey = key;
    key = key.replace(/_/g, '__')
    .replace(/\./g, '_');
    if (!/^[a-zA-Z0-9_]+$/.test(key)) {
        // Create a new 28 char prefix
        var validPrefix = dbu.getValidPrefix(key).substr(0, length * 2 / 3);
        return validPrefix + dbu.hashKey(origKey).substr(0, length - validPrefix.length);
    } else if (key.length > length) {
        return key.substr(0, length * 2 / 3) + dbu.hashKey(origKey).substr(0, length / 3);
    } else {
        return key;
    }
};

dbu.keyspaceName = function(domain, bucket) {
    var prefix = dbu.makeValidKey(domain, Math.max(26, 48 - bucket.length - 3));
    return prefix
        // 6 chars _hash_ to prevent conflicts between domains & table names
    + '_T_' + dbu.makeValidKey(bucket, 48 - prefix.length - 3);
};

dbu.fieldName = function(name) {
    if (/^[a-zA-Z0-9_]+$/.test(name)) {
        return '"' + name + '"';
    } else {
        return '"' + name.replace(/"/g, '""') + '"';
    }
};

dbu.makeSchemaInfo = function makeSchemaInfo(schema) {
    var psi = extend(true, {}, schema);
    psi.versioned = false;
    var lastElem = schema.index[schema.index.length - 1];
    var lastKey = lastElem.attribute;
    psi.attributes._del = 'timeuuid';
    if (lastKey && lastElem.type === 'range'
            && lastElem.order === 'desc'
            && schema.attributes[lastKey] === 'timeuuid') {
        psi.tid = lastKey;
    } else {
        psi.attributes._tid = 'timeuuid';
        psi.index.push({ attribute: '_tid', type: 'range', order: 'desc' });
        psi.tid = '_tid';
    }

    // Create summary data on the primary data index
    psi.iKeys = dbu.indexKeys(psi.index);
    psi.iKeyMap = {};
    psi.index.forEach(function(elem) {
        psi.iKeyMap[elem.attribute] = elem;
    });
    psi.hash = stringify(psi);
    return psi;
};

dbu.indexKeys = function indexKeys (index) {
    var res = [];
    index.forEach(function(elem) {
        if (elem.type === 'hash' || elem.type === 'range') {
            res.push(elem.attribute);
        }
    });
    return res;
};

dbu.validateAndNormalizeSchema = function validateAndNormalizeSchema(schema) {
    schema.version = schema.version || 1;
    if (schema.version !== 1) {
        throw new Error("Schema version 1 expected, got " + schema.version);
    }
    schema.index = dbu.validateIndexSchema(schema, schema.index);
    return schema;
};

dbu.validateIndexSchema = function validateIndexSchema(schema, index) {
    if (!Array.isArray(index) || !index.length) {
        throw new Error("Invalid index " + JSON.stringify(index));
    }
    var haveHash = false;
    index.forEach(function(elem) {
        if (!schema.attributes[elem.attribute]) {
            throw new Error('Index element ' + JSON.stringify(elem) + ' is not in attributes!');
        }
        switch (elem.type) {
            case 'hash':
                haveHash = true;
                break;
            case 'range':
                if (elem.order !== 'asc' && elem.order !== 'desc') {
                    elem.order = 'desc';
                }
                break;
            case 'static':
            case 'proj':
                break;
            default:
                throw new Error('Invalid index element encountered! ' + JSON.stringify(elem));
        }
    });
    if (!haveHash) {
        throw new Error("Indexes without hash are not yet supported!");
    }
    return index;
};

dbu.buildGetQuery = function(keyspace, query, table, schema) {
    var proj = '*';
    var joinNeeded = false;
    if (query.proj) {
        if (Array.isArray(query.proj)) {
            proj = query.proj.map(dbu.fieldName).join(',');
            joinNeeded = query.proj.some(function(key) {
                return schema.iKeyMap[key] && schema.iKeyMap[key].type === 'static';
            });
        } else if (query.proj.constructor === String) {
            proj = dbu.fieldName(query.proj);
            joinNeeded = schema.iKeyMap[query.proj] && schema.iKeyMap[query.proj].type === 'static';
        } else {
            throw new Error('Unsupported query proj: ' + query.proj + ' of type ' + query.proj.constructor);
        }
    } else {
        joinNeeded = table === 'data';
    }

    if (query.distinct) {
        proj = 'DISTINCT ' + proj;
    }

    var sql;
    if (joinNeeded) {
        sql = 'select ' + proj + ' from ' + keyspace + '_' + table
            + ' natural left outer join ' + keyspace + '_static';
    } else {
        sql = 'select ' + proj + ' from ' + keyspace + '_' + table;
    }

    // Build up the condition
    var params = [];
    if (query.attributes) {
        Object.keys(query.attributes).forEach(function(key) {
            // query should not have non key attributes
            if (!schema.iKeyMap[key]) {
                throw new Error("All request attributes need to be key attributes. Bad attribute: " + key);
            }
        });

        sql += ' where ';
        var condResult = dbu.buildCondition(query.attributes, schema);
        sql += condResult.query;
        params = condResult.params;
    }

    if (query.order) {
        var reversed;
        // Establish whether we need to read in forward or reverse order,
        // which is what Cassandra supports. Also validate the order for
        // consistency.
        for (var att in query.order) {
            var dir = query.order[att];
            if (dir !== 'asc' && dir !== 'desc') {
                throw new Error("Invalid sort order " + dir + " on key " + att);
            }
            var idxElem = schema.iKeyMap[att];
            if (!idxElem || idxElem.type !== 'range') {
                throw new Error("Cannot order on attribute " + att
                + "; needs to be a range index, but is " + idxElem);
            }
            var shouldBeReversed = dir !== idxElem.order;
            if (reversed === undefined) {
                reversed = shouldBeReversed;
            } else if (reversed !== shouldBeReversed) {
                throw new Error("Inconsistent sort order; Cassandra only supports "
                + "reversing the default sort order.");
            }
        }

        // Finally, build up the order query
        var toDir = {
            asc: reversed ? 'desc' : 'asc',
            desc: reversed ? 'asc' : 'desc'
        };
        var orderTerms = [];
        schema.index.forEach(function(elem) {
            if (elem.type === 'range') {
                var dir = toDir[elem.order];
                orderTerms.push(dbu.fieldName(elem.attribute) + ' ' + dir);
            }
        });

        if (orderTerms.length) {
            sql += ' order by ' + orderTerms.join(',');
        }
    }
    console.log(sql);
    return {sql: sql, params: params};
};

dbu.buildPutQuery = function(req, keyspace, table, schema, action) {
    if (!schema) throw new Error('Table not found!');

    var dataKVMap = {};
    var staticKVMap = {};
    schema.iKeys.forEach(function(key) {
        if (req.attributes[key] === undefined) {
            throw new Error("Index attribute " + JSON.stringify(key) + " missing in "
                + JSON.stringify(req) + "; schema: " + JSON.stringify(schema, null, 2));
        } else {
            dataKVMap[key] = req.attributes[key];
            if (schema.iKeyMap[key].type === 'hash') {
                staticKVMap[key] = req.attributes[key];
            }
        }
    });

    var dataCondition = dbu.buildCondition(dataKVMap);
    var staticNeeded = false;

    for (var key in req.attributes) {
        var val = req.attributes[key];
        if (val !== undefined && schema.attributes[key]) {
            if (!schema.iKeyMap[key]) {
                dataKVMap[key] = val;
            } else if (schema.iKeyMap[key].type === 'static') {
                staticKVMap[key] = val;
                staticNeeded = true;
            }
        }
    }

    if (req.if && req.if.constructor === String) {
        req.if = req.if.trim().split(/\s+/).join(' ').toLowerCase();
    }

    var sql = '';
    var staticSql;
    var condResult;
    var cond = '';
    if (action === 'insert' || req.if === 'not exists') {
        var keyList = Object.keys(dataKVMap);
        var proj = keyList.map(dbu.fieldName).join(',');
        sql = 'insert ';
        if (req.if === 'not exists') {
            sql += 'or ignore ';
        } else {
            sql += 'or replace '
        }
        sql += 'into ' + keyspace + '_' + table + ' (' + proj + ') values (';
        sql += Array.apply(null, new Array(keyList.length)).map(function(){ return '?' }).join(', ') + ')' + cond;

        if (staticNeeded) {
            staticSql = 'insert or replace into ' + keyspace + '_static ('
                + Object.keys(staticKVMap).map(dbu.fieldName).join(', ')
                + ') values ('
                + Object.keys(staticKVMap).map(function() { return '?'}).join(', ') + ')';
        }
    } else if ( action === 'update' || req.if ) {
        // TODO: UPDATE IS UNTOUCHED YET
        var condParams = [];
        if (req.if) {
            cond = ' if ';
            condResult = dbu.buildCondition(req.if);
            cond += condResult.query;
            condParams = condResult.params;
        }

        var updateProj = keys.map(dbu.fieldName).join(' = ?,') + ' = ? ';
        sql += 'update ' + keyspace + '_' + table + using + ' set ' + updateProj + ' where ';
        sql += dataCondition.query + cond;
        params = usingParams.concat(params, dataCondition.params, condParams);
    } else {
        throw new Error("Can't Update or Insert");
    }
    result = {
        data: {
            sql: sql,
            params: Object.keys(dataKVMap).map(function(key) {
                return dataKVMap[key]
            })
        }
    };
    if (staticNeeded) {
        result.static = {
            sql: staticSql,
            params: Object.keys(staticKVMap).map(function(key) {
                return staticKVMap[key];
            })
        };
    }
    console.log(sql);
    if (staticNeeded) {
        console.log(staticSql);
    }
    return result;
};

dbu.buildCondition = function(pred) {
    var params = [];
    var keys = [];
    var conjunctions = [];
    for (var predKey in pred) {
        var sql = '';
        var predObj = pred[predKey];
        sql += dbu.fieldName(predKey);
        if (predObj === undefined) {
            throw new Error('Query error: attribute ' + JSON.stringify(predKey) + ' is undefined');
        } else if (predObj === null || predObj.constructor !== Object) {
            // Default to equality
            sql += ' = ?';
            params.push(predObj);
            keys.push(predKey);
        } else {
            var predKeys = Object.keys(predObj);
            if (predKeys.length === 1) {
                var predOp = predKeys[0];
                var predArg = predObj[predOp];
                switch (predOp.toLowerCase()) {
                    case 'eq': sql += ' = ?'; params.push(predArg); keys.push(predKey); break;
                    case 'lt': sql += ' < ?'; params.push(predArg); keys.push(predKey); break;
                    case 'gt': sql += ' > ?'; params.push(predArg); keys.push(predKey); break;
                    case 'le': sql += ' <= ?'; params.push(predArg); keys.push(predKey); break;
                    case 'ge': sql += ' >= ?'; params.push(predArg); keys.push(predKey); break;
                    case 'neq':
                    case 'ne': sql += ' != ?'; params.push(predArg); keys.push(predKey); break;
                    case 'between':
                        sql += ' >= ?' + ' AND '; params.push(predArg[0]); keys.push(predKey);
                        sql += dbu.cassID(predKey) + ' <= ?'; params.push(predArg[1]); keys.push(predKey);
                        break;
                    default: throw new Error ('Operator ' + predOp + ' not supported!');
                }
            } else {
                throw new Error ('Invalid predicate ' + JSON.stringify(pred));
            }
        }
        conjunctions.push(sql);
    }
    return {
        query: conjunctions.join(' AND '),
        keys: keys,
        params: params
    };
};

dbu.buildStaticsTableSql = function(schema, keyspace) {
    var staticFields = [];
    var hashKeys = [];
    var hasRangeKey = false;
    schema.index.forEach(function(index) {
        if (index.type === 'static') {
            if (schema.iKeyMap[index.attribute]
                && schema.iKeyMap[index.attribute].type === 'hash') {
                throw new Error('Hash key cannot be a static column');
            }
            staticFields.push(index.attribute)
        } else if (index.type === 'hash') {
            hashKeys.push(index.attribute);
        } else if (index.type === 'range') {
            hasRangeKey = true;
        }
    });
    if (staticFields.length === 0) {
        return;
    }
    if (hashKeys.length === 0) {
        throw new Error('Trying to create a static column without a hash key');
    }
    if (!hasRangeKey) {
        throw new Error('Cannot create static columns without a range key');
    }
    var sql = 'create table if not exists ';
    sql += keyspace + '_static (';
    sql += hashKeys.concat(staticFields).map(function(key) {
        return dbu.fieldName(key) + ' ' + schema.attributes[key]
    }).join(', ');
    sql += ', primary key (' + hashKeys.map(dbu.fieldName).join(', ') + '), ';
    sql += 'foreign key (' + hashKeys.map(dbu.fieldName).join(', ') + ') ';
    sql += 'references ' + keyspace + '_data )';
    console.log(sql);
    return sql;
};

dbu.buildTableSql = function(schema, keyspace, table) {
    var indexKeys = [];
    var sql = 'create table if not exists ' + keyspace + '_' + table + ' (';
    sql += Object.keys(schema.attributes)
    .filter(function(attr) {
        return !schema.iKeyMap[attr] || schema.iKeyMap[attr].type !== 'static';
    })
    .map(function(attr) {
        if (schema.iKeyMap[attr]
                && (schema.iKeyMap[attr].type === 'hash' || schema.iKeyMap[attr].type === 'range')) {
            indexKeys.push(attr);
        }
        var type = schema.attributes[attr];
        if (type.indexOf('set') == 0) {
            type = 'blob';
        }
        return dbu.fieldName(attr) + ' ' + type;
    })
    .join(', ');
    sql += ', primary key (' + indexKeys.map(dbu.fieldName).join(', ') + ') )';
    console.log(sql);
    return sql;
};

module.exports = dbu;