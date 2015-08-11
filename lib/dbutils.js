"use strict";

var extend = require('extend');
var crypto = require('crypto');
var stringify = require('json-stable-stringify');
var TimeUuid = require('cassandra-uuid').TimeUuid;

var dbu = {};

dbu.conversions = {
    json: {
        write: JSON.stringify,
        read: JSON.parse,
        type: 'blob'
    },
    string: {
        read: function(value) {
            if (value !== null
                    && value !== undefined
                    && typeof value !== 'string') {
                return value.toString();
            }
            return value;
        },
        type: 'text'
    },
    blob: {
        write: function(blob) {
            if (!blob) {
                return null;
            }
            if (blob instanceof Buffer) {
                return blob;
            } else {
                return new Buffer(blob);
            }
        },
        read: function(val) {
            if (!val) {
                return null;
            }
            if (val instanceof Buffer) {
                return val;
            } else {
                return new Buffer(val);
            }
        },
        type: 'blob'
    },
    boolean: {
        read: function(value) {
            return value !== 0;
        },
        write: function(value) {
            return value ? 1 : 0;
        },
        type: 'integer'
    },
    decimal: {
        read: toString(),
        type: 'integer'
    },
    timeuuid: {
        // On write we shuffle uuid bits so that the timestamp bits end up
        // first in correct order. This allows to compare time uuid as string
        read: function(value) {
            if (value) {
                value = value.substr(7, 8) + '-'
                + value.substr(3, 4) + '-1'
                + value.substr(0, 3) + '-'
                + value.substr(15);
            }
            return value;
        },
        write: function(value) {
            if (value) {
                if (!TimeUuid.test(value)) {
                    throw new Error('Illegal uuid value ' + value);
                }
                value = value.substr(15, 3)
                + value.substr(9, 4)
                + value.substr(0, 8)
                + value.substr(19);
            }
            return value;
        },
        type: 'text'
    },
    uuid: {
        read: toString()
    }
};

// Conversion factories. We create a function for each type so that it can be
// compiled monomorphically.
function toString() {
    return function(val) {
        if (val) {
            return val.toString();
        }
        return null;
    };
}

function generateSetConverter(convObj) {
    return {
        write: function(valArray) {
            if (!Array.isArray(valArray) || valArray.length === 0) {
                // We treat the Empty set as being equivalent to null
                return null;
            } else {
                return JSON.stringify(valArray.map(convObj.write));
            }
        },
        read: function(valJson) {
            if (!valJson) {
                return null;
            }
            var valArray = JSON.parse(valJson);
            if (valArray) {
                valArray = valArray.map(convObj.read);
                var valSet = new Set(valArray);
                valArray = [];
                valSet.forEach(function(val) {
                    valArray.push(val);
                });
                return valArray.sort(function(val1, val2) {
                    if (typeof val1 === "number"
                    || (typeof val1 === "object" && val1.constructor === Number)) {
                        return val1 - val2;
                    } else {
                        val1 = JSON.stringify(val1);
                        val2 = JSON.stringify(val2);
                        if (val1 === val2) {
                            return 0;
                        } else if (val1 < val2) {
                            return -1;
                        } else {
                            return 1;
                        }
                    }
                });
            }
            return null;
        },
        type: 'blob'
    };
}

function generateConverters(schema) {
    schema.converters = {};
    Object.keys(schema.attributes).forEach(function(key) {
        var set_type = /^set<(\w+)>$/.exec(schema.attributes[key]);
        var obj_type = set_type ? set_type[1] : schema.attributes[key];
        var obj_converter = dbu.conversions[obj_type] || {};
        if (!obj_converter.write) {
            obj_converter.write = function(val) {
                return val;
            };
        }
        if (!obj_converter.read) {
            obj_converter.read = function(val) {
                return val;
            };
        }
        if (!obj_converter.type) {
            obj_converter.type = obj_type;
        }
        if (set_type) {
            schema.converters[schema.attributes[key]] = generateSetConverter(obj_converter);
        } else {
            schema.converters[schema.attributes[key]] = obj_converter;
        }
    });
    return schema;
}

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

function getAllKeysOfTypes(schema, types) {
    return Object.keys(schema.iKeyMap).filter(function(key) {
        return schema.iKeyMap[key] && types.indexOf(schema.iKeyMap[key].type) >= 0;
    });
}

dbu.hashKey = function hashKey(key) {
    return crypto.Hash('sha1')
    .update(key)
    .digest()
    .toString('base64')
        // Replace [+/] from base64 with _ (illegal in Cassandra)
    .replace(/[+\/]/g, '_')
        // Remove base64 padding, has no entropy
    .replace(/=+$/, '');
};

dbu.fieldName = function(name) {
    if (/^[a-zA-Z0-9_]+$/.test(name)) {
        return '"' + name + '"';
    } else {
        return '"' + name.replace(/"/g, '""') + '"';
    }
};

dbu.makeIndexSchema = function makeIndexSchema(dataSchema, indexName) {
    var index = dataSchema.secondaryIndexes[indexName];
    var s = {
        name: indexName,
        attributes: {},
        index: index,
        iKeys: [],
        iKeyMap: {}
    };

    // Build index attributes for the index schema
    index.forEach(function(elem) {
        var name = elem.attribute;
        s.attributes[name] = dataSchema.attributes[name];
        if (elem.type === 'hash' || elem.type === 'range') {
            s.iKeys.push(name);
            s.iKeyMap[name] = elem;
        }
    });

    // Make sure the main index keys are included in the new index
    dataSchema.iKeys.forEach(function(att) {
        if (!s.attributes[att] && att !== dataSchema.tid) {
            s.attributes[att] = dataSchema.attributes[att];
            var indexElem = {type: 'range', order: 'desc'};
            indexElem.attribute = att;
            index.push(indexElem);
            s.iKeys.push(att);
            s.iKeyMap[att] = indexElem;
        }
    });

    // include the orignal schema's conversion table
    s.conversions = {};
    if (dataSchema.conversions) {
        for (var attr in s.attributes) {
            if (dataSchema.conversions[attr]) {
                s.conversions[attr] = dataSchema.conversions[attr];
            }
        }
    }

    // Construct the default projection
    s.proj = Object.keys(s.attributes);
    return s;
};

function findTidElement(schema) {
    for (var key in schema.index) {
        if (schema.index.hasOwnProperty(key)) {
            var element = schema.index[key];
            if (element.type === 'range'
                    && element.order === 'desc'
                    && schema.attributes[element.attribute] === 'timeuuid') {
                return element.attribute;
            }
        }
    }
    return null;
}

dbu.makeSchemaInfo = function makeSchemaInfo(schema) {
    var psi = extend(true, {}, schema);
    var tidAttribute = findTidElement(schema);

    if (tidAttribute) {
        psi.tid = tidAttribute;
    } else {
        psi.attributes._tid = 'timeuuid';
        psi.index.push({attribute: '_tid', type: 'range', order: 'desc'});
        psi.tid = '_tid';
    }
    psi.attributes._exist_until = 'timestamp';

    psi.versioned = false;

    // Create summary data on the primary data index
    psi.iKeys = dbu.indexKeys(psi.index);
    psi.iKeyMap = {};
    psi.index.forEach(function(elem) {
        psi.iKeyMap[elem.attribute] = elem;
    });

    if (!psi.revisionRetentionPolicy) {
        psi.revisionRetentionPolicy = {type: 'all'};
    }
    psi.proj = Object.keys(psi.attributes);

    // Secondary index primary key === main table hash + range keys - tid
    psi.secondaryIndexPrimaryKeys = getAllKeysOfTypes(psi, ['hash', 'range'])
    .filter(function(key) {
        return key !== psi.tid;
    });
    // All secondary index keys joined
    psi.allSecondaryIndexKeys = new Set(psi.secondaryIndexPrimaryKeys);
    Object.keys(psi.secondaryIndexes).forEach(function(indexName) {
        psi.secondaryIndexes[indexName] = dbu.makeIndexSchema(psi, indexName);
        Object.keys(psi.secondaryIndexes[indexName].attributes).forEach(function(key) {
            psi.allSecondaryIndexKeys.add(key);
        });
    });

    psi.hash = stringify(psi);
    generateConverters(psi);
    return psi;
};

dbu.indexKeys = function indexKeys(index) {
    var res = [];
    index.forEach(function(elem) {
        if (elem.type === 'hash' || elem.type === 'range') {
            res.push(elem.attribute);
        }
    });
    return res;
};

function constructOrder(query, schema) {
    var orderTerms = [];
    Object.keys(schema.iKeyMap).forEach(function(key) {
        var elem = schema.iKeyMap[key];
        if (elem.type === 'range') {
            var dir = query && query.order && query.order[elem.attribute]
                ? query.order[elem.attribute]
                : elem.order;
            orderTerms.push(dbu.fieldName(elem.attribute) + ' ' + dir);
        }
    });
    if (orderTerms.length) {
        return ' order by ' + orderTerms.join(',') + ' ';
    } else {
        return '';
    }
}

function constructProj(query, schema) {
    var projArr = query.proj || schema.proj;
    var proj;
    if (Array.isArray(projArr)) {
        proj = projArr.map(dbu.fieldName).join(',');
    } else if (projArr.constructor === String) {
        proj = dbu.fieldName(projArr);
    }
    if (query.distinct) {
        proj = ' distinct ' + proj + ' ';
    }
    return proj;
}

function isStaticJoinNeeded(query, schema) {
    if (query && query.proj) {
        if (Array.isArray(query.proj)) {
            return query.proj.some(function(key) {
                return schema.iKeyMap[key] && schema.iKeyMap[key].type === 'static';
            });
        } else if (query.proj.constructor === String) {
            return schema.iKeyMap[query.proj] && schema.iKeyMap[query.proj].type === 'static';
        } else {
            throw new Error('Unsupported query proj: ' + query.proj + ' of type ' + query.proj.constructor);
        }
    } else {
        return Object.keys(schema.iKeyMap).some(function(key) {
            return schema.iKeyMap[key].type === 'static';
        });
    }
}

dbu.staticTableExist = function(schema) {
    return isStaticJoinNeeded(null, schema);
};

function constructLimit(query) {
    var sql = '';
    if (query.limit) {
        sql += ' limit ' + query.limit;
    }

    if (query.next) {
        sql += ' offset ' + query.next;
    }
    return sql;
}

dbu.buildGetQuery = function(tableName, query, schema, includePreparedForDelete) {
    var proj;
    var limit = constructLimit(query);
    var params = [];
    var condition = '';
    var sql;

    if (includePreparedForDelete === undefined) {
        includePreparedForDelete = true;
    }

    if (query.attributes) {
        var condResult = buildCondition(query.attributes, schema, includePreparedForDelete);
        condition = ' where ' + condResult.query + ' ';
        params = condResult.params;
    }

    if (query.index) {
        var indexSchema = schema.secondaryIndexes[query.index];
        proj = constructProj(query, indexSchema);
        sql = 'select ' + proj + ' from [' + tableName + '_secondaryIndex]' + condition + constructOrder(query, indexSchema) + limit;
    } else {
        proj = constructProj(query, schema);
        if (isStaticJoinNeeded(query, schema)) {
            sql = 'select ' + proj + ' from [' + tableName + '_data]' + ' natural left outer join [' + tableName + '_static]';
        } else {
            sql = 'select ' + proj + ' from [' + tableName + '_data]';
        }
        sql += condition + constructOrder(query, schema) + limit;
    }
    return {sql: sql, params: params};
};

dbu.buildPutQuery = function(req, tableName, schema) {
    var dataKVMap = {};
    var staticKVMap = {};
    var primaryKeyKVMap = {};

    schema.iKeys.forEach(function(key) {
        if (req.attributes[key] && (schema.iKeys.indexOf(key) >= 0)) {
            primaryKeyKVMap[key] = req.attributes[key];
        }
    });

    if (req && req.attributes) {
        Object.keys(req.attributes).forEach(function(key) {
            req.attributes[key] = schema.converters[schema.attributes[key]].write(req.attributes[key]);
        });
    }
    schema.iKeys.forEach(function(key) {
        dataKVMap[key] = req.attributes[key];
        if (schema.iKeyMap[key].type === 'hash') {
            staticKVMap[key] = req.attributes[key];
        }
    });

    var staticNeeded = false;

    Object.keys(req.attributes).forEach(function(key) {
        var val = req.attributes[key];
        if (val !== undefined && schema.attributes[key]) {
            if (!schema.iKeyMap[key]) {
                dataKVMap[key] = val;
            } else if (schema.iKeyMap[key].type === 'static') {
                staticKVMap[key] = val;
                staticNeeded = true;
            }
        }
    });

    if (req.if && req.if.constructor === String) {
        req.if = req.if.trim().split(/\s+/).join(' ').toLowerCase();
    }

    var staticSql;
    var sql;
    var dataParams = [];

    if (req.if instanceof Object) {
        var condition = buildCondition(Object.assign(primaryKeyKVMap, req.if), schema);
        sql = 'update [' + tableName + '_data] set ';
        sql += Object.keys(dataKVMap)
        .filter(function(column) {
            return schema.iKeys.indexOf(column) < 0;
        })
        .map(function(column) {
            dataParams.push(dataKVMap[column]);
            return dbu.fieldName(column) + '= ?';
        }).join(',');
        sql += ' where ' + condition.query;
        dataParams = dataParams.concat(condition.params);
    } else {
        var keyList = Object.keys(dataKVMap);
        var proj = keyList.map(dbu.fieldName).join(',');
        if (req.if === 'not exists') {
            sql = 'insert or ignore ';
        } else {
            sql = 'insert or replace ';
        }
        sql += 'into [' + tableName + '_data] (' + proj + ') values (';
        sql += Array.apply(null, new Array(keyList.length)).map(function() {
            return '?';
        }).join(', ') + ')';

        Object.keys(dataKVMap).map(function(key) {
            dataParams.push(dataKVMap[key]);
        });
    }

    if (staticNeeded) {
        staticSql = 'insert or replace into [' + tableName + '_static] ('
            + Object.keys(staticKVMap).map(dbu.fieldName).join(', ')
            + ') values ('
            + Object.keys(staticKVMap).map(function() {
                return '?';
            }).join(', ') + ')';
    }
    var result = {
        data: {
            sql: sql,
            params: dataParams
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
    return result;
};

dbu.buildSecondaryIndexUpdateQuery = function(req, tableName, schema) {
    var result = [];
    var secondaryIndexNames = Object.keys(schema.secondaryIndexes);
    var dataKVMap = {};
    var sql;

    if (secondaryIndexNames.length === 0) {
        return result;
    }

    sql = 'insert or replace into [' + tableName + '_secondaryIndex] (';
    Object.keys(req.attributes)
    .filter(function(key) {
        return schema.allSecondaryIndexKeys.has(key);
    })
    .forEach(function(key) {
        dataKVMap[key] = req.attributes[key];
    });
    sql += Object.keys(dataKVMap).map(dbu.fieldName).join(', ') + ') values (';
    sql += Object.keys(dataKVMap).map(function() { return '?'; }).join(', ') + ')';
    result.push({
        sql : sql,
        params: Object.keys(dataKVMap).map(function(key) { return dataKVMap[key]; })
    });

    return result;
};

function buildCondition(pred, schema, includePreparedForDelete) {
    var params = [];
    var conjunctions = [];
    Object.keys(pred).forEach(function(predKey) {
        var predObj = pred[predKey];
        var sql = dbu.fieldName(predKey);
        if (predObj === null || predObj.constructor !== Object) {
            // Default to equality
            sql += ' = ?';
            params.push(schema.converters[schema.attributes[predKey]].write(predObj));
        } else {
            var predKeys = Object.keys(predObj);
            if (predKeys.length === 1) {
                var predOp = predKeys[0];
                var predArg = predObj[predOp];
                switch (predOp.toLowerCase()) {
                    case 'eq':
                        sql += ' = ?';
                        params.push(schema.converters[schema.attributes[predKey]].write(predArg));
                        break;
                    case 'lt':
                        sql += ' < ?';
                        params.push(schema.converters[schema.attributes[predKey]].write(predArg));
                        break;
                    case 'gt':
                        sql += ' > ?';
                        params.push(schema.converters[schema.attributes[predKey]].write(predArg));
                        break;
                    case 'le':
                        sql += ' <= ?';
                        params.push(schema.converters[schema.attributes[predKey]].write(predArg));
                        break;
                    case 'ge':
                        sql += ' >= ?';
                        params.push(schema.converters[schema.attributes[predKey]].write(predArg));
                        break;
                    case 'between':
                        sql += ' >= ?' + ' AND ';
                        params.push(schema.converters[schema.attributes[predKey]].write(predArg[0]));
                        sql += dbu.fieldName(predKey) + ' <= ?';
                        params.push(schema.converters[schema.attributes[predKey]].write(predArg[1]));
                        break;
                    default:
                        throw new Error('Operator ' + predOp + ' not supported!');
                }
            }
        }
        conjunctions.push(sql);
    });
    // Also include check that _exist_until not expired
    if (includePreparedForDelete) {
        conjunctions.push('(' + dbu.fieldName('_exist_until') + ' > ? OR '
            + dbu.fieldName('_exist_until') + ' is null )');
        params.push(new Date().getTime());
    } else {
        conjunctions.push(dbu.fieldName('_exist_until') + ' is null');
    }
    return {
        query: conjunctions.join(' AND '),
        params: params
    };
}

dbu.buildStaticsTableSql = function(schema, tableName) {
    var staticFields = [];
    var hashKeys = [];
    var hasRangeKey = false;
    schema.index.forEach(function(index) {
        if (index.type === 'static') {
            staticFields.push(index.attribute);
        } else if (index.type === 'hash') {
            hashKeys.push(index.attribute);
        } else if (index.type === 'range') {
            hasRangeKey = true;
        }
    });
    if (staticFields.length === 0) {
        return;
    }
    var sql = 'create table if not exists ';
    sql += '[' + tableName + '_static] (';
    sql += hashKeys.concat(staticFields).map(function(key) {
        return dbu.fieldName(key) + ' ' + schema.converters[schema.attributes[key]].type;
    }).join(', ');
    sql += ', primary key (' + hashKeys.map(dbu.fieldName).join(', ') + '), ';
    sql += 'foreign key (' + hashKeys.map(dbu.fieldName).join(', ') + ') ';
    sql += 'references [' + tableName + '_data] )';
    return sql;
};

dbu.buildTableSql = function(schema, tableName) {
    var indexKeys = getAllKeysOfTypes(schema, ['hash', 'range']);
    var sql = 'create table if not exists ' + '[' + tableName + '_data] (';
    sql += Object.keys(schema.attributes)
    .filter(function(attr) {
        return !schema.iKeyMap[attr] || schema.iKeyMap[attr].type !== 'static';
    })
    .map(function(attr) {
        return dbu.fieldName(attr) + ' ' + schema.converters[schema.attributes[attr]].type;
    })
    .join(', ');
    sql += ', primary key (' + indexKeys.map(dbu.fieldName).join(', ') + ') )';
    return sql;
};

dbu.buildSecondaryIndexTableSql = function(schema, tableName) {
    var result = [];
    var secondaryIndexNames = Object.keys(schema.secondaryIndexes);
    var tableSql;
    if (secondaryIndexNames.length === 0) {
        return result;
    }

    tableSql = 'create table if not exists ' + '[' + tableName + '_secondaryIndex] (';
    schema.allSecondaryIndexKeys.forEach(function(attr) {
        tableSql += dbu.fieldName(attr) + ' ' + schema.converters[schema.attributes[attr]].type + ', ';
    });
    tableSql += 'primary key (' + schema.secondaryIndexPrimaryKeys.map(dbu.fieldName).join(', ') + ') )';
    result.push(tableSql);

    // Next, create SQLite indexes over secondary index key columns for faster lookup
    Object.keys(schema.secondaryIndexes).forEach(function(indexName) {
        var indexSchema = schema.secondaryIndexes[indexName];
        var indexSql = 'create index if not exists [' + tableName + '_index_' + indexName + ']';
        indexSql += ' on [' + tableName + '_secondaryIndex] (';
        indexSql += getAllKeysOfTypes(indexSchema, ['hash', 'range']).map(dbu.fieldName).join(', ') + ')';
        result.push(indexSql);
    });
    return result;
};

dbu.buildDeleteExpiredQuery = function(schema, tableName) {
    return {
        sql: 'delete from [' + tableName + '_data] where ' + dbu.fieldName('_exist_until') + ' < ?',
        params: [new Date().getTime()]
    };
};

dbu.buildDeleteQuery = function(tableName, keys) {
    var sql = 'delete from [' + tableName + '_data] where ';
    var params  = [];
    sql += Object.keys(keys).map(function(key) {
        params.push(keys[key]);
        return ' ' + dbu.fieldName(key) + ' = ? ';
    })
    .join('and');
    return {
        sql: sql,
        params: params
    };
};

module.exports = dbu;
