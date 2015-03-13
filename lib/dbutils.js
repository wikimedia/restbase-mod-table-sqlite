"use strict";
//var uuid = require('node-uuid');
var crypto = require('crypto');
var extend = require('extend');
var util = require('util');
var uuid = require('node-uuid');
var extend = require('extend');

/*
 * Various static database utility methods
 *
 * Three main sections:
 * 1) low-level helpers
 * 2) schema handling
 * 3) CQL query building
 */

var dbu = {};

/*
 * # Section 1: low-level helpers
 */


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
util.inherits(HTTPError, Error);
dbu.HTTPError = HTTPError;


// Simple array to set conversion
dbu.arrayToSet = function arrayToSet(arr) {
    var o = {};
    arr.forEach(function(key) {
        o[key] = true;
    });
    return o;
};

dbu.cassID = function cassID (name) {
    if (/^[a-zA-Z0-9_]+$/.test(name)) {
        return '"' + name + '"';
    } else {
        return '"' + name.replace(/"/g, '""') + '"';
    }
};

dbu.idxTable = function idxTable (name, bucket) {
    var idx = 'idx_' + name;
    if (bucket) {
        return idx + '_' + bucket;
    } else {
        return idx + '_ever';
    }
};

dbu.tidFromDate = function tidFromDate(date) {
    // Create a new, deterministic timestamp
    return uuid.v1({
        node: [0x01, 0x23, 0x45, 0x67, 0x89, 0xab],
        clockseq: 0x1234,
        msecs: date.getTime(),
        nsecs: 0
    });
};

// Hash a key into a valid Cassandra key name
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


/**
 * Derive a valid keyspace name from a random bucket name. Try to use valid
 * chars from the requested name as far as possible, but fall back to a sha1
 * if not possible. Also respect Cassandra's limit of 48 or fewer alphanum
 * chars & first char being an alpha char.
 *
 * @param {string} reverseDomain, a domain in reverse dot notation
 * @param {string} key, the bucket name to derive the key of
 * @return {string} Valid Cassandra keyspace key
 */
dbu.keyspaceName = function keyspaceName (reverseDomain, key) {
    var prefix = dbu.makeValidKey(reverseDomain, Math.max(26, 48 - key.length - 3));
    return prefix
        // 6 chars _hash_ to prevent conflicts between domains & table names
        + '_T_' + dbu.makeValidKey(key, 48 - prefix.length - 3);
};

/*
 * # Section 2: Schema validation, normalization and -handling
 */

dbu.validateIndexSchema = function validateIndexSchema(schema, index) {

    if (!Array.isArray(index) || !index.length) {
        //console.log(req);
        throw new Error("Invalid index " + JSON.stringify(index));
    }

    var haveHash = false;

    index.forEach(function(elem) {
        if (!schema.attributes[elem.attribute]) {
            throw new Error('Index element ' + JSON.stringify(elem)
                    + ' is not in attributes!');
        }

        switch (elem.type) {
        case 'hash':
            haveHash = true;
            break;
        case 'range':
            if (elem.order !== 'asc' && elem.order !== 'desc') {
                // Default to ascending sorting.
                //
                // Normally you should specify the sorting explicitly. In
                // particular, you probably always want to use descending
                // order for time series data (timeuuid) where access to the
                // most recent data is most common.
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


dbu.validateAndNormalizeSchema = function validateAndNormalizeSchema(schema) {
    if (!schema.version) {
        schema.version = 1;
    } else if (schema.version !== 1) {
        throw new Error("Schema version 1 expected, got " + schema.version);
    }

    // Check options
    if (schema.options) {
        var opts = schema.options;
        for (var key in opts) {
            switch(key) {
            case 'durability':
                var val = opts[key];
                if (val !== 'low' && val !== 'standard') {
                    throw new Error ('Invalid durability level: ' + opts[key]);
                }
                break;
            default:
                throw new Error('Unknown option: ' + key);
            }
        }
    }

    // Normalize & validate indexes
    schema.index = dbu.validateIndexSchema(schema, schema.index);
    if (!schema.secondaryIndexes) {
        schema.secondaryIndexes = {};
    }
    //console.dir(schema.secondaryIndexes);
    for (var index in schema.secondaryIndexes) {
        schema.secondaryIndexes[index] = dbu.validateIndexSchema(schema, schema.secondaryIndexes[index]);
    }

    // XXX: validate attributes
    return schema;
};

/**
 * Generates read/write conversion functions for set-typed attributes
 *
 * @param {Object} convObj the conversion object to use for individual values (from dbu.conversions)
 * @returns {Object} an object with 'read' and 'write' attributes
 */
function generateSetConvertor (convObj, blobType) {
    if (!convObj) {
        return {
            write: function(arr) {
                // Default to-null conversion for empty sets
                if (!Array.isArray(arr) || arr.length === 0) {
                    return null;
                } else {
                    return arr;
                }
            },
            // XXX: Should we convert null to the empty array here?
            read: null
        };
    }
    var res = {
        write: null,
        read: null
    };
    if (blobType) {
        res.write = convObj.write;
    } else if (convObj.write) {
        res.write = function (valArray) {
            if (!Array.isArray(valArray) || valArray.length === 0) {
                // Empty set is equivalent to null in Cassandra
                return null;
            } else {
                return valArray.map(convObj.write);
            }
        };
    }
    if (convObj.read) {
        res.read = function (valArray) {
            if (blobType) {
                return JSON.parse(valArray).map(convObj.read);
            }
            return valArray.map(convObj.read);
        };
    }
    return res;
}


/*
 * Derive additional schema info from the public schema
 */
dbu.makeSchemaInfo = function makeSchemaInfo(schema) {
    // Private schema information
    // Start with a deep clone of the schema
    var psi = extend(true, {}, schema);
    // Then add some private properties
    psi.versioned = false;

    // Check if the last index entry is a timeuuid, which we take to mean that
    // this table is versioned
    var lastElem = schema.index[schema.index.length - 1];
    var lastKey = lastElem.attribute;

    // Extract attributes that need conversion in the read or write path
    psi.conversions = {};
    for (var att in psi.attributes) {
        var type = psi.attributes[att];
        var set_type = /set<(\w+)>/.exec(type);
        if (set_type) {
            if (set_type[1] === 'blob') {
                psi.conversions[att] = generateSetConvertor(dbu.conversions['json_blob'], true);
            } else {
                psi.conversions[att] = dbu.conversions['json']
            }
        } else if (dbu.conversions[type]) {
            // this is regular type and conversion methods are defined for it
            psi.conversions[att] = dbu.conversions[type];
        }
    }


    // Add a non-index _del flag to track deletions
    // This is normally null, but will be set on an otherwise empty row to
    // mark the row as deleted.
    psi.attributes._del = 'timeuuid';

    if (lastKey && lastElem.type === 'range'
            && lastElem.order === 'desc'
            && schema.attributes[lastKey] === 'timeuuid') {
        psi.tid = lastKey;
    } else {
        // Add a hidden _tid timeuuid attribute
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


    // Now create secondary index schemas
    // Also, create a map from attribute to indexes
    var attributeIndexes = {};
    for (var si in psi.secondaryIndexes) {
        psi.secondaryIndexes[si] = dbu.makeIndexSchema(psi, si);
        var idx = psi.secondaryIndexes[si];
        idx.iKeys.forEach(function(att) {
            if (!attributeIndexes[att]) {
                attributeIndexes[att] = [si];
            } else {
                attributeIndexes[att].push(si);
            }
        });
    }
    psi.attributeIndexes = attributeIndexes;

    return psi;
};

/**
 * Converts a result row from SQLite to JS values
 *
 * @param {Row} row the result row to convert; modified in place
 * @param {Schema} schema the schema to use for conversion
 * @returns {Row} the row with converted attribute values
 */
dbu.convertRow = function convertRow (row, schema) {
    Object.keys(row).forEach(function(att) {
        if (row[att] !== null && schema.conversions[att] && schema.conversions[att].read) {
            row[att] = schema.conversions[att].read(row[att]);
        }
    });
    return row;
};

// Extract the index keys from a table schema
dbu.indexKeys = function indexKeys (index) {
    var res = [];
    index.forEach(function(elem) {
        if (elem.type === 'hash' || elem.type === 'range') {
            res.push(elem.attribute);
        }
    });
    return res;
};

dbu.makeIndexSchema = function makeIndexSchema (dataSchema, indexName) {

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
            var indexElem = { type: 'range', order: 'desc' };
            indexElem.attribute = att;
            index.push(indexElem);
            s.iKeys.push(att);
            s.iKeyMap[att] = indexElem;
        }
    });

    // Add the data table's tid as a plain attribute, if not yet included
    if (!s.attributes[dataSchema.tid]) {
        var tidKey = dataSchema.tid;
        s.attributes[tidKey] = dataSchema.attributes[tidKey];
        s.tid = tidKey;
    }

    // include the orignal schema's conversion table
    /*s.conversions = {};
    if (dataSchema.conversions) {
        for (var attr in s.attributes) {
            if (dataSchema.conversions[attr]) {
                s.conversions[attr] = dataSchema.conversions[attr];
            }
        }
    }*/

    s.attributes._del = 'timeuuid';

    return s;
};

var schemaTypeToSQLTypeMap = {
    'blob': 'blob',
    'set<blob>': 'blob', // JSON in blob
    'decimal': 'real',
    'set<decimal>': 'blob', // JSON in blob
    'double': 'real',
    'set<double>': 'blob', // JSON in blob
    'float': 'real',
    'set<float>': 'blob', // JSON in blob
    'boolean': 'boolean',
    'set<boolean>': 'blob', // JSON in blob
    'int': 'integer',
    'set<int>': 'blob', // JSON in blob
    'varint': 'integer',
    'set<varint>': 'blob', // JSON in blob
    'string': 'text',
    'set<string>': 'blob', // JSON in blob
    'timeuuid': 'blob',
    'set<timeuuid>': 'blob', // JSON in blob
    'uuid': 'blob',
    'set<uuid>': 'blob', // JSON in blob
    'timestamp': 'integer',
    'set<timestamp>': 'blob', // JSON in blob
    'json': 'blob',
    'set<json>': 'blob' // JSON in blob
};

// Map a schema type to the corresponding CQL type
dbu.schemaTypeToSQLType = function(schemaType) {
    var sqlType = schemaTypeToSQLTypeMap[schemaType];
    if (!sqlType) {
        throw new Error('Invalid schema type ' + sqlType);
    }
    return sqlType;
};

function encodeBlob (blob) {
    //blob = JSON.parse(blob);
    //console.log(new Buffer(blob.data));
    if (blob instanceof Buffer) {
        return blob;
    } else {
        return new Buffer(blob);
    }
}

function reverseTimestamp (timestamp) {
    var stamps = timestamp.split("-"), x;
    x = stamps[0];
    stamps[0] = stamps[2];
    stamps[2] = x;
    return stamps.join("-");
}

dbu.conversions = {
    json: { write: JSON.stringify, read: JSON.parse },
    json_blob: { write: JSON.stringify, read: encodeBlob},
    timeuuid: { write: reverseTimestamp, read: reverseTimestamp},
    blob: { read: encodeBlob}
};



/*
 * # Section 3: SQL query generation
 */

dbu.buildCondition = function buildCondition (pred, schema, table) {
    function convert(key, val) {
        var convObj = schema.conversions[key];
        if (convObj && convObj.write) {
            return convObj.write(val);
        } else {
            return val;
        }
    }

    var params = [];
    var keys = [];
    var conjunctions = [];
    for (var predKey in pred) {
        var cql = '';
        var predObj = pred[predKey];
        if (table) {
            cql += dbu.cassID(table) +"."+ dbu.cassID(predKey);
        } else {
            cql += dbu.cassID(predKey);
        }
        if (predObj === undefined) {
            throw new Error('Query error: attribute ' + JSON.stringify(predKey)
                    + ' is undefined');
        } else if (predObj === null || predObj.constructor !== Object) {
            // Default to equality
            cql += ' = ?';
            params.push(convert(predKey, predObj));
            keys.push(predKey);
        } else {
            var predKeys = Object.keys(predObj);
            if (predKeys.length === 1) {
                var predOp = predKeys[0];
                var predArg = predObj[predOp];
                switch (predOp.toLowerCase()) {
                case 'eq': cql += ' = ?'; params.push(convert(predKey, predArg)); keys.push(predKey); break;
                case 'lt': cql += ' < ?'; params.push(convert(predKey, predArg)); keys.push(predKey); break;
                case 'gt': cql += ' > ?'; params.push(convert(predKey, predArg)); keys.push(predKey); break;
                case 'le': cql += ' <= ?'; params.push(convert(predKey, predArg)); keys.push(predKey); break;
                case 'ge': cql += ' >= ?'; params.push(convert(predKey, predArg)); keys.push(predKey); break;
                case 'neq':
                case 'ne': cql += ' != ?'; params.push(convert(predKey, predArg)); keys.push(predKey); break;
                case 'between':
                        cql += ' >= ?' + ' AND '; params.push(convert(predKey, predArg[0])); keys.push(predKey);
                        cql += dbu.cassID(predKey) + ' <= ?'; params.push(convert(predKey, predArg[1])); keys.push(predKey);
                        break;
                default: throw new Error ('Operator ' + predOp + ' not supported!');
                }
            } else {
                throw new Error ('Invalid predicate ' + JSON.stringify(pred));
            }
        }
        conjunctions.push(cql);
    }
    return {
        query: conjunctions.join(' AND '),
        keys: keys,
        params: params
    };
};

dbu.buildPutQuery = function(req, keyspace, table, schema, action) {

    if (!schema) {
        throw new Error('Table not found!');
    }
    var sindexKVMap = {};

    // XXX: should we require non-null secondary index entries too?
    var indexKVMap = {};
    var staticIndexKVMap = {};
    schema.iKeys.forEach(function(key) {
        if (req.attributes[key] === undefined) {
            throw new Error("Index attribute " + JSON.stringify(key) + " missing in "
                    + JSON.stringify(req) + "; schema: " + JSON.stringify(schema, null, 2));
        } else {
            indexKVMap[key] = req.attributes[key];
            if (schema.iKeyMap[key].type!=='range') {
                staticIndexKVMap[key] = req.attributes[key];
                if (key!==schema.tid) {
                    sindexKVMap[key] = req.attributes[key];
                }
            } 
        }
    });

    var keys = [];
    var staticKeys = [];
    var paramKeys = [];
    var params = [];
    var staticParams = [];
    var placeholders = [];
    var conversions = schema.conversions || {};
    for (var key in req.attributes) {
        var val = req.attributes[key];
        if (val !== undefined && schema.attributes[key]) {
            if (!schema.iKeyMap[key]) {
                var conversionObj = conversions[key];
                if (conversionObj && conversionObj.write) {
                    val = conversionObj.write(val);
                }
                keys.push(key);
                params.push(val);
                paramKeys.push(key);
            }

            if ( schema.iKeyMap[key] && schema.iKeyMap[key].type==='static') {
                staticKeys.push(key);
                var convObj = schema.conversions[key];
                if (convObj && convObj.write) {
                    val = convObj.write(val);
                } 
                staticParams.push(val);
            } else {
                placeholders.push('?');
            }
        }
    }
    var condRes = dbu.buildCondition(sindexKVMap, schema);
    if (Object.keys(schema.secondaryIndexes).length>0) {
        placeholders.push('?');
        keys.push("_is_latest");
        params.push('true');
        var cql3 = 'update ' + dbu.cassID(keyspace + '_' + table);
        cql3 += ' set _is_latest = ? where ' + schema.tid + ' != ? ';
        cql3 += ' and ' + condRes.query;
        var params3 = ['false', req.attributes[schema.tid]].concat(condRes.params);
    }
    var using = '';
    var usingParams = [];
    var usingParamsKeys = [];
    //TODO- see http://stackoverflow.com/questions/14461851/how-to-have-an-automatic-timestamp-in-sqlite
    /*if (req.timestamp && !req.if) {
        using = ' USING TIMESTAMP ? ';
        usingParams.push(cass.types.Long.fromNumber(Math.round(req.timestamp * 1000)));
        usingParamsKeys.push(null);
    }*/

    // switch between insert & update
    // - always perform insert first only if req.
    //   if is undefined or req.if is 'if not exists'
    // - update when "req.if" is not "not exists" or insert return "already exists"
    //   - Need to verify that all primary key members are supplied as well,
    //     else error.

    var cql = '', condResult;

    if (req.if && req.if.constructor === String) {
        req.if = req.if.trim().split(/\s+/).join(' ').toLowerCase();
    }

    condRes = dbu.buildCondition(indexKVMap, schema);
    var cond = '', cql2, params2 = [];
    if (action === 'insert' && !req.if || req.if === 'not exists') {
        if (req.if === 'not exists') {
            cond = ' or ignore ';
        }
        var proj = schema.iKeys.concat(keys).map(dbu.cassID).join(',');
        cql = 'insert ' + cond + ' into ' + dbu.cassID(keyspace + '_' + table)
                + ' (' + proj + ') values (';
        cql += placeholders.join(',') + ')' + using;
        params = condRes.params.concat(params, usingParams);
        paramKeys = condRes.keys.concat(paramKeys, usingParamsKeys);
        if (staticKeys.length) {
            proj = [];
            schema.iKeys.forEach(function(elem) {
                if (schema.iKeyMap[elem].type === 'hash') {
                    proj.push(elem);
                    params2.push(req.attributes[elem]);
                }
            });
            placeholders = [];
            for (var i = 0; i<proj.length+staticKeys.length; i++) {
                placeholders.push('?');
            }
            proj = proj.concat(staticKeys).map(dbu.cassID).join(',');
            cql2 = 'insert into ' + dbu.cassID(keyspace + '_' + table  + '_static')
                + ' (' + proj + ') values (';
            cql2 += placeholders.join(',') + ')' + using;
            params2 = params2.concat(staticParams, usingParams);
            if (Object.keys(schema.secondaryIndexes).length>0) {
                return [{query:cql, params:params}, {query:cql2, params:params2}, {query:cql3, params: params3}];
            }
            return [{query:cql, params:params}, {query:cql2, params:params2}];
        }
    } else if ( action === 'update' || req.if ) {
        var condParams = [];
        var condParamKeys = [];
        if (req.if) {
            cond = ' AND ';
            condResult = dbu.buildCondition(req.if, schema);
            cond += condResult.query;
            condParams = condResult.params;
            condParamKeys = condResult.keys;
        }

        var updateProj = keys.map(dbu.cassID).join(' = ?,') + ' = ? ';
        cql += 'update ' + dbu.cassID(keyspace + '_' + table)
               + using + ' set ' + updateProj + ' where ';
        cql += condRes.query + cond;
        params = usingParams.concat(params, condRes.params, condParams);
        if (staticKeys.length) {
            condRes = dbu.buildCondition(staticIndexKVMap. schema);
            updateProj = staticKeys.map(dbu.cassID).join(' = ?,') + ' = ? ';
            cql2 = 'update ' + dbu.cassID(keyspace + '_' + table  + '_static')
                + using + ' set ' + updateProj + ' where ';
            cql2 += condRes.query + cond;
            params2 = usingParams.concat(staticParams, condRes.params);
            if (Object.keys(schema.secondaryIndexes).length>0) {
                return [{query:cql, params:params}, {query:cql2, params:params2}, {query:cql3, params: params3}];
            }
            return [{query:cql, params:params}, {query:cql2, params:params2}];
        }
    } else {
        throw new Error("Can't Update or Insert");
    }
    //console.log(cql, params);
    if (Object.keys(schema.secondaryIndexes).length>0) {
        return [{query:cql3, params: params3}, {query: cql, params: params}]; 
    }
    return [{query: cql, params: params}];
};

dbu.buildGetQuery = function(keyspace, req, table, schema) {
    var proj = '*';

    if (req.index) {
        if (!schema.secondaryIndexes[req.index]) {
            // console.dir(cachedSchema);
            throw new Error("Index not found: " + req.index);
        }
        var conversions = schema.conversions;
        schema = schema.secondaryIndexes[req.index];
        schema.conversions = conversions;
        var idxCond = ' indexed by ' + dbu.idxTable(req.index);
    }

    // fetch static columns
    var statics = {}, hashKeys = {};
    schema.index.forEach(function(elem) {
        if (elem.type === 'static') {
            statics[elem.attribute] = true;
        } else if (elem.type === 'hash') {
            hashKeys[elem.attribute] = true;
        }
    });
    if (req.proj) {
        if (req.proj === "*") {
            req.proj = Object.keys(schema.attributes);
        }
        if (Array.isArray(req.proj)) {
            proj = req.proj;
            if (req.index) {
                proj
            }
            if (Object.keys(statics).length) {
                //check if static columns in proj
                Object.keys(statics).forEach(function(elem){
                    if (proj.indexOf(elem) <= 0) {
                        delete statics[elem];
                    } else {
                        proj.splice(proj.indexOf(elem), 1);
                    }
                });
                statics = Object.keys(statics).map(dbu.cassID);
                var staticsCond = statics.map(function(elem){
                            return dbu.cassID(keyspace+'_'+table+'_static')+'.'+elem
                          }).join(',');
            }
            proj = proj.map(dbu.cassID);
            proj = proj.map(function(elem){return dbu.cassID(keyspace+'_'+table)+'.'+elem}).join(',');
            if (staticsCond && staticsCond !== "" ) {
                proj = proj + ', ' + staticsCond;
            }
        } else if (req.proj.constructor === String) {
            proj = dbu.cassID(req.proj);
        }
    } else if (req.order) {
        // Work around 'order by' bug in cassandra when using *
        // Trying to change the natural sort order only works with a
        // projection in 2.0.9
        if (schema) {
            proj = Object.keys(schema.attributes).map(dbu.cassID).join(',');
        }
    }

    if (req.limit && req.limit.constructor !== Number) {
        req.limit = undefined;
    }

    for ( var item in req.attributes ) {
        // req should not have non key attributes
        if (!schema.iKeyMap[item]) {
            throw new Error("Request attributes need to be key attributes");
        }
    }

    /*if (req.distinct) {
        proj = 'distinct ' + proj;
    }*/

    var cql = 'select ' + proj + ' from '
        + dbu.cassID(keyspace +'_'+ table);
    if (req.index) {
        cql += idxCond + ' ';
    }

    var condResult;
    var params = [];
    var paramKeys = [];
    // Build up the condition
    if (req.attributes) {
        if (staticsCond) {
            condResult = dbu.buildCondition(req.attributes, schema, keyspace+'_'+table);
            cql += ', ' + dbu.cassID(keyspace +'_'+ table + '_static');
            var newReq = []
            Object.keys(hashKeys).forEach(function(elem){
                newReq.push(dbu.cassID(keyspace+'_'+table)+'.'+elem+' = '+dbu.cassID(keyspace+'_'+table+'_static')+'.'+elem)
            });
            cql += ' ON ';
            cql += newReq.join(' AND ');
            cql += ' AND '  + condResult.query;
            params = condResult.params;
            if (req.index) {
                cql += ' and _is_latest = ?';
                params.push('false');
            }
        } else {
            condResult = dbu.buildCondition(req.attributes, schema);
            cql += ' where ';
            cql += condResult.query;
            params = condResult.params;
            if (req.index) {
                cql += ' and _is_latest = ?';
                params.push('true');
            }
            paramKeys = condResult.keys;
        }
    }

    if (req.order) {
        var reversed;
        // Establish whether we need to read in forward or reverse order,
        // which is what Cassandra supports. Also validate the order for
        // consistency.
        for (var att in req.order) {
            var dir = req.order[att];
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
                orderTerms.push(dbu.cassID(elem.attribute) + ' ' + dir);
            }
        });

        if (orderTerms.length) {
            cql += ' order by ' + orderTerms.join(',');
        }
    }
    if (req.limit) {
        cql += ' limit ' + req.limit;
    }
    return {query: cql, params: params};
};

module.exports = dbu;