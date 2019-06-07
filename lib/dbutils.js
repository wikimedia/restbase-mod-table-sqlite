'use strict';

const extend = require('extend');
const crypto = require('crypto');
const stringify = require('fast-json-stable-stringify');

const dbu = {};
const uuidV1Test = (uuid) => /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(uuid);

// Conversion factories. We create a function for each type so that it can be
// compiled monomorphically.
function toString() {
    return (val) => {
        if (val) {
            return val.toString();
        }
        return null;
    };
}

dbu.conversions = {
    json: {
        write: JSON.stringify,
        read: JSON.parse,
        type: 'blob'
    },
    string: {
        read(value) {
            if (value !== null &&
                    value !== undefined &&
                    typeof value !== 'string') {
                return value.toString();
            }
            return value;
        },
        type: 'text'
    },
    blob: {
        write(blob) {
            if (!blob) {
                return null;
            }
            if (blob instanceof Buffer) {
                return blob;
            } else {
                return new Buffer(blob);
            }
        },
        read(val) {
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
        read(value) {
            return value !== 0;
        },
        write(value) {
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
        read(value) {
            if (value) {
                value = `${value.substr(7, 8)}-${value.substr(3, 4)}` +
                    `-1${value.substr(0, 3)}-${value.substr(15)}`;
            }
            return value;
        },
        write(value) {
            if (value) {
                if (!uuidV1Test(value)) {
                    throw new Error(`Illegal uuid value ${value}`);
                }
                value = value.substr(15, 3) +
                value.substr(9, 4) +
                value.substr(0, 8) +
                value.substr(19);
            }
            return value;
        },
        type: 'text'
    },
    uuid: {
        read: toString()
    },
    long: {
        type: 'string',
        write: toString()
    }
};

function generateSetConverter(convObj) {
    return {
        write(valArray) {
            if (!Array.isArray(valArray) || valArray.length === 0) {
                // We treat the Empty set as being equivalent to null
                return null;
            } else {
                return JSON.stringify(valArray.map(convObj.write));
            }
        },
        read(valJson) {
            if (!valJson) {
                return null;
            }
            let valArray = JSON.parse(valJson);
            if (valArray) {
                valArray = valArray.map(convObj.read);
                const valSet = new Set(valArray);
                valArray = [];
                valSet.forEach((val) => {
                    valArray.push(val);
                });
                return valArray.sort((val1, val2) => {
                    if (typeof val1 === 'number' ||
                    (typeof val1 === 'object' && val1.constructor === Number)) {
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
    Object.keys(schema.attributes).forEach((key) => {
        const setType = /^set<(\w+)>$/.exec(schema.attributes[key]);
        const objType = setType ? setType[1] : schema.attributes[key];
        const objConverter = dbu.conversions[objType] || {};
        if (!objConverter.write) {
            objConverter.write = (val) => val;
        }
        if (!objConverter.read) {
            objConverter.read = (val) => val;
        }
        if (!objConverter.type) {
            objConverter.type = objType;
        }
        if (setType) {
            schema.converters[schema.attributes[key]] = generateSetConverter(objConverter);
        } else {
            schema.converters[schema.attributes[key]] = objConverter;
        }
    });
    return schema;
}

/*
 * Error instance wrapping HTTP error responses
 *
 * Has the same properties as the original response.
 */
class HTTPError extends Error {
    constructor(responce) {
        super(JSON.stringify(responce));
        this.name = 'HTTPError';
        Object.assign(this, responce);
    }
}
dbu.HTTPError = HTTPError;

function getAllKeysOfTypes(schema, types) {
    return Object.keys(schema.iKeyMap)
    .filter((key) => schema.iKeyMap[key] &&
        types.indexOf(schema.iKeyMap[key].type) >= 0);
}

dbu.hashKey = function hashKey(key) {
    return new crypto.Hash('sha1')
    .update(key)
    .digest()
    .toString('base64')
    // Replace [+/] from base64 with _ (illegal in Cassandra)
    .replace(/[+/]/g, '_')
    // Remove base64 padding, has no entropy
    .replace(/=+$/, '');
};

dbu.fieldName = (name) => {
    if (/^[a-zA-Z0-9_]+$/.test(name)) {
        return `"${name}"`;
    } else {
        return `"${name.replace(/"/g, '""')}"`;
    }
};

dbu.makeSchemaInfo = function makeSchemaInfo(schema, ignoreDomain) {
    const psi = extend(true, {}, schema);
    if (!ignoreDomain) {
        psi.attributes._domain = 'string';
        psi.index.unshift({
            type: 'hash',
            attribute: '_domain'
        });
    }
    psi.attributes._exist_until = 'timestamp';

    psi.versioned = false;

    // Create summary data on the primary data index
    psi.iKeys = dbu.indexKeys(psi.index);
    psi.iKeyMap = {};
    psi.index.forEach((elem) => {
        psi.iKeyMap[elem.attribute] = elem;
    });

    psi.proj = Object.keys(psi.attributes);

    psi.hash = stringify(psi);
    generateConverters(psi);
    return psi;
};

dbu.indexKeys = function indexKeys(index) {
    const res = [];
    index.forEach((elem) => {
        if (elem.type === 'hash' || elem.type === 'range') {
            res.push(elem.attribute);
        }
    });
    return res;
};

function constructOrder(query, schema) {
    const orderTerms = [];
    Object.keys(schema.iKeyMap).forEach((key) => {
        const elem = schema.iKeyMap[key];
        if (elem.type === 'range') {
            const dir = query && query.order &&
                query.order[elem.attribute] ? query.order[elem.attribute] : elem.order;
            orderTerms.push(`${dbu.fieldName(elem.attribute)} ${dir}`);
        }
    });
    if (orderTerms.length) {
        return ` order by ${orderTerms.join(',')} `;
    } else {
        return '';
    }
}

function constructProj(query, schema) {
    const projArr = query.proj || schema.proj;
    let proj;
    if (Array.isArray(projArr)) {
        proj = projArr.map(dbu.fieldName).join(',');
    } else if (projArr.constructor === String) {
        proj = dbu.fieldName(projArr);
    }
    if (query.distinct) {
        proj = ` distinct ${proj} `;
    }
    return proj;
}

function isStaticJoinNeeded(query, schema) {
    if (query && query.proj) {
        if (Array.isArray(query.proj)) {
            return query.proj.some((key) => schema.iKeyMap[key] &&
                schema.iKeyMap[key].type === 'static');
        } else if (query.proj.constructor === String) {
            return schema.iKeyMap[query.proj] && schema.iKeyMap[query.proj].type === 'static';
        } else {
            throw new Error('Unsupported query proj: ' +
                `${query.proj} of type ${query.proj.constructor}`);
        }
    } else {
        return Object.keys(schema.iKeyMap).some((key) => schema.iKeyMap[key].type === 'static');
    }
}

dbu.staticTableExist = (schema) => isStaticJoinNeeded(null, schema);

function constructLimit(query) {
    let sql = '';
    if (query.limit) {
        sql += ` limit ${query.limit}`;
    }

    if (query.next) {
        sql += ` offset ${query.next}`;
    }
    return sql;
}

function buildCondition(pred, schema, includePreparedForDelete, extractParams) {
    const params = [];
    const conjunctions = [];
    Object.keys(pred).forEach((predKey) => {
        const predObj = pred[predKey];
        if (predObj === null || predObj.constructor !== Object) {
            // Default to equality
            conjunctions.push(`${dbu.fieldName(predKey)} = ?`);
            if (extractParams) {
                params.push(schema.converters[schema.attributes[predKey]].write(predObj));
            }
        } else {
            Object.keys(predObj).forEach((predOp) => {
                const predArg = predObj[predOp];
                let sql = dbu.fieldName(predKey);

                if (extractParams) {
                    if (predOp === 'between') {
                        params.push(schema.converters[schema.attributes[predKey]]
                        .write(predArg[0]));
                        params.push(schema.converters[schema.attributes[predKey]]
                        .write(predArg[1]));
                    } else {
                        params.push(schema.converters[schema.attributes[predKey]].write(predArg));
                    }
                }
                /* eslint-disable indent */
                switch (predOp.toLowerCase()) {
                    case 'eq':
                        sql += ' = ?';
                        break;
                    case 'lt':
                        sql += ' < ?';
                        break;
                    case 'gt':
                        sql += ' > ?';
                        break;
                    case 'le':
                        sql += ' <= ?';
                        break;
                    case 'ge':
                        sql += ' >= ?';
                        break;
                    case 'between':
                        sql += ' >= ? AND ';
                        sql += `${dbu.fieldName(predKey)} <= ?`;
                        break;
                    default:
                        throw new Error(`Operator ${predOp} not supported!`);
                }
                /* eslint-enable indent */
                conjunctions.push(sql);
            });
        }
    });
    // Also include check that _exist_until not expired
    if (includePreparedForDelete) {
        conjunctions.push(`(${dbu.fieldName('_exist_until')} > ? ` +
            `OR ${dbu.fieldName('_exist_until')} is null )`);
        if (extractParams) {
            params.push(new Date().getTime());
        }
    } else {
        conjunctions.push(`${dbu.fieldName('_exist_until')} is null`);
    }
    return {
        query: conjunctions.join(' AND '),
        params
    };
}

dbu.buildGetQuery = (tableName, query, schema, includePreparedForDelete) => {
    const limit = constructLimit(query);
    let condition = '';
    let sql;

    if (query.attributes) {
        const condResult = buildCondition(query.attributes,
            schema, includePreparedForDelete, false);
        condition = ` where ${condResult.query} `;
    }

    const proj = constructProj(query, schema);
    if (isStaticJoinNeeded(query, schema)) {
        sql = `select ${proj} from [${tableName}_data] ` +
            `natural left outer join [${tableName}_static]`;
    } else {
        sql = `select ${proj} from [${tableName}_data]`;
    }
    sql += condition + constructOrder(query, schema) + limit;
    return sql;
};

dbu.buildDeleteQuery = (query, tableName, schema) => {
    const condResult = buildCondition(query.attributes, schema, false, true);
    return {
        sql: `delete from [${tableName}_data] where ${condResult.query}`,
        params: condResult.params
    };
};

function extractConditionParams(query, schema) {
    const params = [];
    const pred = query.attributes;
    Object.keys(pred).forEach((predKey) => {
        const predObj = pred[predKey];
        if (!predObj || predObj.constructor !== Object) {
            params.push(schema.converters[schema.attributes[predKey]].write(predObj));
            pred[predKey] = null;
        } else {
            const predKeys = Object.keys(predObj);
            if (predKeys[0].toLowerCase() === 'between') {
                const predArg = predObj[predKeys[0]];
                params.push(schema.converters[schema.attributes[predKey]].write(predArg[0]));
                params.push(schema.converters[schema.attributes[predKey]].write(predArg[1]));
                predArg[0] = null;
                predArg[1] = null;
            } else {
                predKeys.forEach((predOp) => {
                    params.push(schema.converters[schema.attributes[predKey]]
                        .write(predObj[predOp]));
                    predObj[predOp] = null;
                });
            }
        }
    });
    return params;
}

dbu.extractGetParams = (query, schema, includePreparedForDelete) => {
    let params;
    if (query.attributes) {
        params = extractConditionParams(query, schema);
    } else {
        params = [];
    }

    // Also include check that _exist_until not expired
    if (includePreparedForDelete) {
        params.push(new Date().getTime());
    } else {
        query.includePreparedForDelete = false;
    }
    return params;
};

function buildUpdateQuery(req, tableName, schema, dataKVMap, primaryKeyKVMap, ignore) {
    let dataParams = [];
    const condition = buildCondition(Object.assign(primaryKeyKVMap, req.if), schema, true, true);
    let sql = `${ignore ? 'update or ignore ' : 'update '}[${tableName}_data] set `;
    sql += Object.keys(dataKVMap).filter((column) => schema.iKeys.indexOf(column) < 0)
    .map((column) => {
        dataParams.push(dataKVMap[column]);
        return `${dbu.fieldName(column)}= ?`;
    }).join(',');
    sql += ` where ${condition.query}`;
    dataParams = dataParams.concat(condition.params);
    return {
        sql,
        params: dataParams
    };
}

function buildInsertQuery(tableName, dataKVMap) {
    let sql;
    const dataParams = [];
    const keyList = Object.keys(dataKVMap);
    const proj = keyList.map(dbu.fieldName).join(',');

    sql = 'insert or ignore ';
    sql += `into [${tableName}_data] (${proj}) values (`;
    sql += `${Array.apply(null, new Array(keyList.length)).map(() => '?').join(', ')})`;

    Object.keys(dataKVMap).forEach((key) => {
        dataParams.push(dataKVMap[key]);
    });

    return {
        sql,
        params: dataParams
    };
}

dbu.buildPutQuery = (req, tableName, schema, ignoreStatic) => {
    const dataKVMap = {};
    const staticKVMap = {};
    const primaryKeyKVMap = {};

    schema.iKeys.forEach((key) => {
        const value = req.attributes[key];
        if (value !== undefined &&
                value !== null &&
                (schema.iKeys.indexOf(key) >= 0)) {
            primaryKeyKVMap[key] = req.attributes[key];
        }
    });

    if (req && req.attributes) {
        Object.keys(req.attributes).forEach((key) => {
            req.attributes[key] = schema.converters[schema.attributes[key]]
                .write(req.attributes[key]);
        });
    }
    schema.iKeys.forEach((key) => {
        dataKVMap[key] = req.attributes[key];
        if (schema.iKeyMap[key].type === 'hash') {
            staticKVMap[key] = req.attributes[key];
        }
    });

    let staticNeeded = false;

    Object.keys(req.attributes).forEach((key) => {
        const val = req.attributes[key];
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

    const queries = [];

    if (req.if instanceof Object) {
        queries.push(buildUpdateQuery(req, tableName, schema, dataKVMap, primaryKeyKVMap));
    } else if (req.if === 'not exists') {
        queries.push(buildInsertQuery(tableName, dataKVMap));
    } else {
        if (Object.keys(dataKVMap).length > Object.keys(primaryKeyKVMap).length) {
            queries.push(buildUpdateQuery(req, tableName, schema,
                dataKVMap, primaryKeyKVMap, false));
        }
        queries.push(buildInsertQuery(tableName, dataKVMap));
    }

    if (staticNeeded && !ignoreStatic) {
        const staticSql = `insert or replace into [${tableName}_static] ` +
            `(${Object.keys(staticKVMap).map(dbu.fieldName).join(', ')}) ` +
            `values (${Object.keys(staticKVMap).map(() => '?').join(', ')})`;
        const staticData = Object.keys(staticKVMap).map((key) => staticKVMap[key]);
        queries.push({
            sql: staticSql,
            params: staticData
        });
    }
    return queries;
};

dbu.buildDeleteOlderQuery = (schema, table, row) => {
    const predicates = {};
    schema.iKeys.forEach((att) => {
        predicates[att] = row[att];
    });
    const condition = buildCondition(predicates, schema, true, true);
    return {
        sql: `DELETE FROM [${table}_data] WHERE ${condition.query}`,
        params: condition.params
    };
};

dbu.buildStaticsTableSql = (schema, tableName) => {
    const staticFields = [];
    const hashKeys = [];
    schema.index.forEach((index) => {
        if (index.type === 'static') {
            staticFields.push(index.attribute);
        } else if (index.type === 'hash') {
            hashKeys.push(index.attribute);
        }
    });
    if (staticFields.length === 0) {
        return;
    }
    let sql = 'create table if not exists ';
    sql += `[${tableName}_static] (`;
    sql += hashKeys.concat(staticFields).map((key) =>
        `${dbu.fieldName(key)} ${schema.converters[schema.attributes[key]].type}`
    ).join(', ');
    sql += `, primary key (${hashKeys.map(dbu.fieldName).join(', ')}), `;
    sql += `foreign key (${hashKeys.map(dbu.fieldName).join(', ')}) `;
    sql += `references [${tableName}_data] )`;
    return sql;
};

dbu.buildTableSql = (schema, tableName) => {
    const indexKeys = getAllKeysOfTypes(schema, ['hash', 'range']);
    let sql = `create table if not exists [${tableName}_data] (`;
    sql += Object.keys(schema.attributes)
    .filter((attr) => !schema.iKeyMap[attr] || schema.iKeyMap[attr].type !== 'static')
    .map((attr) => `${dbu.fieldName(attr)} ${schema.converters[schema.attributes[attr]].type}`)
    .join(', ');
    sql += `, primary key (${indexKeys.map(dbu.fieldName).join(', ')}) )`;
    return sql;
};

dbu.indexOverSecIndexName = (tableName, indexName) => `[${tableName}_index_${indexName}]`;

dbu.buildDeleteExpiredQuery = (schema, tableName) => ({
    sql: `delete from [${tableName}_data] where ${dbu.fieldName('_exist_until')} < ?`,
    params: [new Date().getTime()]
});

module.exports = dbu;
