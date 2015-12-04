"use strict";

var dbu = require('./dbutils');
var P = require('bluebird');
var util = require('util');
var stringify = require('json-stable-stringify');


/**
 * Base migration handler for unsupported schema
 */
function Unsupported(attr, current, proposed) {
    this.attr = attr;
    this.current = current;
    this.proposed = proposed;
}

Unsupported.prototype.validate = function() {
    if (this.current.hash !== this.proposed.hash) {
        throw new Error(this.attr + ' attribute migrations are unsupported');
    }
};

Unsupported.prototype.migrate = function() {
    return P.resolve();
};

/**
 * Table name handler
 */
function Table(parentMigrator, current, proposed) {
    Unsupported.call(this, 'table', current, proposed);
}

util.inherits(Table, Unsupported);

/**
 * attributes object migration handler
 */
function Attributes(parentMigrator, current, proposed) {
    this.client = parentMigrator.db.client;
    this.log = parentMigrator.db.log;
    this.conf = parentMigrator.db.conf;

    this.table = parentMigrator.table;
    this.consistency = parentMigrator.req.consistency;
    this.current = current;
    this.proposed = proposed;
    this.newSchema = parentMigrator.proposed;
    this.oldSchema = parentMigrator.current;

    var currSet = new Set(Object.keys(this.current));
    var propSet = new Set(Object.keys(this.proposed));

    this.addColumns = Array.from(propSet).filter(function(x) { return !currSet.has(x); });
    // TODO: null-out deleted columns
    // We can't delete the column in SQLite, but we would remove it from * projection
    this.delColumns = Array.from(currSet).filter(function(x) { return !propSet.has(x); });
}

Attributes.prototype.validate = function() {
    return;
};

Attributes.prototype._alterTable = function(fullTableName) {
    return 'ALTER TABLE [' + fullTableName + ']';
};

Attributes.prototype._colType = function(col) {
    return this.newSchema.converters[this.newSchema.attributes[col]].type;
};

Attributes.prototype._alterTableAdd = function(col) {
    var colIndex = this.newSchema.index
       && this.newSchema.index.find(function(index) { return index.attribute === col; });
    if (colIndex && colIndex.type === 'static') {
        if (!dbu.staticTableExist(this.oldSchema)) {
            return dbu.buildStaticsTableSql(this.newSchema, this.table);
        } else {
            return this._alterTable(this.table + '_static') +
                ' ADD COLUMN ' + dbu.fieldName(col) + ' ' + this._colType(col);
        }
    } else {
        return this._alterTable(this.table + '_data') +
            ' ADD COLUMN ' + dbu.fieldName(col) + ' ' + this._colType(col);
    }
};

Attributes.prototype.migrate = function() {
    var self = this;
    return P.each(self.addColumns, function(col) {
        self.log('warn/schemaMigration/attributes', {
            message: 'adding column' + col,
            column: col
        });
        var sql = self._alterTableAdd(col);
        return self.client.run([{ sql: sql }])
        .catch(function(e) {
            if (!new RegExp('Invalid column name ' + col
                    + ' because it conflicts with an existing column').test(e.message)
                    && !/duplicate column name/.test(e.message)) {
                // Ignore the error if the column already exists.
                throw(e);
            }
        });
    });
};

/**
 * Index definition migrations
 */
function Index(parentMigrator, current, proposed) {
    var self = this;
    self.current = current;
    self.proposed = proposed;
    self.currentSchema = parentMigrator.current;
    self.proposedSchema = parentMigrator.proposed;

    self.addIndex = proposed.filter(function(x) { return !self._hasSameIndex(self.current, x); });
    self.delIndex = current.filter(function(x) { return !self._hasSameIndex(self.proposed, x); });

    self.alteredColumns = [];

    // If index added and the column existed previously,
    // need to remove it and add back to change index.
    // Not supported.
    self.addIndex.forEach(function(index) {
        if (self.currentSchema.attributes[index.attribute]) {
            self.alteredColumns.push(index.attribute);
        }
    });

    // If index deleted the column is not deleted,
    // need to remove it and add back to change index.
    // Not supported.
    self.delIndex.forEach(function(index) {
        if (self.proposedSchema.attributes[index.attribute]) {
            self.alteredColumns.push(index.attribute);
        }
    });
}

Index.prototype.validate = function() {
    var self = this;
    if (self.addIndex.some(function(index) { return index.type !== 'static'; })
    || self.delIndex.some(function(index) { return index.type !== 'static'; })) {
        throw new Error('Only static index additions and removals supported');
    }
    if (self.alteredColumns.length > 0) {
        throw new Error('Changing index on existing column not supported');
    }
};

Index.prototype._hasSameIndex = function(indexes, proposedIndex) {
    return indexes.some(function(idx) {
        return idx.attribute === proposedIndex.attribute
        && idx.type === proposedIndex.type
        && idx.order === proposedIndex.order;
    });
};

Index.prototype.migrate = function() {
    // The migration is happening on individual attribute migration
};

/**
 * Secondary index definition migrations
 */
function SecondaryIndexes(parentMigrator, current, proposed) {
    var self = this;
    self.client = parentMigrator.db.client;
    self.log = parentMigrator.db.log;
    self.conf = parentMigrator.db.conf;

    self.table = parentMigrator.table;

    self.addedIndexes = [];
    self.deletedIndexes = [];
    self.changedIndexes = [];

    new Set(Object.keys(current).concat(Object.keys(proposed))).forEach(function(indexName) {
        if (!proposed[indexName]) {
            self.deletedIndexes.push(indexName);
        } else if (!current[indexName]) {
            self.addedIndexes.push(indexName);
        } else if (!self._isEqual(current[indexName], proposed[indexName])) {
            self.changedIndexes.push(indexName);
        }
    });
}

SecondaryIndexes.prototype.validate = function() {
    var self = this;
    if (self.addedIndexes.length > 0) {
        throw new Error('Adding secondary indices is not supported');
    }
    if (self.changedIndexes.length > 0) {
        throw new Error('Altering of secondary indices is not supported');
    }
};

SecondaryIndexes.prototype.migrate = function() {
    var self = this;
    return self.deletedIndexes.forEach(function(indexName) {
        self.log('warn/schemaMigration/secondaryIndexes', {
            message: 'deleting secondary index ' + indexName,
            index: indexName
        });
        var sql = self._removeIndexTable(indexName);
        return self.client.run([{ sql: sql }]);
    });
};

SecondaryIndexes.prototype._isEqual = function(currentIndex, proposedIndex) {
    return stringify(currentIndex) === stringify(proposedIndex);
};

SecondaryIndexes.prototype._removeIndexTable = function(indexName) {
    var self = this;
    // We can't drop columns from the joined secondary index table,
    // so just delete an index over primary keys for this secondary index
    return 'drop index if exists ' + dbu.indexOverSecIndexName(self.table, indexName);
};

/**
 * Version handling
 */
function Version(parentMigrator, current, proposed) {
    this.db = parentMigrator.db;
    this.current = current;
    this.proposed = proposed;
}

// versions must be monotonically increasing.
Version.prototype.validate = function() {
    if (this.current >= this.proposed) {
        throw new Error('new version must be higher than previous');
    }
};

Version.prototype.migrate = function() {
    this.db.log('warn/schemaMigration/version', {
        current: this.current,
        proposed: this.proposed
    });
    return P.resolve();
};

var migrationHandlers = {
    table: Table,
    attributes: Attributes,
    index: Index,
    secondaryIndexes: SecondaryIndexes,
    version: Version
};

/**
 * Schema migration.
 *
 * Accepts arguments for the current, and proposed schema as schema-info
 * objects (hint: the output of dbu#makeSchemaInfo).  Validation of the
 * proposed migration is performed, and an exception raised if necessary.
 * Note: The schemas themselves are not validated, only the migration; schema
 * input should be validated ahead of time).
 *
 * @param  {object] client; an instance of DB
 * @param  {object} schemaFrom; current schema info object.
 * @param  {object} schemaTo; proposed schema info object.
 * @throws  {Error} if the proposed migration fails to validate
 */
function SchemaMigrator(db, req, table, current, proposed) {
    this.db = db;
    this.req = req;
    this.table = table;
    this.current = current;
    this.proposed = proposed;

    var self = this;
    this.migrators = Object.keys(migrationHandlers).map(function(key) {
        return new migrationHandlers[key](self, current[key], proposed[key]);
    });

    this._validate();
}

SchemaMigrator.prototype._validate = function() {
    this.migrators.forEach(function(migrator) {
        migrator.validate();
    });
};

/**
 * Perform any required migration tasks.
 *
 * @return a promise that resolves when the migration tasks are complete
 */
SchemaMigrator.prototype.migrate = function() {
    return P.each(this.migrators, function(migrator) {
        return migrator.migrate();
    });
};

module.exports = SchemaMigrator;
