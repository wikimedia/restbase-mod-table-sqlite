"use strict";

var dbu = require('./dbutils');
var P = require('bluebird');
var util = require('util');


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
function Attributes(parentMigrator, current, proposed, newSchema) {
    this.client = parentMigrator.db.client;
    this.log = parentMigrator.db.log;
    this.conf = parentMigrator.db.conf;

    this.table = parentMigrator.table;
    this.consistency = parentMigrator.req.consistency;
    this.current = current;
    this.proposed = proposed;
    this.newSchema = newSchema;

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

Attributes.prototype._alterTable = function() {
    return 'ALTER TABLE ['+ this.table + ']';
};

Attributes.prototype._colType = function(col) {
    return this.newSchema.converters[this.newSchema.attributes[col]].type;
};

Attributes.prototype._alterTableAdd = function(col) {
    return this._alterTable()+' ADD COLUMN '+ dbu.fieldName(col) + ' ' + this._colType(col);
};

Attributes.prototype.migrate = function() {
    var self = this;
    return P.each(self.addColumns, function(col) {
        self.log('warn/schemaMigration/attributes', {
            message: 'adding column' + col,
            column: col
        });
        var sql = self._alterTableAdd(col);
        return self.client.run([
            {sql: sql}
        ])
        .catch(function(e) {
            if (!new RegExp('Invalid column name ' + col
                + ' because it conflicts with an existing column').test(e.message)) {
                throw(e);
            }
            // Else: Ignore the error if the column already exists.
        });

    });
};

/**
 * Index definition migrations
 */
function Index(parentMigrator, current, proposed) {
    Unsupported.call(this, 'index', current, proposed);
}

util.inherits(Index, Unsupported);


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
        proposed: this.proposed,
    });
    return P.resolve();
};

var migrationHandlers = {
    table: Table,
    attributes: Attributes,
    index: Index,
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
    this.table = table + '_data';
    this.current = current;
    this.proposed = proposed;

    var self = this;
    this.migrators = Object.keys(migrationHandlers).map(function(key) {
        return new migrationHandlers[key](self, current[key], proposed[key], proposed);
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
