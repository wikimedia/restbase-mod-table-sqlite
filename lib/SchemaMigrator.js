'use strict';

const dbu = require('./dbutils');
const P = require('bluebird');

/**
 * Base migration handler for unsupported schema
 */
class Unsupported {
    constructor(attr, current, proposed) {
        this.attr = attr;
        this.current = current;
        this.proposed = proposed;
    }

    validate() {
        if (this.current.hash !== this.proposed.hash) {
            throw new Error(`${this.attr} attribute migrations are unsupported`);
        }
    }
}

Unsupported.prototype.migrate = () => P.resolve();

/**
 * Table name handler
 */
class Table extends Unsupported {
    constructor(parentMigrator, current, proposed) {
        super('table', current, proposed);
    }
}

/**
 * attributes object migration handler
 */
class Attributes {
    constructor(parentMigrator, current, proposed) {
        this.client = parentMigrator.db.client;
        this.log = parentMigrator.db.log;
        this.conf = parentMigrator.db.conf;

        this.table = parentMigrator.table;
        this.consistency = parentMigrator.req.consistency;
        this.current = current;
        this.proposed = proposed;
        this.newSchema = parentMigrator.proposed;
        this.oldSchema = parentMigrator.current;

        const currSet = new Set(Object.keys(this.current));
        const propSet = new Set(Object.keys(this.proposed));

        this.addColumns = Array.from(propSet).filter((x) => !currSet.has(x));
        // TODO: null-out deleted columns
        // We can't delete the column in SQLite, but we would remove it from * projection
        this.delColumns = Array.from(currSet).filter((x) => !propSet.has(x));
    }

    _colType(col) {
        return this.newSchema.converters[this.newSchema.attributes[col]].type;
    }

    _alterTableAdd(col) {
        const colIndex = this.newSchema.index &&
           this.newSchema.index.find((index) => index.attribute === col);
        if (colIndex && colIndex.type === 'static') {
            if (!dbu.staticTableExist(this.oldSchema)) {
                return dbu.buildStaticsTableSql(this.newSchema, this.table);
            } else {
                return `${this._alterTable(`${this.table}_static`)} ` +
                    `ADD COLUMN ${dbu.fieldName(col)} ${this._colType(col)}`;
            }
        } else {
            return `${this._alterTable(`${this.table}_data`)} ` +
                `ADD COLUMN ${dbu.fieldName(col)} ${this._colType(col)}`;
        }
    }

    migrate() {
        return P.each(this.addColumns, (col) => {
            this.log('warn/schemaMigration/attributes', {
                message: `adding column${col}`,
                column: col
            });
            const sql = this._alterTableAdd(col);
            return this.client.run([{ sql }])
            .catch((e) => {
                const regex = new RegExp(`Invalid column name ${col} because ` +
                    'it conflicts with an existing column');
                if (!regex.test(e.message) &&
                        !/duplicate column name/.test(e.message)) {
                    // Ignore the error if the column already exists.
                    throw (e);
                }
            });
        });
    }
}

Attributes.prototype.validate = () => {};

Attributes.prototype._alterTable = (fullTableName) => `ALTER TABLE [${fullTableName}]`;

/**
 * Index definition migrations
 */
class Index {
    constructor(parentMigrator, current, proposed) {
        this.current = current;
        this.proposed = proposed;
        this.currentSchema = parentMigrator.current;
        this.proposedSchema = parentMigrator.proposed;

        this.addIndex = proposed.filter((x) => !this._hasSameIndex(this.current, x));
        this.delIndex = current.filter((x) => !this._hasSameIndex(this.proposed, x));

        this.alteredColumns = [];

        // If index added and the column existed previously,
        // need to remove it and add back to change index.
        // Not supported.
        this.addIndex.forEach((index) => {
            if (this.currentSchema.attributes[index.attribute]) {
                this.alteredColumns.push(index.attribute);
            }
        });

        // If index deleted the column is not deleted,
        // need to remove it and add back to change index.
        // Not supported.
        this.delIndex.forEach((index) => {
            if (this.proposedSchema.attributes[index.attribute]) {
                this.alteredColumns.push(index.attribute);
            }
        });
    }

    validate() {
        if (this.addIndex.some((index) => index.type !== 'static') ||
            this.delIndex.some((index) => index.type !== 'static')) {
            throw new Error('Only static index additions and removals supported');
        }
        if (this.alteredColumns.length > 0) {
            throw new Error('Changing index on existing column not supported');
        }
    }
}

Index.prototype._hasSameIndex = (indexes, proposedIndex) =>
    indexes.some((idx) => idx.attribute === proposedIndex.attribute &&
        idx.type === proposedIndex.type &&
        idx.order === proposedIndex.order);

Index.prototype.migrate = () => {
    // The migration is happening on individual attribute migration
};

/**
 * Version handling
 */
class Version {
    constructor(parentMigrator, current, proposed) {
        this.db = parentMigrator.db;
        this.current = current;
        this.proposed = proposed;
    }

    // versions must be monotonically increasing.
    validate() {
        if (this.current >= this.proposed) {
            throw new Error('new version must be higher than previous');
        }
    }

    migrate() {
        this.db.log('warn/schemaMigration/version', {
            current: this.current,
            proposed: this.proposed
        });
        return P.resolve();
    }
}

const migrationHandlers = {
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
 * @param  {Object} client     an instance of DB
 * @param  {Object} schemaFrom current schema info object.
 * @param  {Object} schemaTo   proposed schema info object.
 * @throws {Error}             if the proposed migration fails to validate
 */
class SchemaMigrator {
    constructor(db, req, table, current, proposed) {
        this.db = db;
        this.req = req;
        this.table = table;
        this.current = current;
        this.proposed = proposed;

        this.migrators = Object.keys(migrationHandlers)
        .map((key) => new migrationHandlers[key](this, current[key], proposed[key]));

        this._validate();
    }

    _validate() {
        this.migrators.forEach((migrator) => {
            migrator.validate();
        });
    }

    /**
     * Perform any required migration tasks.
     * @return {Promise} a promise that resolves when the migration tasks are complete
     */
    migrate() {
        return P.each(this.migrators, (migrator) => migrator.migrate());
    }
}

module.exports = SchemaMigrator;
