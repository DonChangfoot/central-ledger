// Oracle Schema Compiler
// -------
const inherits = require('inherits');
const SchemaCompiler = require('../../../schema/compiler');
const utils = require('../utils');
const Trigger = require('./trigger');

function SchemaCompiler_Oracle() {
  SchemaCompiler.apply(this, arguments);
}
inherits(SchemaCompiler_Oracle, SchemaCompiler);

// Rename a table on the schema.
SchemaCompiler_Oracle.prototype.renameTable = function(tableName, to) {
  const renameTable = Trigger.renameTableAndAutoIncrementTrigger(
    this.client.logger,
    tableName,
    to
  );
  this.pushQuery(renameTable);
};

// Check whether a table exists on the query.
SchemaCompiler_Oracle.prototype.hasTable = function(tableName) {
  this.pushQuery({
    sql:
      'select TABLE_NAME from USER_TABLES where TABLE_NAME = ' +
      this.formatter.parameter(tableName),
    output(resp) {
      return resp.length > 0;
    },
  });
};

// Check whether a column exists on the schema.
SchemaCompiler_Oracle.prototype.hasColumn = function(tableName, column) {
  const sql =
    `select COLUMN_NAME from USER_TAB_COLUMNS ` +
    `where TABLE_NAME = ${this.formatter.parameter(tableName)} ` +
    `and COLUMN_NAME = ${this.formatter.parameter(column)}`;
  this.pushQuery({ sql, output: (resp) => resp.length > 0 });
};

SchemaCompiler_Oracle.prototype.dropSequenceIfExists = function(sequenceName) {
  this.pushQuery(
    utils.wrapSqlWithCatch(
      `drop sequence ${this.formatter.wrap(sequenceName)}`,
      -2289
    )
  );
};

SchemaCompiler_Oracle.prototype._dropRelatedSequenceIfExists = function(
  tableName
) {
  // removing the sequence that was possibly generated by increments() column
  const sequenceName = utils.generateCombinedName(
    this.client.logger,
    'seq',
    tableName
  );
  this.dropSequenceIfExists(sequenceName);
};

SchemaCompiler_Oracle.prototype.dropTable = function(tableName) {
  this.pushQuery(`drop table ${this.formatter.wrap(tableName)}`);

  // removing the sequence that was possibly generated by increments() column
  this._dropRelatedSequenceIfExists(tableName);
};

SchemaCompiler_Oracle.prototype.dropTableIfExists = function(tableName) {
  this.pushQuery(
    utils.wrapSqlWithCatch(`drop table ${this.formatter.wrap(tableName)}`, -942)
  );

  // removing the sequence that was possibly generated by increments() column
  this._dropRelatedSequenceIfExists(tableName);
};

module.exports = SchemaCompiler_Oracle;
