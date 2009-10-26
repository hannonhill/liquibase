package liquibase.database.sql;

import liquibase.database.*;
import liquibase.exception.StatementNotSupportedOnDatabaseException;

public class AddAutoIncrementStatement implements SqlStatement {

    private String schemaName;
    private String tableName;
    private String columnName;
    private String columnDataType;

    public AddAutoIncrementStatement(String schemaName, String tableName, String columnName, String columnDataType) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnDataType = columnDataType;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getColumnDataType() {
        return columnDataType;
    }

    public boolean supportsDatabase(Database database) {
        return !(database instanceof OracleDatabase
                || database instanceof MSSQLDatabase
                || database instanceof DerbyDatabase
                || database instanceof CacheDatabase
                || database instanceof FirebirdDatabase
                || database instanceof H2Database
                || database instanceof PostgresDatabase
                || database instanceof SQLiteDatabase
                || database instanceof SybaseASADatabase
                );
    }

    public String getSqlStatement(Database database) throws StatementNotSupportedOnDatabaseException {
        if (!supportsDatabase(database)) {
            throw new StatementNotSupportedOnDatabaseException(this, database);
        }

        if (database instanceof HsqlDatabase) {
            return "ALTER TABLE " + database.escapeTableName(getSchemaName(), getTableName()) + " ALTER COLUMN " + database.escapeColumnName(getSchemaName(), getTableName(), getColumnName()) + " " + getColumnDataType() + " GENERATED BY DEFAULT AS IDENTITY IDENTITY";
        } else if (database instanceof SybaseASADatabase) {
            return "ALTER TABLE " + database.escapeTableName(getSchemaName(), getTableName()) + " MODIFY " + database.escapeColumnName(getSchemaName(), getTableName(), getColumnName()) + " DEFAULT AUTOINCREMENT";
        } else if (database instanceof DB2Database) {
            return "ALTER TABLE " + database.escapeTableName(getSchemaName(), getTableName()) + " ALTER COLUMN " + database.escapeColumnName(getSchemaName(), getTableName(), getColumnName()) + " SET GENERATED ALWAYS AS IDENTITY";
        }

        return "ALTER TABLE " + database.escapeTableName(getSchemaName(), getTableName()) + " MODIFY " + database.escapeColumnName(getSchemaName(), getTableName(), getColumnName()) + " " + getColumnDataType() + " AUTO_INCREMENT";
    }

    public String getEndDelimiter(Database database) {
        return ";";
    }
}
