package liquibase.database;

import liquibase.database.sql.RawSqlStatement;
import liquibase.database.sql.SqlStatement;
import liquibase.database.structure.DatabaseSnapshot;
import liquibase.database.structure.DerbyDatabaseSnapshot;
import liquibase.exception.JDBCException;
import liquibase.diff.DiffStatusListener;

import java.sql.Connection;
import java.sql.Types;
import java.text.ParseException;
import java.util.Set;

public class DerbyDatabase extends AbstractDatabase {
    private static final DataType BOOLEAN_TYPE = new DataType("SMALLINT", false);
    private static final DataType CURRENCY_TYPE = new DataType("DECIMAL", true);
    private static final DataType UUID_TYPE = new DataType("CHAR(36)", false);
    private static final DataType CLOB_TYPE = new DataType("CLOB", true);
    private static final DataType BLOB_TYPE = new DataType("BLOB", true);
    private static final DataType TIMESTAMP_TYPE = new DataType("TIMESTAMP", false);

    public boolean isCorrectDatabaseImplementation(Connection conn) throws JDBCException {
        return "Apache Derby".equalsIgnoreCase(getDatabaseProductName(conn));
    }

    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:derby")) {
            return "org.apache.derby.jdbc.EmbeddedDriver";
        }
        return null;
    }

    public String getProductName() {
        return "Apache Derby";
    }

    public String getTypeName() {
        return "derby";
    }

    protected String getDefaultDatabaseSchemaName() throws JDBCException {//NOPMD
        return super.getDefaultDatabaseSchemaName().toUpperCase();
    }

    public boolean supportsSequences() {
        return false;
    }

    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    public DataType getBooleanType() {
        return BOOLEAN_TYPE;
    }

    public DataType getCurrencyType() {
        return CURRENCY_TYPE;
    }

    public DataType getUUIDType() {
        return UUID_TYPE;
    }

    public DataType getClobType() {
        return CLOB_TYPE;
    }

    public DataType getBlobType() {
        return BLOB_TYPE;
    }

    public DataType getDateTimeType() {
        return TIMESTAMP_TYPE;
    }

    public String getCurrentDateTimeFunction() {
        return "CURRENT_TIMESTAMP";
    }

    public String getFalseBooleanValue() {
        return "0";
    }

    public String getTrueBooleanValue() {
        return "1";
    }

    public String getAutoIncrementClause() {
        return "GENERATED BY DEFAULT AS IDENTITY";
    }


    public String getDateLiteral(String isoDate) {
        if (isDateOnly(isoDate)) {
            return "DATE(" + super.getDateLiteral(isoDate) + ")";
        } else if (isTimeOnly(isoDate)) {
            return "TIME(" + super.getDateLiteral(isoDate) + ")";
        } else {
            String dateString = super.getDateLiteral(isoDate);
            int decimalDigits = dateString.length() - dateString.indexOf('.') - 2;
            String padding = "";
            for (int i=6; i> decimalDigits; i--) {
                padding += "0";
            }
            return "TIMESTAMP(" + dateString.replaceFirst("'$", padding+"'") + ")";
        }
    }

    public boolean supportsTablespaces() {
        return false;
    }

    public SqlStatement getViewDefinitionSql(String schemaName, String name) throws JDBCException {
        return new RawSqlStatement("select V.VIEWDEFINITION from SYS.SYSVIEWS V, SYS.SYSTABLES T, SYS.SYSSCHEMAS S WHERE  V.TABLEID=T.TABLEID AND T.SCHEMAID=S.SCHEMAID AND T.TABLETYPE='V' AND T.TABLENAME='" + name + "' AND S.SCHEMANAME='"+convertRequestedSchemaToSchema(schemaName)+"'");
    }


    public String getViewDefinition(String schemaName, String name) throws JDBCException {
        return super.getViewDefinition(schemaName, name).replaceFirst("CREATE VIEW \\w+ AS ", "");
    }

    public void setConnection(Connection conn) {
        super.setConnection(new DerbyConnectionDelegate(conn));
    }

    public Object convertDatabaseValueToJavaObject(Object defaultValue, int dataType, int columnSize, int decimalDigits) throws ParseException {
        if (defaultValue != null && defaultValue instanceof String) {
            if (dataType == Types.TIMESTAMP) {
                defaultValue = ((String) defaultValue).replaceFirst("^TIMESTAMP\\('", "").replaceFirst("'\\)", "");
            } else if (dataType == Types.DATE) {
                defaultValue = ((String) defaultValue).replaceFirst("^DATE\\('", "").replaceFirst("'\\)", "");
            } else if (dataType == Types.TIME) {
                defaultValue = ((String) defaultValue).replaceFirst("^TIME\\('", "").replaceFirst("'\\)", "");
            }
        }
        return super.convertDatabaseValueToJavaObject(defaultValue, dataType, columnSize, decimalDigits);
    }

    public DatabaseSnapshot createDatabaseSnapshot(String schema, Set<DiffStatusListener> statusListeners) throws JDBCException {
        return new DerbyDatabaseSnapshot(this, statusListeners, schema);
    }
}
