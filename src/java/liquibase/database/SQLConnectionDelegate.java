package liquibase.database;

import java.sql.*;
import java.util.Map;

/**
 * A ConnectionWrapper implementation which delegates completely to an
 * underlying java.sql.connection.
 * 
 * @author <a href="mailto:csuml@yahoo.co.uk">Paul Keeble</a>
 */
public class SQLConnectionDelegate implements DatabaseConnection {
    java.sql.Connection con;

    public SQLConnectionDelegate(java.sql.Connection connection) {
        this.con = connection;
    }
    
    /**
     * Returns the connection that this Delegate is using.
     * 
     * @return The connection originally passed in the constructor
     */
    public Connection getWrappedConnection() {
        return con;
    }

    public void clearWarnings() throws SQLException {
        con.clearWarnings();
    }

    public void close() throws SQLException {
        rollback();
        con.close();
    }

    public void commit() throws SQLException {
        if (!con.getAutoCommit()) {
            con.commit();
        }
    }

    public Statement createStatement() throws SQLException {
        return con.createStatement();
    }

    public Statement createStatement(int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return con.createStatement(resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return con.createStatement(resultSetType, resultSetConcurrency);
    }

    public boolean getAutoCommit() throws SQLException {
        return con.getAutoCommit();
    }

    public String getCatalog() throws SQLException {
        return con.getCatalog();
    }

    public int getHoldability() throws SQLException {
        return con.getHoldability();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return con.getMetaData();
    }

    public int getTransactionIsolation() throws SQLException {
        return con.getTransactionIsolation();
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return con.getTypeMap();
    }

    public SQLWarning getWarnings() throws SQLException {
        return con.getWarnings();
    }

    public boolean isClosed() throws SQLException {
        return con.isClosed();
    }

    public boolean isReadOnly() throws SQLException {
        return con.isReadOnly();
    }

    public String nativeSQL(String sql) throws SQLException {
        return con.nativeSQL(sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return con.prepareCall(sql, resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        return con.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return con.prepareCall(sql);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return con.prepareStatement(sql, resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        return con.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return con.prepareStatement(sql, autoGeneratedKeys);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        return con.prepareStatement(sql, columnIndexes);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        return con.prepareStatement(sql, columnNames);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return con.prepareStatement(sql);
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        con.releaseSavepoint(savepoint);
    }

    public void rollback() throws SQLException {
    	if (!con.getAutoCommit()) {
            con.rollback();
        }
    }

    public void rollback(Savepoint savepoint) throws SQLException {
    	if (!con.getAutoCommit()) {
    		con.rollback(savepoint);
    	}
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        // Fix for Sybase jConnect JDBC driver bug.
        // Which throws SQLException(JZ016: The AutoCommit option is already set to false)
        // if con.setAutoCommit(false) called twise or more times with value 'false'.
//        if (con.getAutoCommit() != autoCommit) {
        	con.setAutoCommit(autoCommit);
//        }
    }

    public void setCatalog(String catalog) throws SQLException {
        con.setCatalog(catalog);
    }

    public void setHoldability(int holdability) throws SQLException {
        con.setHoldability(holdability);
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        con.setReadOnly(readOnly);
    }

    public Savepoint setSavepoint() throws SQLException {
        return con.setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return con.setSavepoint(name);
    }

    public void setTransactionIsolation(int level) throws SQLException {
        con.setTransactionIsolation(level);
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        con.setTypeMap(map);
    }

    public Connection getUnderlyingConnection() {
        return con;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof SQLConnectionDelegate)) {
            return false;
        }
        
        SQLConnectionDelegate otherObj = (SQLConnectionDelegate) obj;
        try {
            return this.getUnderlyingConnection().getMetaData().getURL().equals(otherObj.getUnderlyingConnection().getMetaData().getURL())
                    && this.getUnderlyingConnection().getMetaData().getUserName().equals(otherObj.getUnderlyingConnection().getMetaData().getUserName());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int hashCode() {
        return this.getUnderlyingConnection().hashCode();
    }
}
