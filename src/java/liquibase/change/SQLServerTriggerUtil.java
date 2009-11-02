/*
 * Created on Oct 22, 2009 by Bradley Wagner
 * 
 * Copyright(c) 2000-2009 Hannon Hill Corporation.  All rights reserved.
 */
package liquibase.change;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import liquibase.database.Database;
import liquibase.database.MSSQLDatabase;
import liquibase.database.sql.SqlStatement;
import liquibase.exception.StatementNotSupportedOnDatabaseException;

/**
 * Utility methods for accessing and manipulating SQL Server table triggers
 * 
 * @author  Bradley Wagner
 * @version $Id$
 * @since   6.4
 */
public final class SQLServerTriggerUtil
{
    /**
     * Private constructor, static access only
     */
    private SQLServerTriggerUtil()
    {

    }

    /**
     * Fetches all triggers for Cascade tables beginning
     * with "cxml_" and creates a Set of ordered triples containing:
     * - trigger name
     * - table name  
     * - trigger query
     * 
     * @param conn
     * @return Returns a Set of Pairs of corresponding tables names and trigger queries
     * @throws Exception
     */
    public static Set<String[]> getRelevantTriggers(Connection conn) throws Exception
    {
        Set<String[]> triggers = new HashSet<String[]>();
        Statement stmt = conn.createStatement();

        // get all trigger, table names
        ResultSet rs = stmt.executeQuery("select name as 'Trigger', object_name(parent_obj) as 'Table' from sysobjects where xtype = 'TR'");
        while (rs.next())
        {
            String triggerName = rs.getString("Trigger");
            String tableName = rs.getString("Table");

            if (tableName == null || !tableName.toLowerCase().startsWith("cxml_"))
            {
                continue;
            }

            String[] trigger = new String[3];
            trigger[0] = triggerName;
            trigger[1] = tableName;
            triggers.add(trigger);
        }

        // fetch trigger queries for trigger names
        for (String[] trigger : triggers)
        {
            trigger[2] = getTriggerQuery(stmt, trigger[0]);
        }

        return triggers;
    }

    /**
     * Fetches the trigger for the input table name
     * and returns an ordered triple containing:
     * - trigger name
     * - table name  
     * - trigger query
     * or <code>null</code> if no trigger exists
     * 
     * @param tableName
     * @param conn
     * @throws SQLException
     */
    public static String[] getDeleteTriggerForTable(String tableName, Connection conn) throws SQLException
    {

        Statement stmt = conn.createStatement();

        // get all trigger, table names
        ResultSet rs = stmt
                .executeQuery("select name as 'Trigger', object_name(parent_obj) as 'Table' from sysobjects where xtype = 'TR' AND Table='"
                        + tableName + "'");
        if (!rs.next())
        {
            return null;
        }

        String triggerName = rs.getString("Trigger");

        String[] trigger = new String[3];
        trigger[0] = triggerName;
        trigger[1] = tableName;
        trigger[2] = getTriggerQuery(stmt, triggerName);
        return trigger;
    }

    private static String getTriggerQuery(Statement stmt, String triggerName) throws SQLException
    {
        ResultSet rs = null;
        try
        {
            rs = stmt.executeQuery("sp_helptext " + triggerName);
            int results = 0;
            StringBuilder triggerQuery = new StringBuilder();
            while (rs.next())
            {
                triggerQuery.append(rs.getString(1));
                results++;
            }
            if (results == 0)
            {
                return null;
            }

            return triggerQuery.toString();
        }
        finally
        {
            if (rs != null)
                rs.close();
        }
    }

    /**
     * Finds UPDATE sub-queries of the form:
     * UPDATE {table} SET {column} = NULL
     * (where {table} is "cxml_*")
     * and assembles them as a Set of ordered pairs:
     * - table name
     * - column name
     * 
     * @param triggerName
     * @param tableName
     * @param triggerQuery
     * @return
     */
    public static Set<String[]> findCascadeUpdateStatements(String triggerName, String tableName, String triggerQuery)
    {
        Set<String[]> updateStmts = new HashSet<String[]>();
        String regex = "UPDATE (cxml_\\w+) SET (\\w+) = NULL";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE & Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(triggerQuery);
        while (matcher.find())
        {
            String[] tableAndCol = new String[2];
            tableAndCol[0] = matcher.group(1);
            tableAndCol[1] = matcher.group(2);
            updateStmts.add(tableAndCol);
        }

        return updateStmts;
    }

    /**
     * Create a SqlStatement to drop the trigger with the input name
     * 
     * @param triggerName
     * @return
     */
    public static SqlStatement generateDropTrigger(String triggerName)
    {
        final String query = "DROP TRIGGER " + triggerName;
        return new SqlStatement()
        {
            public boolean supportsDatabase(Database database)
            {
                return database instanceof MSSQLDatabase;
            }

            public String getSqlStatement(Database database) throws StatementNotSupportedOnDatabaseException
            {
                return query;
            }

            public String getEndDelimiter(Database database)
            {
                return ";";
            }
        };
    }

    /**
     * Generates a SqlStatement for creating a new trigger on the table
     * with the input name that nulls the columns that reference the
     * input PK column. The referencing columns are represented as a
     * Set of pairs of table and column names.
     * 
     * A referencing pair is an ordered pair:
     * - table name
     * - column name
     * 
     * @param tableName Table to create the trigger for
     * @param pkColName PK column in the referenced Table
     * @param referencingPairs Pairs of Table and Column names that reference the table
     * @return Returns a SqlStatement
     */
    public static SqlStatement generateCreateTriggerStmt(String tableName, String pkColName, Set<String[]> referencingPairs)
    {
        String triggerName = createTriggerName(tableName);
        final StringBuilder sb = new StringBuilder();
        sb.append("CREATE TRIGGER ");
        sb.append(triggerName);
        sb.append(" ON ");
        sb.append(tableName);
        sb.append(" INSTEAD OF DELETE AS ");
        for (String[] referencingPair : referencingPairs)
        {
            String fkTableName = referencingPair[0];
            String fkColumnName = referencingPair[1];
            sb.append("UPDATE ");
            sb.append(fkTableName);
            sb.append(" SET ");
            sb.append(fkColumnName);
            sb.append(" = NULL FROM ");
            sb.append(fkTableName);
            sb.append(" AS fktable JOIN deleted AS D on fktable.");
            sb.append(fkColumnName);
            sb.append(" = D.");
            sb.append(pkColName);
            sb.append("\n");
        }
        sb.append("DELETE toDelete FROM ");
        sb.append(tableName);
        sb.append(" toDelete INNER JOIN deleted ON toDelete.");
        sb.append(pkColName);
        sb.append(" = deleted.");
        sb.append(pkColName);

        return new SqlStatement()
        {
            public boolean supportsDatabase(Database database)
            {
                return database instanceof MSSQLDatabase;
            }

            public String getSqlStatement(Database database) throws StatementNotSupportedOnDatabaseException
            {
                return sb.toString();
            }

            public String getEndDelimiter(Database database)
            {
                return ";";
            }
        };
    }

    /**
     * Creates the trigger name from the specified table
     * 
     * @param tableName
     * @return Returns the name of the Trigger
     */
    public static String createTriggerName(String tableName)
    {
        return "TRG_" + tableName.toUpperCase() + "_DELETE";
    }
}
