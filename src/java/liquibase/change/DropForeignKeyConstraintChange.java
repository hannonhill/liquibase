package liquibase.change;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.MSSQLDatabase;
import liquibase.database.SQLiteDatabase;
import liquibase.database.sql.DropForeignKeyConstraintStatement;
import liquibase.database.sql.SqlStatement;
import liquibase.database.structure.DatabaseObject;
import liquibase.database.structure.ForeignKey;
import liquibase.database.structure.Table;
import liquibase.exception.InvalidChangeDefinitionException;
import liquibase.exception.UnsupportedChangeException;
import liquibase.util.StringUtils;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Drops an existing foreign key constraint.
 */
public class DropForeignKeyConstraintChange extends AbstractChange
{
    private String baseTableSchemaName;
    private String baseTableName;
    private String constraintName;

    public DropForeignKeyConstraintChange()
    {
        super("dropForeignKeyConstraint", "Drop Foreign Key Constraint");
    }

    public String getBaseTableSchemaName()
    {
        return baseTableSchemaName;
    }

    public void setBaseTableSchemaName(String baseTableSchemaName)
    {
        this.baseTableSchemaName = baseTableSchemaName;
    }

    public String getBaseTableName()
    {
        return baseTableName;
    }

    public void setBaseTableName(String baseTableName)
    {
        this.baseTableName = baseTableName;
    }

    public String getConstraintName()
    {
        return constraintName;
    }

    public void setConstraintName(String constraintName)
    {
        this.constraintName = constraintName;
    }

    public void validate(Database database) throws InvalidChangeDefinitionException
    {
        if (StringUtils.trimToNull(baseTableName) == null)
        {
            throw new InvalidChangeDefinitionException("baseTableName is required", this);
        }
        if (StringUtils.trimToNull(constraintName) == null)
        {
            throw new InvalidChangeDefinitionException("constraintName is required", this);
        }

    }

    public SqlStatement[] generateStatements(Database database) throws UnsupportedChangeException
    {

        if (database instanceof SQLiteDatabase)
        {
            // return special statements for SQLite databases
            return generateStatementsForSQLiteDatabase(database);
        }

        if (database instanceof MSSQLDatabase)
        {
            return generateStatementsForMSSQLDatabase(database);
        }

        return new SqlStatement[]
        {
            createDropConstraintStmt(database)
        };
    }

    private SqlStatement createDropConstraintStmt(Database database)
    {
        return new DropForeignKeyConstraintStatement(getBaseTableSchemaName() == null ? database.getDefaultSchemaName() : getBaseTableSchemaName(),
                getBaseTableName(), getConstraintName());
    }

    private SqlStatement[] generateStatementsForMSSQLDatabase(Database database) throws UnsupportedChangeException
    {
        try
        {
            List<SqlStatement> stmts = new ArrayList<SqlStatement>();
            DatabaseConnection dbConn = database.getConnection();
            Connection conn = dbConn.getUnderlyingConnection();
            DatabaseMetaData metadata = dbConn.getMetaData();

            String referencingTableName = this.baseTableName;
            String referencingColName = null;
            String referencedTableName = null;
            String referencedColName = null;
            String fkName = this.constraintName;
            boolean fkFound = false;

            ResultSet rs = metadata.getImportedKeys(null, null, referencingTableName);
            while (rs.next())
            {
                if (rs.getString("FK_NAME").equalsIgnoreCase(fkName))
                {
                    referencingColName = rs.getString("FKCOLUMN_NAME");
                    referencedTableName = rs.getString("PKTABLE_NAME");
                    referencedColName = rs.getString("PKCOLUMN_NAME");
                    fkFound = true;
                    break;
                }
            }
            rs.close();

            if (!fkFound)
            {
                throw new Exception("FK with name: " + fkName + " on table: " + referencingTableName + " cannot be found in database.");
            }

            // find the trigger
            String[] trigger = SQLServerTriggerUtil.getDeleteTriggerForTable(referencedTableName, conn);

            // only look to validate existing triggers
            if (trigger != null)
            {
                // parse the trigger for set of tables/columns
                Set<String[]> updateStmts = SQLServerTriggerUtil.findCascadeUpdateStatements(trigger[0], trigger[1], trigger[2]);
                int initialSize = updateStmts.size();
                for (Iterator<String[]> iter = updateStmts.iterator(); iter.hasNext();)
                {
                    String[] updateStmt = iter.next();
                    String tableName = updateStmt[0];
                    String colName = updateStmt[1];

                    if (referencingTableName.equalsIgnoreCase(tableName) && referencingColName.equalsIgnoreCase(colName))
                    {
                        iter.remove();
                    }
                }

                // if the set contained the FKs table/column, then recreate the trigger
                if (updateStmts.size() < initialSize)
                {
                    stmts.add(SQLServerTriggerUtil.generateDropTrigger(trigger[0]));
                    stmts.add(SQLServerTriggerUtil.generateCreateTriggerStmt(trigger[1], referencedColName, updateStmts));
                }
            }

            stmts.add(new DropForeignKeyConstraintStatement(getBaseTableSchemaName() == null ? database.getDefaultSchemaName()
                    : getBaseTableSchemaName(), getBaseTableName(), getConstraintName()));

            return stmts.toArray(new SqlStatement[stmts.size()]);
        }
        catch (Exception e)
        {
            throw new UnsupportedChangeException(e.getMessage(), e);
        }
    }

    private SqlStatement[] generateStatementsForSQLiteDatabase(Database database) throws UnsupportedChangeException
    {
        // SQLite does not support foreign keys until now.
        // See for more information: http://www.sqlite.org/omitted.html
        // Therefore this is an empty operation...
        return new SqlStatement[] {};
    }

    public String getConfirmationMessage()
    {
        return "Foreign key " + getConstraintName() + " dropped";
    }

    public Element createNode(Document currentChangeLogFileDOM)
    {
        Element node = currentChangeLogFileDOM.createElement(getTagName());

        if (getBaseTableSchemaName() != null)
        {
            node.setAttribute("baseTableSchemaName", getBaseTableSchemaName());
        }

        node.setAttribute("baseTableName", getBaseTableName());
        node.setAttribute("constraintName", getConstraintName());

        return node;
    }

    public Set<DatabaseObject> getAffectedDatabaseObjects()
    {
        Set<DatabaseObject> returnSet = new HashSet<DatabaseObject>();

        Table baseTable = new Table(getBaseTableName());
        returnSet.add(baseTable);

        ForeignKey fk = new ForeignKey();
        fk.setName(constraintName);
        fk.setForeignKeyTable(baseTable);
        returnSet.add(fk);

        return returnSet;

    }

}
