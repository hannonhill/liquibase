package liquibase.change;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.MSSQLDatabase;
import liquibase.database.SQLiteDatabase;
import liquibase.database.sql.DropForeignKeyConstraintStatement;
import liquibase.database.sql.SqlStatement;
import liquibase.database.structure.DatabaseObject;
import liquibase.database.structure.DatabaseSnapshot;
import liquibase.database.structure.ForeignKey;
import liquibase.database.structure.Table;
import liquibase.exception.InvalidChangeDefinitionException;
import liquibase.exception.UnsupportedChangeException;
import liquibase.util.StringUtils;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import sun.security.krb5.internal.UDPClient;

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
        Set<SqlStatement> stmts = new HashSet<SqlStatement>();
        DatabaseConnection dbConn = database.getConnection();
        Connection conn = dbConn.getUnderlyingConnection();
        try
        {
            DatabaseSnapshot snap = database.createDatabaseSnapshot(null, null);
            ForeignKey fk = snap.getForeignKey(this.constraintName);
            String referencingCol = fk.getForeignKeyColumns();
            String referencedTable = fk.getPrimaryKeyTable().getName();
                
            // find the trigger
            String[] trigger = SQLServerTriggerUtil.getDeleteTriggerForTable(referencedTable, conn);
                
            // parse the trigger for set of tables/columns
            Set<String[]> updateStmts = SQLServerTriggerUtil.findCascadeUpdateStatements(trigger[0], trigger[1], trigger[2]);
            int initialSize = updateStmts.size();
            for (Iterator<String[]> iter = updateStmts.iterator(); iter.hasNext(); )
            {
                String[] updateStmt = iter.next();
                String tableName = updateStmt[0];
                String colName = updateStmt[1];
                
                if (this.baseTableName.equalsIgnoreCase(tableName) && referencingCol.equalsIgnoreCase(colName))
                {
                    iter.remove();
                }
            }
            
            // if the set contained the FKs table/column, then recreate the trigger
            if (updateStmts.size() < initialSize)
            {
                stmts.add(SQLServerTriggerUtil.generateDropTrigger(trigger[0]));
                stmts.add(SQLServerTriggerUtil.generateCreateTriggerStmt(trigger[1], fk.getPrimaryKeyColumns(), updateStmts));
            }
        }
        catch (Exception e)
        {
            throw new UnsupportedChangeException(e.getMessage(), e);
        }
        
        stmts.add(new DropForeignKeyConstraintStatement(getBaseTableSchemaName() == null ? database.getDefaultSchemaName() : getBaseTableSchemaName(),
                getBaseTableName(), getConstraintName()));
        
        SqlStatement[] stmtsArr = new SqlStatement[stmts.size()];
        int i = 0;
        for (SqlStatement stmt : stmts)
        {
            stmtsArr[i] = stmt;
            i++;
        }
        
        return stmtsArr;
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
