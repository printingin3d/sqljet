/**
 * SqlJetAlterTableDef.java
 * Copyright (C) 2009-2013 TMate Software Ltd
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * For information on how to redistribute this software under
 * the terms of a license other than GNU General Public License
 * contact TMate Software at support@sqljet.com
 */
package org.tmatesoft.sqljet.core.internal.schema;

import static org.tmatesoft.sqljet.core.internal.SqlJetAssert.assertNotEmpty;
import static org.tmatesoft.sqljet.core.internal.SqlJetAssert.assertTrue;

import javax.annotation.Nonnull;

import org.antlr.runtime.ParserRuleReturnScope;
import org.antlr.runtime.tree.CommonTree;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.lang.SqlParser;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnDef;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class SqlJetAlterTableDef {

    private static final String INVALID_ALTER_TABLE_STATEMENT = "Invalid ALTER TABLE statement";

    private final @Nonnull String tableName;
    private final String newTableName;
    private final @Nonnull String tableQuotedName;
    private final String newTableQuotedName;
    private final ISqlJetColumnDef newColumnDef;

    /**
     * @param ast
     * @throws SqlJetException
     */
    public SqlJetAlterTableDef(ParserRuleReturnScope parsedSql) throws SqlJetException {
        final CommonTree ast = (CommonTree)parsedSql.getTree();
        final int childCount = ast.getChildCount();
        assertTrue(childCount >= 5, SqlJetErrorCode.MISUSE, INVALID_ALTER_TABLE_STATEMENT);
        assertTrue("alter".equalsIgnoreCase(((CommonTree) ast.getChild(0)).getText()), 
        		SqlJetErrorCode.MISUSE, INVALID_ALTER_TABLE_STATEMENT);
        assertTrue("table".equalsIgnoreCase(((CommonTree) ast.getChild(1)).getText()), 
        		SqlJetErrorCode.MISUSE, INVALID_ALTER_TABLE_STATEMENT);

        final CommonTree tableNameNode = (CommonTree) ast.getChild(2);
        tableName = assertNotEmpty(tableNameNode.getText(), SqlJetErrorCode.BAD_PARAMETER);
        tableQuotedName = assertNotEmpty(SqlParser.quotedId(tableNameNode), SqlJetErrorCode.BAD_PARAMETER);
        final CommonTree actionNode = (CommonTree) ast.getChild(3);
        final String action = actionNode.getText();
        final CommonTree child = (CommonTree) ast.getChild(4);
        if ("add".equalsIgnoreCase(action)) {
            newTableName = null;
            newTableQuotedName = null;
            final CommonTree newColumnNode;
            if ("column".equalsIgnoreCase(child.getText())) {
            	assertTrue(childCount == 6, SqlJetErrorCode.MISUSE, INVALID_ALTER_TABLE_STATEMENT);
                newColumnNode = (CommonTree) ast.getChild(5);
            } else {
            	assertTrue(childCount == 5, SqlJetErrorCode.MISUSE, INVALID_ALTER_TABLE_STATEMENT);
                newColumnNode = child;
            }
            newColumnDef = new SqlJetColumnDef(newColumnNode);
        } else if ("rename".equalsIgnoreCase(action)) {
            newColumnDef = null;
            assert "to".equalsIgnoreCase(child.getText());
            assertTrue(childCount >= 6, SqlJetErrorCode.MISUSE, INVALID_ALTER_TABLE_STATEMENT);
            
            final CommonTree newTableNode = (CommonTree) ast.getChild(5);
            newTableName = newTableNode.getText();
            newTableQuotedName = SqlParser.quotedId(newTableNode);
        } else {
            throw new SqlJetException(SqlJetErrorCode.MISUSE, INVALID_ALTER_TABLE_STATEMENT);
        }
    }

    /**
     * @return
     */
    public @Nonnull String getTableName() {
        return tableName;
    }

    /**
     * @return
     */
    public String getNewTableName() {
        return newTableName;
    }

    public @Nonnull String getTableQuotedName() {
		return tableQuotedName;
	}

    public String getNewTableQuotedName() {
		return newTableQuotedName;
	}

    /**
     * @return
     */
    public ISqlJetColumnDef getNewColumnDef() {
        return newColumnDef;
    }
}
