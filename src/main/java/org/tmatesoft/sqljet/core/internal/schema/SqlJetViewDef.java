/**
 * VirtualTablesTest.java
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

import org.antlr.runtime.tree.CommonTree;
import org.tmatesoft.sqljet.core.schema.ISqlJetViewDef;

public class SqlJetViewDef implements ISqlJetViewDef {

    private final String name;

    private final boolean ifNotExists;
    private final String sqlStatement;

    private final long rowId;

    private SqlJetViewDef(String name, boolean ifNotExists, String sqlStatement, long rowId) {
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.sqlStatement = sqlStatement;
        this.rowId = rowId;
    }

    public SqlJetViewDef(String sql, CommonTree ast) {
        CommonTree optionsNode = (CommonTree) ast.getChild(0);

        sqlStatement = sql;
        ifNotExists = SqlJetTableDef.hasOption(optionsNode, "exists");

        CommonTree nameNode = (CommonTree) ast.getChild(1);
        name = nameNode.getText();
        this.rowId = 0;
    }

    @Override
    public String getName() {
        return name;
    }

    public boolean isKeepExisting() {
        return ifNotExists;
    }

    @Override
    public String toSQL() {
        return sqlStatement;
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public long getRowId() {
        return rowId;
    }

    public ISqlJetViewDef withRowId(long rowId) {
        return new SqlJetViewDef(name, ifNotExists, sqlStatement, rowId);
    }
}
