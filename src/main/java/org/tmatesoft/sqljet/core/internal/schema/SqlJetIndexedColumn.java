/**
 * SqlJetIndexedColumn.java
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
 */
package org.tmatesoft.sqljet.core.internal.schema;

import javax.annotation.Nonnull;

import org.antlr.runtime.tree.CommonTree;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetIndexedColumn;
import org.tmatesoft.sqljet.core.schema.SqlJetSortingOrder;

/**
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 */
public class SqlJetIndexedColumn implements ISqlJetIndexedColumn {

    private final String name;
    private final String collation;
    private final SqlJetSortingOrder sortingOrder;

    private ISqlJetColumnDef tableColumn;

    public SqlJetIndexedColumn(String name, String collation, SqlJetSortingOrder sortingOrder) {
        this.name = name;
        this.collation = collation;
        this.sortingOrder = sortingOrder;
    }

    public SqlJetIndexedColumn(String name) {
        this(name, null, null);
    }

    public static @Nonnull SqlJetIndexedColumn parse(CommonTree ast) {
        String collation = null;
        SqlJetSortingOrder sortingOrder = null;
        for (int i = 0; i < ast.getChildCount(); i++) {
            CommonTree child = (CommonTree) ast.getChild(i);
            if ("collate".equalsIgnoreCase(child.getText())) {
                collation = child.getChild(0).getText();
            } else if ("asc".equalsIgnoreCase(child.getText())) {
                sortingOrder = SqlJetSortingOrder.ASC;
            } else if ("desc".equalsIgnoreCase(child.getText())) {
                sortingOrder = SqlJetSortingOrder.DESC;
            } else {
                assert false;
            }
        }
        return new SqlJetIndexedColumn(ast.getText(), collation, sortingOrder);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getCollation() {
        return collation;
    }

    @Override
    public SqlJetSortingOrder getSortingOrder() {
        return sortingOrder;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(SqlJetUtility.quoteName(getName()));
        if (getCollation() != null) {
            buffer.append(" COLLATE ");
            buffer.append(getCollation());
        }
        if (getSortingOrder() != null) {
            buffer.append(' ');
            buffer.append(getSortingOrder());
        }
        return buffer.toString();
    }

    /**
     * @param tableColumn
     *            the tableColumn to set
     */
    public void setTableColumn(ISqlJetColumnDef tableColumn) {
        this.tableColumn = tableColumn;
    }

    /**
     * @return the tableColumn
     */
    @Override
    public ISqlJetColumnDef getTableColumn() {
        return tableColumn;
    }

}
