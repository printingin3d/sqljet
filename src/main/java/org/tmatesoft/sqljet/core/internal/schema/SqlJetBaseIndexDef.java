/**
 * SqlJetBaseIndexDef.java
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

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.schema.ISqlJetIndexDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetIndexedColumn;

/**
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 */
public class SqlJetBaseIndexDef implements ISqlJetIndexDef {

    private String name;
    private String tableName;
    private int page;
    private long rowId;

    public SqlJetBaseIndexDef(String name, @Nonnull String tableName, int page) {
        this.name = name;
        this.tableName = tableName;
        this.page = page;
    }

    @Override
	public void setName(String name) {
        this.name = name;
    }

    @Override
	public String getName() {
        return name;
    }

    @Override
	public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
	public String getTableName() {
        return tableName;
    }

    @Override
	public boolean isUnique() {
        return false;
    }

    @Override
	public List<ISqlJetIndexedColumn> getColumns() {
        return Collections.emptyList();
    }

    @Override
	public ISqlJetIndexedColumn getColumn(String name) {
        return null;
    }

    @Override
	public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    @Override
	public long getRowId() {
        return rowId;
    }

    public void setRowId(long rowId) {
        this.rowId = rowId;
    }

    @Override
    public String toString() {
    	StringBuilder buffer = new StringBuilder();
        buffer.append(getPage());
        buffer.append("/");
        buffer.append(getRowId());
        buffer.append(": ");
        buffer.append(getName());
        buffer.append(" ON ");
        buffer.append(getTableName());
        return buffer.toString();
    }

    @Override
	public String toSQL() {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.schema.ISqlJetIndexDef#isImplicit()
     */
    @Override
	public boolean isImplicit() {
        return true;
    }

}
