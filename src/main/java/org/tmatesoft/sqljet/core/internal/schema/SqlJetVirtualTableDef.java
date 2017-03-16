/**
 * SqlJetVirtualTableDef.java
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.antlr.runtime.tree.CommonTree;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetVirtualTableDef;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetVirtualTableDef implements ISqlJetVirtualTableDef {

    private final @Nonnull String tableName;
    private final String databaseName;
    private final String moduleName;
    private final List<ISqlJetColumnDef> moduleColumns;

    private int page;
    private long rowId;

    /**
     * @throws SqlJetException
     * 
     */
    public SqlJetVirtualTableDef(CommonTree ast, int page) throws SqlJetException {
        final CommonTree nameNode = (CommonTree) ast.getChild(1);
        tableName = assertNotEmpty(nameNode.getText(), SqlJetErrorCode.BAD_PARAMETER);
        databaseName = nameNode.getChildCount() > 0 ? nameNode.getChild(0).getText() : null;

        final CommonTree moduleNode = (CommonTree) ast.getChild(2);
        moduleName = moduleNode.getText();

        List<ISqlJetColumnDef> moduleColumns = new ArrayList<>();
        if (ast.getChildCount() > 3) {
            CommonTree defNode = (CommonTree) ast.getChild(3);
            if ("columns".equalsIgnoreCase(defNode.getText())) {
                for (int i = 0; i < defNode.getChildCount(); i++) {
                    moduleColumns.add(new SqlJetColumnDef((CommonTree) defNode.getChild(i)));
                }
            }
        }
        this.moduleColumns = Collections.unmodifiableList(moduleColumns);

        this.page = page;
    }

    @Override
    public @Nonnull String getTableName() {
        return tableName;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getModuleName() {
        return moduleName;
    }

    @Override
    public List<ISqlJetColumnDef> getModuleColumns() {
        return moduleColumns;
    }

    @Override
    public int getPage() {
        return page;
    }

    @Override
    public void setPage(int page) {
        this.page = page;
    }

    @Override
    public long getRowId() {
        return rowId;
    }

    @Override
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
        buffer.append(toSQL(false));
        return buffer.toString();
    }

    @Override
    public String toSQL() {
        return toSQL(true);
    }

    public String toSQL(boolean schemaStrict) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("CREATE VIRTUAL TABLE ");
        if (!schemaStrict) {
            if (getDatabaseName() != null) {
                buffer.append(getDatabaseName());
                buffer.append('.');
            }
        }
        buffer.append(getTableName());
        buffer.append(" USING ");
        buffer.append(getModuleName());
        if (!moduleColumns.isEmpty()) {
            buffer.append(" (");
            boolean first = true;
            for (ISqlJetColumnDef cd : moduleColumns) {
                if (!first) {
                    buffer.append(", ");
                }
                first = false;
                buffer.append(cd.toString());
            }
            buffer.append(')');
        }
        return buffer.toString();
    }

}
