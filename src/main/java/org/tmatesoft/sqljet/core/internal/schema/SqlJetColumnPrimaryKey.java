/**
 * SqlJetColumnPrimaryKey.java
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

import org.antlr.runtime.tree.CommonTree;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnPrimaryKey;
import org.tmatesoft.sqljet.core.schema.SqlJetConflictAction;

/**
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 */
public class SqlJetColumnPrimaryKey extends SqlJetColumnIndexConstraint implements ISqlJetColumnPrimaryKey {

    private Boolean ascending;
    private boolean autoincremented;
    private SqlJetConflictAction conflictAction;

    public SqlJetColumnPrimaryKey(ISqlJetColumnDef column, String name, Boolean ascending, boolean autoincremented,
            SqlJetConflictAction conflictAction) {
        super(column, name);
        this.ascending = ascending;
        this.autoincremented = autoincremented;
        this.conflictAction = conflictAction;
    }

    public SqlJetColumnPrimaryKey(SqlJetColumnDef column, String name, CommonTree ast) {
        super(column, name);
        assert "primary".equalsIgnoreCase(ast.getText());
        for (int i = 0; i < ast.getChildCount(); i++) {
            CommonTree child = (CommonTree) ast.getChild(i);
            if ("asc".equalsIgnoreCase(child.getText())) {
                ascending = Boolean.TRUE;
            } else if ("desc".equalsIgnoreCase(child.getText())) {
                ascending = Boolean.FALSE;
            } else if ("autoincrement".equalsIgnoreCase(child.getText())) {
                autoincremented = true;
            } else if ("conflict".equalsIgnoreCase(child.getText())) {
                assert child.getChildCount() == 1;
                child = (CommonTree) child.getChild(0);
                conflictAction = SqlJetConflictAction.decode(child.getText());
            } else {
                assert false;
            }
        }
    }

    @Override
    public Boolean isAscending() {
        return ascending;
    }

    @Override
    public boolean isAutoincremented() {
        return autoincremented;
    }

    @Override
    public SqlJetConflictAction getConflictAction() {
        return conflictAction;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(super.toString());
        if (buffer.length() > 0) {
            buffer.append(' ');
        }
        buffer.append("PRIMARY KEY");
        if (Boolean.TRUE.equals(ascending)) {
            buffer.append(" ASC");
        }
        if (Boolean.FALSE.equals(ascending)) {
            buffer.append(" DESC");
        }
        if (conflictAction != null) {
            buffer.append(" ON CONFLICT ");
            buffer.append(conflictAction);
        }
        if (autoincremented) {
            buffer.append(" AUTOINCREMENT");
        }
        return buffer.toString();
    }
}
