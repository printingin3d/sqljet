/**
 * SqlJetRaiseExpression.java
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
import org.tmatesoft.sqljet.core.schema.ISqlJetExpression;

/**
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 */
public class SqlJetRaiseExpression extends SqlJetExpression implements ISqlJetExpression {
    public static enum Action {
        IGNORE, ROLLBACK, ABORT, FAIL;

        public static Action decode(String s) {
            if ("ignore".equalsIgnoreCase(s)) {
                return IGNORE;
            } else if ("rollback".equalsIgnoreCase(s)) {
                return ROLLBACK;
            } else if ("abort".equalsIgnoreCase(s)) {
                return ABORT;
            } else if ("fail".equalsIgnoreCase(s)) {
                return FAIL;
            }
            return null;
        }
    }
	
    private final Action action;
    private final String errorMessage;

    public SqlJetRaiseExpression(CommonTree ast) {
        action = Action.decode(ast.getChild(0).getText());
        if (ast.getChildCount() > 1) {
            errorMessage = ast.getChild(1).getText();
        } else {
            errorMessage = null;
        }
    }

    @Override
    public String toString() {
    	StringBuilder buffer = new StringBuilder();
        buffer.append("RAISE (");
        buffer.append(action);
        if (errorMessage != null) {
            buffer.append(' ');
            buffer.append(errorMessage);
        }
        buffer.append(')');
        return buffer.toString();
    }
}
