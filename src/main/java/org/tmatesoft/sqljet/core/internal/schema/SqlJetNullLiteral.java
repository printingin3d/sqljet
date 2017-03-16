/**
 * SqlJetNullLiteral.java
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

import org.tmatesoft.sqljet.core.schema.ISqlJetNullLiteral;

/**
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 */
public class SqlJetNullLiteral extends SqlJetExpression implements ISqlJetNullLiteral {
    private static final SqlJetNullLiteral INSTANCE = new SqlJetNullLiteral();

    public static SqlJetNullLiteral getInstance() {
        return INSTANCE;
    }

    private SqlJetNullLiteral() {
    }

    @Override
    public String toString() {
        return "NULL";
    }
}
