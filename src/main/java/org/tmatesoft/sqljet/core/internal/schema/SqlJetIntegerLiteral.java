/**
 * SqlJetIntegerLiteral.java
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
import org.tmatesoft.sqljet.core.schema.ISqlJetLiteralValue;

/**
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 */
public class SqlJetIntegerLiteral extends SqlJetExpression implements ISqlJetLiteralValue {

    private final long value;

    public SqlJetIntegerLiteral(CommonTree ast) {
        value = Long.parseLong(ast.getChild(0).getText());
    }

    @Override
	public Long getValue() {
        return Long.valueOf(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
