/**
 * SqlJetBlobLiteral.java
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
public class SqlJetBlobLiteral extends SqlJetExpression implements ISqlJetLiteralValue {

    private final byte[] value;

    public SqlJetBlobLiteral(CommonTree ast) {
        this.value = parseBlob(ast.getChild(0).getText());
    }

    @Override
	public byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        return asBlob(value);
    }

    public static byte[] parseBlob(String data) {
        data = data.substring(2, data.length() - 1).toLowerCase(); // x'...'
        byte[] buffer = new byte[data.length() / 2];
        for (int i = 0; i < buffer.length; i++) {
            char c1 = data.charAt(i * 2);
            char c2 = data.charAt(i * 2 + 1);
            int b1 = c1 - (c1 >= 'a' ? 'a' : '0');
            int b2 = c2 - (c2 >= 'a' ? 'a' : '0');
            buffer[i] = (byte) (b1 * 16 + b2);
        }
        return buffer;
    }

    public static String asBlob(byte[] data) {
    	StringBuilder buffer = new StringBuilder("x'");
        for (byte b : data) {
            buffer.append((char) (b / 16 > 9 ? 'a' + b / 16 - 10 : '0' + b / 16));
            buffer.append((char) (b % 16 > 9 ? 'a' + b % 16 - 10 : '0' + b % 16));
        }
        buffer.append("'");
        return buffer.toString();
    }
}
