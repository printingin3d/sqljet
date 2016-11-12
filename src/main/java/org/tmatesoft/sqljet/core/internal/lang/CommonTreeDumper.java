/**
 * CommonTreeDumper.java
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
package org.tmatesoft.sqljet.core.internal.lang;

import org.antlr.runtime.tree.CommonTree;

/**
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 */
public class CommonTreeDumper {
	public static CommonTreeDumper INSTANCE = new CommonTreeDumper();
	
	private CommonTreeDumper() {}

    protected void addPrefix(StringBuilder buffer, int length) {
        buffer.append("{");
    }

    protected void addSuffix(StringBuilder buffer, int length) {
        buffer.append("}");
    }

    public void addTree(StringBuilder buffer, CommonTree tree, int offset) {
        buffer.append(tree.getText());
        for (int i = 0; i < tree.getChildCount(); i++) {
            addPrefix(buffer, offset);
            addTree(buffer, (CommonTree) tree.getChild(i), offset + 1);
            addSuffix(buffer, offset);
        }
    }

    public static String toString(CommonTree tree) {
        StringBuilder buffer = new StringBuilder();
        INSTANCE.addTree(buffer, tree, 0);
        return buffer.toString();
    }
}
