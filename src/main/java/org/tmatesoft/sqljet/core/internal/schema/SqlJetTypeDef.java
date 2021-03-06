/**
 * SqlJetTypeDef.java
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.antlr.runtime.tree.CommonTree;
import org.tmatesoft.sqljet.core.schema.ISqlJetTypeDef;

/**
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 */
public class SqlJetTypeDef implements ISqlJetTypeDef {

    private final List<String> names;
    private final Integer size1;
    private final Integer size2;

    public SqlJetTypeDef(CommonTree typeNode) {
        CommonTree paramsNode = (CommonTree) typeNode.getChild(0);
        if (paramsNode.getChildCount() > 0) {
            String text = paramsNode.getChild(0).getText();
            size1 = Integer.valueOf(text);
        } else {
            size1 = null;
        }
        if (paramsNode.getChildCount() > 1) {
            String text = paramsNode.getChild(1).getText();
            size2 = Integer.valueOf(text);
        } else {
            size2 = null;
        }
        List<String> typeNames = new ArrayList<>();
        for (int i = 1; i < typeNode.getChildCount(); i++) {
            typeNames.add(typeNode.getChild(i).getText());
        }
        this.names = Collections.unmodifiableList(typeNames);
    }

    @Override
    public List<String> getNames() {
        return names;
    }

    @Override
    public Integer getSize1() {
        return size1;
    }

    @Override
    public Integer getSize2() {
        return size2;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < getNames().size(); i++) {
            if (i > 0) {
                buffer.append(' ');
            }
            buffer.append(getNames().get(i));
        }
        if (getSize1() != null) {
            buffer.append(" (");
            buffer.append(String.format("%d", getSize1()));
            if (getSize2() != null) {
                buffer.append(", ");
                buffer.append(String.format("%d", getSize2()));
            }
            buffer.append(')');
        }
        return buffer.toString();
    }
}
