package org.tmatesoft.sqljet.core.simpleschema.types;

import org.tmatesoft.sqljet.core.schema.ISqlJetTypeDef;

public abstract class SqlJetAbstractTypeDef implements ISqlJetTypeDef {

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
