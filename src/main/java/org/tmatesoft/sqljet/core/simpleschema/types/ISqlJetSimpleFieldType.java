package org.tmatesoft.sqljet.core.simpleschema.types;

import org.tmatesoft.sqljet.core.schema.ISqlJetTypeDef;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

public interface ISqlJetSimpleFieldType {
    ISqlJetTypeDef toInnerRepresentation();

    SqlJetTypeAffinity getTypeAffinity();

    boolean isInteger();

    String toSql();
}
