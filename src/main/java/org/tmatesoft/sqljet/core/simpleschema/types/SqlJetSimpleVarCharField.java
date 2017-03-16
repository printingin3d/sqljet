package org.tmatesoft.sqljet.core.simpleschema.types;

import java.util.Collections;
import java.util.List;

import org.tmatesoft.sqljet.core.schema.ISqlJetTypeDef;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

public class SqlJetSimpleVarCharField extends SqlJetAbstractTypeDef implements ISqlJetSimpleFieldType {
    private final int size;

    public SqlJetSimpleVarCharField(int size) {
        this.size = size;
    }

    @Override
    public ISqlJetTypeDef toInnerRepresentation() {
        return this;
    }

    @Override
    public SqlJetTypeAffinity getTypeAffinity() {
        return SqlJetTypeAffinity.TEXT;
    }

    @Override
    public boolean isInteger() {
        return false;
    }

    @Override
    public String toSql() {
        return "varchar(" + size + ")";
    }

    @Override
    public List<String> getNames() {
        return Collections.singletonList("varchar");
    }

    @Override
    public Integer getSize1() {
        return Integer.valueOf(size);
    }

    @Override
    public Integer getSize2() {
        return null;
    }

}
