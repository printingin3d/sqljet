package org.tmatesoft.sqljet.core.simpleschema.types;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.schema.ISqlJetTypeDef;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

public class SqlJetSimpleLongField extends SqlJetAbstractTypeDef implements ISqlJetSimpleFieldType {
    private static final @Nonnull SqlJetSimpleLongField INSTANCE = new SqlJetSimpleLongField();

    public static @Nonnull SqlJetSimpleLongField getInstance() {
        return INSTANCE;
    }

    private SqlJetSimpleLongField() {
    }

    @Override
    public ISqlJetTypeDef toInnerRepresentation() {
        return this;
    }

    @Override
    public SqlJetTypeAffinity getTypeAffinity() {
        return SqlJetTypeAffinity.NUMERIC;
    }

    @Override
    public boolean isInteger() {
        return false;
    }

    @Override
    public String toSql() {
        return "long";
    }

    @Override
    public List<String> getNames() {
        return Collections.singletonList("long");
    }

    @Override
    public Integer getSize1() {
        return null;
    }

    @Override
    public Integer getSize2() {
        return null;
    }

}
