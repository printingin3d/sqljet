package org.tmatesoft.sqljet.core.simpleschema.types;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.schema.ISqlJetTypeDef;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

public class SqlJetSimpleTextField extends SqlJetAbstractTypeDef implements ISqlJetSimpleFieldType {
    private static final @Nonnull SqlJetSimpleTextField INSTANCE = new SqlJetSimpleTextField();

    public static @Nonnull SqlJetSimpleTextField getInstance() {
        return INSTANCE;
    }

    private SqlJetSimpleTextField() {
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
        return "text";
    }

    @Override
    public List<String> getNames() {
        return Collections.singletonList("text");
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
