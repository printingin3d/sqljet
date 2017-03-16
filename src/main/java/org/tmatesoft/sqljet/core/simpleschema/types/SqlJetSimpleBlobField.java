package org.tmatesoft.sqljet.core.simpleschema.types;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.schema.ISqlJetTypeDef;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

public class SqlJetSimpleBlobField extends SqlJetAbstractTypeDef implements ISqlJetSimpleFieldType {
    private static final @Nonnull SqlJetSimpleBlobField INSTANCE = new SqlJetSimpleBlobField();

    public static @Nonnull SqlJetSimpleBlobField getInstance() {
        return INSTANCE;
    }

    private SqlJetSimpleBlobField() {
    }

    @Override
    public ISqlJetTypeDef toInnerRepresentation() {
        return this;
    }

    @Override
    public SqlJetTypeAffinity getTypeAffinity() {
        return SqlJetTypeAffinity.NONE;
    }

    @Override
    public boolean isInteger() {
        return false;
    }

    @Override
    public String toSql() {
        return "blob";
    }

    @Override
    public List<String> getNames() {
        return Collections.singletonList("blob");
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
