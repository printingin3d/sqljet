package org.tmatesoft.sqljet.core.simpleschema.types;

import java.util.Collections;
import java.util.List;

import org.tmatesoft.sqljet.core.schema.ISqlJetTypeDef;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

public class SqlJetSimpleDoubleField extends SqlJetAbstractTypeDef implements ISqlJetSimpleFieldType {
    private static final SqlJetSimpleDoubleField INSTANCE = new SqlJetSimpleDoubleField();

    public static SqlJetSimpleDoubleField getInstance() {
        return INSTANCE;
    }

    private SqlJetSimpleDoubleField() {
    }

    @Override
    public ISqlJetTypeDef toInnerRepresentation() {
        return this;
    }

    @Override
    public SqlJetTypeAffinity getTypeAffinity() {
        return SqlJetTypeAffinity.REAL;
    }

    @Override
    public boolean isInteger() {
        return false;
    }

    @Override
    public String toSql() {
        return "double";
    }

    @Override
    public List<String> getNames() {
        return Collections.singletonList("double");
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
