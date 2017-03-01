package org.tmatesoft.sqljet.core.simpleschema.types;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.schema.ISqlJetTypeDef;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

public class SqlJetSimpleIntField implements ISqlJetSimpleFieldType, ISqlJetTypeDef {
	private static final @Nonnull SqlJetSimpleIntField INSTANCE = new SqlJetSimpleIntField();

	public static @Nonnull SqlJetSimpleIntField getInstance() {
		return INSTANCE;
	}

	private SqlJetSimpleIntField() {}
	
	@Override
	public ISqlJetTypeDef toInnerRepresentation() {
		return this;
	}

	@Override
	public SqlJetTypeAffinity getTypeAffinity() {
		return SqlJetTypeAffinity.INTEGER;
	}

	@Override
	public boolean isInteger() {
		return true;
	}

	@Override
	public String toSql() {
		return "integer";
	}

	@Override
	public List<String> getNames() {
		return Collections.singletonList("int");
	}

	@Override
	public Double getSize1() {
		return null;
	}

	@Override
	public Double getSize2() {
		return null;
	}

}
