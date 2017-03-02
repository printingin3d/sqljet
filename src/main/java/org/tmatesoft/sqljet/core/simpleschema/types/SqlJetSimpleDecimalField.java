package org.tmatesoft.sqljet.core.simpleschema.types;

import java.util.Collections;
import java.util.List;

import org.tmatesoft.sqljet.core.schema.ISqlJetTypeDef;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

public class SqlJetSimpleDecimalField extends SqlJetAbstractTypeDef implements ISqlJetSimpleFieldType {
	private final int size1;
	private final int size2;

	public SqlJetSimpleDecimalField(int size1, int size2) {
		this.size1 = size1;
		this.size2 = size2;
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
		return "decimal("+size1+","+size2+")";
	}

	@Override
	public List<String> getNames() {
		return Collections.singletonList("decimal");
	}

	@Override
	public Integer getSize1() {
		return Integer.valueOf(size1);
	}

	@Override
	public Integer getSize2() {
		return Integer.valueOf(size2);
	}

}
