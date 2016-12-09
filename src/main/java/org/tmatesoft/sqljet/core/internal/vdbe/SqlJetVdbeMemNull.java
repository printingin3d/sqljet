package org.tmatesoft.sqljet.core.internal.vdbe;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

public class SqlJetVdbeMemNull extends SqlJetVdbeMemAbstract {
	public static final ISqlJetVdbeMem INSTANCE = new SqlJetVdbeMemNull();
	
	private SqlJetVdbeMemNull() {}

	@Override
	public String stringValue() throws SqlJetException {
		return null;
	}

	@Override
	public long intValue() {
		return 0;
	}

	@Override
	public double realValue() {
		return 0.0;
	}

	@Override
	public boolean isNull() {
		return true;
	}

	@Override
	public boolean isInt() {
		return false;
	}

	@Override
	public boolean isReal() {
		return false;
	}

	@Override
	public boolean isString() {
		return false;
	}

	@Override
	public boolean isBlob() {
		return false;
	}

	@Override
	public SqlJetValueType getType() {
		return SqlJetValueType.NULL;
	}

	@Override
	public ISqlJetMemoryPointer blobValue() throws SqlJetException {
		return null;
	}

	@Override
	public ISqlJetVdbeMem applyAffinity(SqlJetTypeAffinity affinity, SqlJetEncoding enc) throws SqlJetException {
		return this;
	}

	@Override
	public int serialType(int file_format) {
		return 0;
	}

	@Override
	public int serialPut(ISqlJetMemoryPointer buf, int nBuf, int file_format) {
		return 0;
	}
}
