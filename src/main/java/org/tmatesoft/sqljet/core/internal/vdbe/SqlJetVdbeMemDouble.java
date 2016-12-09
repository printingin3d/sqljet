package org.tmatesoft.sqljet.core.internal.vdbe;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

public class SqlJetVdbeMemDouble extends SqlJetVdbeMemAbstract {
	private final double r;

	public SqlJetVdbeMemDouble(double r) {
		this.r = r;
	}

	@Override
	public String stringValue() throws SqlJetException {
		return String.valueOf(r);
	}

	@Override
	public long intValue() {
		return (long) r;
	}

	@Override
	public double realValue() {
		return r;
	}

	@Override
	public boolean isNull() {
		return false;
	}

	@Override
	public boolean isInt() {
		return false;
	}

	@Override
	public boolean isReal() {
		return true;
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
		return SqlJetValueType.FLOAT;
	}

	@Override
	public ISqlJetMemoryPointer blobValue() throws SqlJetException {
		return SqlJetUtility.fromString(stringValue(), SqlJetEncoding.UTF8);
	}

	@Override
	public ISqlJetVdbeMem applyAffinity(SqlJetTypeAffinity affinity, SqlJetEncoding enc) throws SqlJetException {
        if (affinity == SqlJetTypeAffinity.TEXT) {
            return SqlJetVdbeMemFactory.getStr(stringValue(), enc);
        }
		return this;
	}

	@Override
	public int serialType(int file_format) {
		return 7;
	}

	@Override
	public int serialPut(ISqlJetMemoryPointer buf, int nBuf, int file_format) {
        int serialType = this.serialType(file_format);

        buf.putLong(Double.doubleToLongBits(this.r));
        return SqlJetVdbeSerialType.serialTypeLen(serialType);
	}
}
