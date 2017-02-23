package org.tmatesoft.sqljet.core.internal.vdbe;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

public class SqlJetVdbeMemBlob extends SqlJetVdbeMemAbstract {
	private final @Nonnull ISqlJetMemoryPointer z;
	private final @Nonnull SqlJetEncoding enc;

	public SqlJetVdbeMemBlob(@Nonnull ISqlJetMemoryPointer z, @Nonnull SqlJetEncoding enc) {
		this.z = z;
		this.enc = enc;
	}

	@Override
	public @Nonnull String stringValue() {
		return SqlJetUtility.toString(z, enc);
	}

	@Override
	public long intValue() {
        try {
        	return Long.parseLong(stringValue());
        } catch (NumberFormatException e) {
            return 0;
        }
	}

	@Override
	public double realValue() {
        try {
            return Double.parseDouble(stringValue());
        } catch (NumberFormatException e) {
            return 0.0;
        }
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
		return false;
	}

	@Override
	public boolean isString() {
		return false;
	}

	@Override
	public boolean isBlob() {
		return true;
	}

	@Override
	public SqlJetValueType getType() {
		return SqlJetValueType.BLOB;
	}

	@Override
	public ISqlJetMemoryPointer blobValue() {
		return z;
	}

	@Override
	public ISqlJetVdbeMem applyAffinity(SqlJetTypeAffinity affinity, @Nonnull SqlJetEncoding enc) throws SqlJetException {
		return this;
	}

	@Override
	public int serialType(int file_format) {
		return z.getLimit() * 2 + 12;
	}

	@Override
	public int serialPut(ISqlJetMemoryPointer buf, int nBuf, int file_format) {
        assert z.getLimit() <= nBuf;
        int len = this.z.getLimit();
        buf.copyFrom(this.z, len);
        return len;
	}

	@Override
	public Object toObject() {
        return blobValue();
	}
}
