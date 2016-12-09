package org.tmatesoft.sqljet.core.internal.vdbe;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

public class SqlJetVdbeMemInt extends SqlJetVdbeMemAbstract {
	private final long i;

	public SqlJetVdbeMemInt(long i) {
		this.i = i;
	}

	@Override
	public String stringValue() throws SqlJetException {
		return String.valueOf(i);
	}

	@Override
	public long intValue() {
		return i;
	}

	@Override
	public double realValue() {
		return i;
	}

	@Override
	public boolean isNull() {
		return false;
	}

	@Override
	public boolean isInt() {
		return true;
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
		return SqlJetValueType.INTEGER;
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
        /* Figure out whether to use 1, 2, 4, 6 or 8 bytes. */
        long i = this.i;
        long u;
        if (file_format >= 4 && (i & 1) == i) {
            return 8 + (int) i;
        }

        u = SqlJetUtility.absolute(i);

        if (u <= 127) {
			return 1;
		}
        if (u <= 32767) {
			return 2;
		}
        if (u <= 8388607) {
			return 3;
		}
        if (u <= 2147483647) {
			return 4;
		}
        if (u <= (((0x00008000l) << 32) - 1)) {
			return 5;
		}
        return 6;
	}

	@Override
	public int serialPut(ISqlJetMemoryPointer buf, int nBuf, int file_format) {
        int serialType = this.serialType(file_format);

        /* Integer and Real */
        if (serialType <= 7 && serialType > 0) {
            int i;
            long v = this.i;
            int len = i = SqlJetVdbeSerialType.serialTypeLen(serialType);
            assert (len <= nBuf);
            while (i-- > 0) {
                buf.putByteUnsigned(i, (int) v);
                v >>>= 8;
            }
            return len;
        }
        
        /* constants 0 or 1 */
        return 0;
	}
}
