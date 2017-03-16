package org.tmatesoft.sqljet.core.internal.vdbe;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

public class SqlJetVdbeMemString extends SqlJetVdbeMemAbstract {
    private final @Nonnull String str;
    private final @Nonnull SqlJetEncoding enc;

    public SqlJetVdbeMemString(@Nonnull String str, @Nonnull SqlJetEncoding enc) {
        this.str = str;
        this.enc = enc;
    }

    @Override
    public String stringValue() {
        return str;
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
        return true;
    }

    @Override
    public boolean isBlob() {
        return false;
    }

    @Override
    public SqlJetValueType getType() {
        return SqlJetValueType.TEXT;
    }

    @Override
    public ISqlJetMemoryPointer blobValue() {
        return SqlJetUtility.fromString(str, enc);
    }

    @Override
    public ISqlJetVdbeMem applyAffinity(SqlJetTypeAffinity affinity, @Nonnull SqlJetEncoding enc)
            throws SqlJetException {
        if (affinity == SqlJetTypeAffinity.INTEGER || affinity == SqlJetTypeAffinity.REAL
                || affinity == SqlJetTypeAffinity.NUMERIC) {
            // apply numeric affinity
            String zStr = stringValue();
            if (SqlJetUtility.isNumber(zStr)) {
                if (SqlJetUtility.isRealNumber(zStr)) {
                    return SqlJetVdbeMemFactory.getDouble(Double.parseDouble(zStr));
                } else {
                    return SqlJetVdbeMemFactory.getInt(Long.parseLong(zStr));
                }
            }
        }
        return this;
    }

    @Override
    public int serialType(int file_format) {
        return str.getBytes(enc.getCharset()).length * 2 + 13;
    }

    @Override
    public int serialPut(ISqlJetMemoryPointer buf, int nBuf, int file_format) {
        byte[] bytes = str.getBytes(enc.getCharset());
        int len = bytes.length;
        assert len <= nBuf;
        buf.putBytes(bytes);
        return len;
    }

    @Override
    public Object toObject() {
        return stringValue();
    }
}
