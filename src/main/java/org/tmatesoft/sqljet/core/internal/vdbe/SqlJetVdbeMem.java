/**
 * SqlJetMem.java
 * Copyright (C) 2009-2013 TMate Software Ltd
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * For information on how to redistribute this software under
 * the terms of a license other than GNU General Public License
 * contact TMate Software at support@sqljet.com
 */
package org.tmatesoft.sqljet.core.internal.vdbe;

import static org.tmatesoft.sqljet.core.internal.SqlJetUtility.mutexHeld;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtreeCursor;
import org.tmatesoft.sqljet.core.internal.ISqlJetLimits;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetCloneable;
import org.tmatesoft.sqljet.core.internal.SqlJetResultWithOffset;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

/**
 * Internally, the vdbe manipulates nearly all SQL values as Mem structures.
 * Each Mem struct may cache multiple representations (string, integer etc.) of
 * the same value. A value (and therefore Mem structure) has the following
 * properties:
 *
 * Each value has a manifest type. The manifest type of the value stored in a
 * Mem struct is returned by the MemType(Mem*) macro. The type is one of
 * SQLITE_NULL, SQLITE_INTEGER, SQLITE_REAL, SQLITE_TEXT or SQLITE_BLOB.
 *
 *
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class SqlJetVdbeMem extends SqlJetCloneable implements ISqlJetVdbeMem {
    
    /** Integer value. */
    private long i;

    /** Real value */
    private double r;

    /** String or BLOB value */
    protected ISqlJetMemoryPointer z;

    /** Number of characters in string value, excluding '\0' */
    private int n;

    /** One of SQLITE_NULL, SQLITE_TEXT, SQLITE_INTEGER, etc */
    private SqlJetValueType type = SqlJetValueType.NULL;

    /** SQLITE_UTF8, SQLITE_UTF16BE, SQLITE_UTF16LE */
    private SqlJetEncoding enc;

    private static final SqlJetVdbeMemPool pool = new SqlJetVdbeMemPool();
    
    public static SqlJetVdbeMem obtainInstance() {
        return pool.obtain();
    }

    protected SqlJetVdbeMem() {
    }

    /**
     * Release any memory held by the Mem. This may leave the Mem in an
     * inconsistent state, for example with (Mem.z==0) and
     * (Mem.type==SQLITE_TEXT).
     * 
     */
	private void reset() {
        z = null;
    }
    
    @Override
	public void release() {
        i = 0;
        r = 0;
        z = null;
        n = 0;
        type = SqlJetValueType.NULL;
        enc = null;
        pool.release(this);
    }

    /**
     * Compare the values contained by the two memory cells, returning negative,
     * zero or positive if pMem1 is less than, equal to, or greater than pMem2.
     * Sorting order is NULL's first, followed by numbers (integers and reals)
     * sorted numerically, followed by text ordered by the collating sequence
     * pColl and finally blob's ordered by memcmp().Two NULL values are
     * considered equal by this function.
     * @throws SqlJetException 
     */
    @Override
	public int compare(ISqlJetVdbeMem that) throws SqlJetException {
        /*
         * If one value is NULL, it is less than the other. If both values* are
         * NULL, return 0.
         */
        if (this.isNull() || that.isNull()) {
            return (that.isNull() ? 1 : 0) - (this.isNull() ? 1 : 0);
        }
        
        /*
         * If one value is a number and the other is not, the number is less.*
         * If both are numbers, compare as reals if one is a real, or as
         * integers* if both values are integers.
         */
        if (this.isNumber() || that.isNumber()) {
            if (!this.isNumber()) {
                return 1;
            }
            if (!that.isNumber()) {
                return -1;
            }
            /* Comparing to numbers as doubles */
        	double r1 = this.realValue();
            double r2 = that.realValue();
            return Double.compare(r1, r2);
        }

        /*
         * If one value is a string and the other is a blob, the string is less.
         * * If both are strings, compare using the collating functions.
         */
        if (this.isString() || that.isString()) {
            if (!this.isString()) {
                return 1;
            }
            if (!that.isString()) {
                return -1;
            }

            return this.stringValue().compareTo(that.stringValue());
        }

        ISqlJetMemoryPointer blob1 = this.blobValue();
        ISqlJetMemoryPointer blob2 = that.blobValue();
        
        /* Both values must be blobs or strings. Compare using memcmp(). */
        int rc = SqlJetUtility.memcmp(blob1, blob2, Integer.min(blob1.getLimit(), blob2.getLimit()));
        if (rc == 0) {
            rc = blob1.getLimit() - blob2.getLimit();
        }
        return rc;
    }

    @Override
    public String stringValue() throws SqlJetException {
    	if (isNull()) {
    		return null;
    	}
    	if (isInt()) {
    		return String.valueOf(i);
    	}
    	if (isReal()) {
    		return String.valueOf(r);
    	}
    	if (isString() || isBlob()) {
    		return SqlJetUtility.toString(z, enc);
    	}
    	return null;
    }

    /**
     * Add MEM_Str to the set of representations for the given Mem. Numbers are
     * converted using sqlite3_snprintf(). Converting a BLOB to a string is a
     * no-op.
     * 
     * Existing representations MEM_Int and MEM_Real are *not* invalidated.
     * 
     * A MEM_Null value will never be passed to this function. This function is
     * used for converting values to text for returning to the user (i.e. via
     * sqlite3_value_text()), or for ensuring that values to be used as btree
     * keys are strings. In the former case a NULL pointer is returned the user
     * and the later is an internal programming error.
     * 
     * @param enc
     * @throws SqlJetException
     */
	private void stringify(SqlJetEncoding enc) throws SqlJetException {
        assert (!(isString() || isBlob()));
        assert isNumber();

        setStr(SqlJetUtility.fromString(stringValue(), enc), enc);
    }

    /**
     * Move data out of a btree key or data field and into a Mem structure. The
     * data or key is taken from the entry that pCur is currently pointing to.
     * offset and amt determine what portion of the data or key to retrieve. key
     * is true to get the key or false to get data. The result is written into
     * the pMem element.
     * 
     * The pMem structure is assumed to be uninitialized. Any prior content is
     * overwritten without being freed.
     * 
     * If this routine fails for any reason (malloc returns NULL or unable to
     * read from the disk) then the pMem is left in an inconsistent state.
     * 
     * @param pCur
     * @param offset
     *            Offset from the start of data to return bytes from.
     * @param amt
     *            Number of bytes to return.
     * @param key
     *            If true, retrieve from the btree key, not data.
     * @return
     * @throws SqlJetException
     */
	public static ISqlJetMemoryPointer fromBtree(ISqlJetBtreeCursor pCur, int offset, int amt, boolean key) throws SqlJetException {
        assert (mutexHeld(pCur.getCursorDb().getMutex()));

        ISqlJetMemoryPointer result;
        /* Data from the btree layer */
        ISqlJetMemoryPointer zData;
        /* Number of bytes available on the local btree page */
        int[] available = { 0 };

        if (key) {
            zData = pCur.keyFetch(available);
        } else {
            zData = pCur.dataFetch(available);
        }
        assert (zData != null);

        if (offset + amt <= available[0]) {
        	result = zData.pointer(offset);
        } else {
        	result = SqlJetUtility.memoryManager.allocatePtr(amt+2);
            if (key) {
                pCur.key(offset, amt, result);
            } else {
                pCur.data(offset, amt, result);
            }
            result.putByteUnsigned(amt, (byte) 0);
            result.putByteUnsigned(amt + 1, (byte) 0);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetVdbeMem#intValue()
     */
    @Override
	public long intValue() {
        if (isInt()) {
            return i;
        } else if (isReal()) {
            return (long) r;
        } else if (isString() || isBlob()) {
            try {
            	return Long.parseLong(SqlJetUtility.toString(this.z, enc));
            } catch (SqlJetException e) {
                return 0;
            }
        } else {
            return 0;
        }
    }

    /**
     * Delete any previous value and set the value stored in *pMem to NULL.
     */
	public void setNull() {
        type = SqlJetValueType.NULL;
    }

    /**
     * Change the value of a Mem to be a string.
     * 
     * The memory management strategy depends on the value of the xDel
     * parameter. If the value passed is SQLITE_TRANSIENT, then the string is
     * copied into a (possibly existing) buffer managed by the Mem structure.
     * Otherwise, any existing buffer is freed and the pointer copied.
     * 
     * @throws SqlJetException
     */
	public void setStr(ISqlJetMemoryPointer z, SqlJetEncoding enc) throws SqlJetException {
        int nByte = z.remaining(); /* New value for pMem->n */

        if (nByte > ISqlJetLimits.SQLJET_MAX_LENGTH) {
            throw new SqlJetException(SqlJetErrorCode.TOOBIG);
        }

        this.z = z;
        this.n = nByte;
        this.enc = (enc == null ? SqlJetEncoding.UTF8 : enc);
        this.type = SqlJetValueType.TEXT;
    }
    
    /**
     * Change the value of a Mem to be a BLOB.
     * 
     * The memory management strategy depends on the value of the xDel
     * parameter. If the value passed is SQLITE_TRANSIENT, then the string is
     * copied into a (possibly existing) buffer managed by the Mem structure.
     * Otherwise, any existing buffer is freed and the pointer copied.
     * 
     * @throws SqlJetException
     */
    public void setBlob(ISqlJetMemoryPointer z, SqlJetEncoding enc) throws SqlJetException {
    	int nByte = z.remaining(); /* New value for pMem->n */
    	/* New value for pMem->flags */
    	
    	if (nByte > ISqlJetLimits.SQLJET_MAX_LENGTH) {
    		throw new SqlJetException(SqlJetErrorCode.TOOBIG);
    	}
    	
    	this.z = z;
    	this.n = nByte;
    	this.enc = (enc == null ? SqlJetEncoding.UTF8 : enc);
    	this.type = SqlJetValueType.BLOB;
    }

    /**
     * Delete any previous value and set the value stored in *pMem to val,
     * manifest type INTEGER.
     */
	public void setInt64(long val) {
        reset();
        i = val;
        type = SqlJetValueType.INTEGER;
    }

    @Override
	public double realValue() {
        if (isReal()) {
            return r;
        } else if (isInt()) {
            return i;
        } else if (isString() || isBlob()) {
            try {
                return Double.parseDouble(SqlJetUtility.toString(z, enc));
            } catch (SqlJetException e) {
                return 0.0;
            }
        } else {
            return 0.0;
        }
    }

    /**
     * Delete any previous value and set the value stored in *pMem to val,
     * manifest type REAL.
     */
	public void setDouble(double val) {
        if (Double.isNaN(val)) {
            this.setNull();
        } else {
            this.reset();
            this.r = val;
            this.type = SqlJetValueType.FLOAT;
        }
    }

    @Override
	public boolean isNull() {
    	return type == SqlJetValueType.NULL;
    }
    
    @Override
    public boolean isInt() {
    	return type == SqlJetValueType.INTEGER;
    }
    
    @Override
    public boolean isReal() {
    	return type == SqlJetValueType.FLOAT;
    }
    
    @Override
    public boolean isNumber() {
    	return isInt() || isReal();
    }
    
    @Override
    public boolean isString() {
    	return type == SqlJetValueType.TEXT;
    }
    
    @Override
    public boolean isBlob() {
    	return type == SqlJetValueType.BLOB;
    }

    @Override
	public SqlJetValueType getType() {
        return type;
    }

	@Override
	public ISqlJetMemoryPointer blobValue() throws SqlJetException {
        if (isString() || isBlob()) {
            type = SqlJetValueType.BLOB;
            z.limit(n);
            return z;
        }
        return SqlJetUtility.fromString(stringValue(), SqlJetEncoding.UTF8);
    }

    /**
     * Processing is determine by the affinity parameter:
     *
     * <table>
     * <tr>
     * <td>
     * <ul>
     * <li>AFF_INTEGER:</li>
     * <li>AFF_REAL:</li>
     * <li>AFF_NUMERIC:</li>
     * </ul>
     * </td>
     * <td></td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>
     * Try to convert value to an integer representation or a floating-point
     * representation if an integer representation is not possible. Note that
     * the integer representation is always preferred, even if the affinity is
     * REAL, because an integer representation is more space efficient on disk.</td>
     * </tr>
     * <tr>
     * <td>
     * <ul>
     * <li>AFF_TEXT:</li>
     * </ul>
     * </td>
     * <td></td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>Convert value to a text representation.</td>
     * </tr>
     *
     * <tr>
     * <td>
     * <ul>
     * <li>AFF_NONE:</li>
     * </ul>
     * </td>
     * <td></td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>No-op. value is unchanged.</td>
     * </tr>
     * </table>
     *
     * @param affinity
     *            The affinity to be applied
     * @param enc
     *            Use this text encoding
     *
     * @throws SqlJetException
     */
    @Override
	public void applyAffinity(SqlJetTypeAffinity affinity, SqlJetEncoding enc) throws SqlJetException {
        if (affinity == SqlJetTypeAffinity.TEXT) {
            /*
             * Only attempt the conversion to TEXT if there is an integer or
             * real representation (blob and NULL do not get converted) but no
             * string representation.
             */
            if (!isString() && isNumber()) {
                stringify(enc);
            }
        } else if (affinity != SqlJetTypeAffinity.NONE) {
            applyNumericAffinity();
        }
    }

    @Override
	public int serialType(int file_format) {
        if (isNull()) {
            return 0;
        }
        if (isInt()) {
            /* Figure out whether to use 1, 2, 4, 6 or 8 bytes. */
            long MAX_6BYTE = ((((long) 0x00008000) << 32) - 1);
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
            if (u <= MAX_6BYTE) {
				return 5;
			}
            return 6;
        }
        if (isReal()) {
            return 7;
        }
        assert (n >= 0);
        return ((n * 2) + 12 + (isString() ? 1 : 0));
    }

    @Override
	public int serialPut(ISqlJetMemoryPointer buf, int nBuf, int file_format) {
        int serialType = this.serialType(file_format);

        /* Integer and Real */
        if (serialType <= 7 && serialType > 0) {
            long v;
            int i;
            if (serialType == 7) {
                v = Double.doubleToLongBits(this.r);
            } else {
                v = this.i;
            }
            int len = i = SqlJetVdbeSerialType.serialTypeLen(serialType);
            assert (len <= nBuf);
            while (i-- > 0) {
                buf.putByteUnsigned(i, (int) v);
                v >>>= 8;
            }
            return len;
        }

        /* String or blob */
        if (serialType >= 12) {
            assert (this.n == SqlJetVdbeSerialType.serialTypeLen(serialType));
            assert (this.n <= nBuf);
            int len = this.n;
            buf.copyFrom(this.z, len);
            return len;
        }

        /* NULL or constants 0 or 1 */
        return 0;
    }

    /**
     * Try to convert a value into a numeric representation if we can do so
     * without loss of information. In other words, if the string looks like a
     * number, convert it into a number. If it does not look like a number,
     * leave it alone.
     *
     * @throws SqlJetException
     */
    private void applyNumericAffinity() throws SqlJetException {
        if (isString()) {
            String zStr = SqlJetUtility.toString(z, enc);
			if (SqlJetUtility.isNumber(zStr)) {
                if (SqlJetUtility.isRealNumber(zStr)) {
                	setDouble(Double.parseDouble(zStr));
                } else {
                	setInt64(Long.parseLong(zStr));
                }
            }
        }
    }

    /**
     * Deserialize the data blob pointed to by buf as serial type serial_type
     * and store the result in pMem. Return the number of bytes read.
     * 
     * @param buf
     *            Buffer to deserialize from
     * @param serialType
     *            Serial type to deserialize
     * @return
     */
	public static SqlJetResultWithOffset<ISqlJetVdbeMem> serialGet(ISqlJetMemoryPointer buf, int serialType, SqlJetEncoding enc) {
        return serialGet(buf, 0, serialType, enc);
    }

	public static SqlJetResultWithOffset<ISqlJetVdbeMem> serialGet(ISqlJetMemoryPointer buf, int offset, int serialType, SqlJetEncoding enc) {
		SqlJetVdbeMem result = SqlJetVdbeMem.obtainInstance();
		
		result.enc = enc;
    	
        switch (serialType) {
        case 10: /* Reserved for future use */
        case 11: /* Reserved for future use */
        case 0:  /* NULL */
        	result.setNull();
            break;
        case 1:  /* 1-byte signed integer */
        	result.setInt64(buf.getByte(offset));
            return new SqlJetResultWithOffset<>(result, 1);
        case 2:  /* 2-byte signed integer */
        	result.setInt64(SqlJetUtility
                    .fromUnsigned((buf.getByteUnsigned(offset) << 8) | buf.getByteUnsigned(offset + 1)));
            return new SqlJetResultWithOffset<>(result, 2);
        case 3:  /* 3-byte signed integer */
        	result.setInt64((buf.getByte(offset) << 16) | (buf.getByteUnsigned(offset + 1) << 8)
                    | buf.getByteUnsigned(offset + 2));
            return new SqlJetResultWithOffset<>(result, 3);
        case 4:  /* 4-byte signed integer */
        	result.setInt64(SqlJetUtility.fromUnsigned(buf.getIntUnsigned(offset)));
            return new SqlJetResultWithOffset<>(result, 4);
        case 5: { /* 6-byte signed integer */
            long x = (buf.getByteUnsigned(offset) << 8) | buf.getByteUnsigned(offset + 1);
            long y = buf.getIntUnsigned(offset + 2);
            result.setInt64(((long) (short) x << 32) | y);
            return new SqlJetResultWithOffset<>(result, 6);
        }
        case 6: /* 8-byte signed integer */
        case 7: { /* IEEE floating point */
            long x = buf.getIntUnsigned(offset);
            long y = buf.getIntUnsigned(offset + 4);
            x = ((long) (int) x << 32) | y;
            if (serialType == 6) {
            	result.setInt64(x);
            } else {
                // assert( sizeof(x)==8 && sizeof(pMem->r)==8 );
                // swapMixedEndianFloat(x);
                // memcpy(&pMem->r, &x, sizeof(x));
                // pMem.r = ByteBuffer.allocate(8).putLong(x).getDouble();
            	result.r = Double.longBitsToDouble(x);
            	result.type = result.r == Double.NaN ? SqlJetValueType.NULL : SqlJetValueType.FLOAT;
            }
            return new SqlJetResultWithOffset<>(result, 8);
        }
        case 8: /* Integer 0 */
        case 9: /* Integer 1 */
        	result.setInt64(serialType - 8);
            return new SqlJetResultWithOffset<>(result, 0);
        default:
            int len = (serialType - 12) / 2;
            result.z = buf.pointer(offset);
            result.z.limit(len);
            result.n = len;
            if ((serialType & 0x01) != 0) {
            	result.type = SqlJetValueType.TEXT;
            } else {
            	result.type = SqlJetValueType.BLOB;
            }
            return new SqlJetResultWithOffset<>(result, len);
        }
        return new SqlJetResultWithOffset<>(result, 0);
    }

}
