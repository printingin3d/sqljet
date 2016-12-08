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

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtreeCursor;
import org.tmatesoft.sqljet.core.internal.ISqlJetLimits;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetCloneable;
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
    
    public static long instanceCounter = 0;

    /** Integer value. */
    private long i;

    /** Real value */
    private double r;

    /** String or BLOB value */
    protected ISqlJetMemoryPointer z;

    /** Number of characters in string value, excluding '\0' */
    protected int n;

    /** Some combination of MEM_Null, MEM_Str, MEM_Dyn, etc. */
    protected Set<SqlJetVdbeMemFlags> flags = EnumSet.noneOf(SqlJetVdbeMemFlags.class);

    /** One of SQLITE_NULL, SQLITE_TEXT, SQLITE_INTEGER, etc */
    private SqlJetValueType type = SqlJetValueType.NULL;

    /** SQLITE_UTF8, SQLITE_UTF16BE, SQLITE_UTF16LE */
    protected SqlJetEncoding enc;

    /** Dynamic buffer allocated by sqlite3_malloc() */
    protected ISqlJetMemoryPointer zMalloc;

    private static final SqlJetVdbeMemPool pool = new SqlJetVdbeMemPool();
    
    public static SqlJetVdbeMem obtainInstance() {
        return pool.obtain();
    }

    protected SqlJetVdbeMem() {
    }

	@Override
	public void reset() {
        z = null;
        zMalloc = null;
    }
    
    @Override
	public void release() {
        i = 0;
        r = 0;
        z = null;
        n = 0;
        flags = EnumSet.noneOf(SqlJetVdbeMemFlags.class);
        type = SqlJetValueType.NULL;
        enc = null;
        zMalloc = null;
        pool.release(this);
    }

    /**
     * Compare the values contained by the two memory cells, returning negative,
     * zero or positive if pMem1 is less than, equal to, or greater than pMem2.
     * Sorting order is NULL's first, followed by numbers (integers and reals)
     * sorted numerically, followed by text ordered by the collating sequence
     * pColl and finally blob's ordered by memcmp().Two NULL values are
     * considered equal by this function.
     */
    public static int compare(SqlJetVdbeMem pMem1, SqlJetVdbeMem pMem2) {
        /*
         * If one value is NULL, it is less than the other. If both values* are
         * NULL, return 0.
         */
        if (pMem1.isNull() || pMem2.isNull()) {
            return (pMem2.isNull() ? 1 : 0) - (pMem1.isNull() ? 1 : 0);
        }
        
        /*
         * If one value is a number and the other is not, the number is less.*
         * If both are numbers, compare as reals if one is a real, or as
         * integers* if both values are integers.
         */
        if (pMem1.isNumber() || pMem2.isNumber()) {
            if (!pMem1.isNumber()) {
                return 1;
            }
            if (!pMem2.isNumber()) {
                return -1;
            }
            /* Comparing to numbers as doubles */
        	double r1 = pMem1.realValue();
            double r2 = pMem2.realValue();
            return Double.compare(r1, r2);
        }

        /*
         * If one value is a string and the other is a blob, the string is less.
         * * If both are strings, compare using the collating functions.
         */
        if (pMem1.isString() || pMem2.isString()) {
            if (!pMem1.isString()) {
                return 1;
            }
            if (!pMem2.isString()) {
                return -1;
            }

            assert (pMem1.enc == pMem2.enc);
            assert (pMem1.enc.isSupported());
            /*
             * fall through to the blob case and use memcmp().
             */
        }

        /* Both values must be blobs or strings. Compare using memcmp(). */
        int rc = SqlJetUtility.memcmp(pMem1.z, pMem2.z, Integer.min(pMem1.n, pMem2.n));
        if (rc == 0) {
            rc = pMem1.n - pMem2.n;
        }
        return rc;
    }

    @Override
    public String valueString() throws SqlJetException {
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
    		return new String(z.getBytes(), enc.getCharset());
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

        this.flags.remove(SqlJetVdbeMemFlags.Ephem);
        this.flags.remove(SqlJetVdbeMemFlags.Static);

        byte[] bytes;

        /*
         * For a Real or Integer, use sqlite3_mprintf() to produce the UTF-8*
         * string representation of the value. Then, if the required encoding*
         * is UTF-16le or UTF-16be do a translation.** FIX ME: It would be
         * better if sqlite3_snprintf() could do UTF-16.
         */
        if (isInt()) {
            // sqlite3_snprintf(nByte, pMem->z, "%lld", pMem->u.i);
        	bytes = Long.toString(this.i).getBytes();
        } else {
            assert isReal();
            // sqlite3_snprintf(nByte, pMem->z, "%!.15g", pMem->r);
            bytes = Double.toString(this.r).getBytes();
        }
        this.z = this.zMalloc = SqlJetUtility.memoryManager.allocatePtr(bytes);
        this.n = bytes.length;
        this.enc = SqlJetEncoding.UTF8;
        this.flags.add(SqlJetVdbeMemFlags.Str);
        type = SqlJetValueType.TEXT;
        this.changeEncoding(enc);
    }

    /**
     * Make sure pMem->z points to a writable allocation of at least n bytes.
     * 
     * If the memory cell currently contains string or blob data and the third
     * argument passed to this function is true, the current content of the cell
     * is preserved. Otherwise, it may be discarded.
     * 
     * This function sets the MEM_Dyn flag and clears any xDel callback. It also
     * clears MEM_Ephem and MEM_Static. If the preserve flag is not set, Mem.n
     * is zeroed.
     * 
     * @param n
     * @param preserve
     */
	private void grow(int n, boolean preserve) {

        assert (1 >= ((this.zMalloc != null && this.zMalloc == this.z) ? 1 : 0)
                + (this.flags.contains(SqlJetVdbeMemFlags.Ephem) ? 1 : 0)
                + (this.flags.contains(SqlJetVdbeMemFlags.Static) ? 1 : 0));

        if (n < 32) {
			n = 32;
        /*
         * if( sqlite3DbMallocSize(pMem->db, pMem->zMalloc)<n ){ if( preserve &&
         * pMem->z==pMem->zMalloc ){ pMem->z = pMem->zMalloc =
         * sqlite3DbReallocOrFree(pMem->db, pMem->z, n); preserve = 0; }else{
         * sqlite3DbFree(pMem->db, pMem->zMalloc); pMem->zMalloc =
         * sqlite3DbMallocRaw(pMem->db, n); } }
         */
		}

        this.zMalloc = SqlJetUtility.memoryManager.allocatePtr(n);

        if (preserve && this.z != null) {
        	this.zMalloc.copyFrom(this.z, this.n);
        }
        this.z = this.zMalloc;
        this.flags.remove(SqlJetVdbeMemFlags.Ephem);
        this.flags.remove(SqlJetVdbeMemFlags.Static);
    }

    /**
     * If pMem is an object with a valid string representation, this routine
     * ensures the internal encoding for the string representation is
     * 'desiredEnc', one of SQLITE_UTF8, SQLITE_UTF16LE or SQLITE_UTF16BE.
     * 
     * If pMem is not a string object, or the encoding of the string
     * representation is already stored using the requested encoding, then this
     * routine is a no-op.
     * 
     * SQLITE_OK is returned if the conversion is successful (or not required).
     * SQLITE_NOMEM may be returned if a malloc() fails during conversion
     * between formats.
     * 
     * @param enc
     * @throws SqlJetException
     */
	private void changeEncoding(SqlJetEncoding desiredEnc) throws SqlJetException {
        assert (desiredEnc.isSupported());
        if (!isString() || this.enc == desiredEnc) {
            return;
        }

        /*
         * MemTranslate() may return SQLITE_OK or SQLITE_NOMEM. If NOMEM is
         * returned, then the encoding of the value may not have changed.
         */
        this.translate(desiredEnc);
    }

    /**
     * This routine transforms the internal text encoding used by pMem to
     * desiredEnc. It is an error if the string is already of the desired
     * encoding, or if *pMem does not contain a string value.
     * 
     * @param desiredEnc
     * @throws SqlJetException
     */
	private void translate(SqlJetEncoding desiredEnc) throws SqlJetException {
        int len; /* Maximum length of output string in bytes */

        assert isString();
        assert (this.enc != desiredEnc);
        assert (this.enc != null);
        assert (this.n >= 0);

        /*
         * If the translation is between UTF-16 little and big endian, then* all
         * that is required is to swap the byte order. This case is handled*
         * differently from the others.
         */
        if (this.enc != SqlJetEncoding.UTF8 && desiredEnc != SqlJetEncoding.UTF8) {
            this.makeWriteable();
            int zIn = 0;             /* Input iterator */
            int zTerm = this.n & ~1; /* End of input */
            while (zIn < zTerm) {
                short temp = (short) this.z.getByteUnsigned(zIn);
                this.z.putByteUnsigned(zIn, this.z.getByteUnsigned(zIn + 1));
                zIn++;
                this.z.putByteUnsigned(zIn++, temp);
            }
            this.enc = desiredEnc;
            return;
        }

        /* Set len to the maximum number of bytes required in the output buffer. */
        if (desiredEnc == SqlJetEncoding.UTF8) {
            /*
             * When converting from UTF-16, the maximum growth results from*
             * translating a 2-byte character to a 4-byte UTF-8 character.* A
             * single byte is required for the output string* nul-terminator.
             */
            this.n &= ~1;
            len = this.n * 2 + 1;
        } else {
            /*
             * When converting from UTF-8 to UTF-16 the maximum growth is caused
             * * when a 1-byte UTF-8 character is translated into a 2-byte
             * UTF-16* character. Two bytes are required in the output buffer
             * for the* nul-terminator.
             */
            len = this.n * 2 + 2;
        }

        /*
         * Set zIn to point at the start of the input buffer and zTerm to point
         * 1* byte past the end.** Variable zOut is set to point at the output
         * buffer, space obtained* from sqlite3_malloc().
         */
        ISqlJetMemoryPointer zOut = SqlJetUtility.translate(this.z, this.enc, desiredEnc); /* Output buffer */
        
        this.n = zOut.remaining();

        assert ((this.n + (desiredEnc == SqlJetEncoding.UTF8 ? 1 : 2)) <= len);

        this.reset();
        this.flags.removeAll(SqlJetUtility.of(SqlJetVdbeMemFlags.Static, SqlJetVdbeMemFlags.Dyn,
                SqlJetVdbeMemFlags.Ephem));
        this.enc = desiredEnc;
        this.flags.addAll(SqlJetUtility.of(SqlJetVdbeMemFlags.Dyn));
        this.z = this.zMalloc = zOut;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.internal.vdbe.ISqlJetVdbeMem#fromBtree(org.
     * tmatesoft.sqljet.core.ISqlJetBtreeCursor, int, int, boolean)
     */
    @Override
	public void fromBtree(ISqlJetBtreeCursor pCur, int offset, int amt, boolean key) throws SqlJetException {
        assert (mutexHeld(pCur.getCursorDb().getMutex()));

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
            this.reset();
            this.z = zData.pointer(offset);
            this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Blob, SqlJetVdbeMemFlags.Ephem);
        } else {
            this.grow(amt + 2, false);
            this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Blob, SqlJetVdbeMemFlags.Dyn);
            this.enc = null;
            this.type = SqlJetValueType.BLOB;
            try {
                if (key) {
                    pCur.key(offset, amt, this.z);
                } else {
                    pCur.data(offset, amt, this.z);
                }
            } catch (SqlJetException e) {
                this.reset();
                throw e;
            } finally {
                if (this.z != null) {
                    this.z.putByteUnsigned(amt, (byte) 0);
                    this.z.putByteUnsigned(amt + 1, (byte) 0);
                }
            }
        }
        this.n = amt;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.internal.vdbe.ISqlJetVdbeMem#makeWriteable()
     */
    @Override
	public void makeWriteable() {
        if ((isString() || isBlob()) && this.z != this.zMalloc) {
            this.grow(this.n + 2, true);
            this.z.putByteUnsigned(this.n, (byte) 0);
            this.z.putByteUnsigned(this.n + 1, (byte) 0);
            this.z.limit(this.n);
        }
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
            /*
             * pMem->flags |= MEM_Str; if( sqlite3VdbeChangeEncoding(pMem,
             * SQLITE_UTF8) || sqlite3VdbeMemNulTerminate(pMem) ){ return 0; }
             * assert( pMem->z ); sqlite3Atoi64(pMem->z, &value);
             */
            flags.add(SqlJetVdbeMemFlags.Str);
            try {
                changeEncoding(SqlJetEncoding.UTF8);
            } catch (SqlJetException e) {
                return 0;
            }
            return Long.parseLong(SqlJetUtility.toString(this.z));
        } else {
            return 0;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetVdbeMem#setNull()
     */
    @Override
	public void setNull() {
        flags = EnumSet.noneOf(SqlJetVdbeMemFlags.class);
        type = SqlJetValueType.NULL;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetVdbeMem#setStr(java.nio.ByteBuffer,
     * org.tmatesoft.sqljet.core.SqlJetEncoding)
     */
    @Override
	public void setStr(ISqlJetMemoryPointer z, SqlJetEncoding enc) throws SqlJetException {
        int nByte = z.remaining(); /* New value for pMem->n */
        /* New value for pMem->flags */
        flags = EnumSet.of(enc == null ? SqlJetVdbeMemFlags.Blob : SqlJetVdbeMemFlags.Str);

        if (nByte > ISqlJetLimits.SQLJET_MAX_LENGTH) {
            throw new SqlJetException(SqlJetErrorCode.TOOBIG);
        }

        this.z = z;
        this.n = nByte;
        this.enc = (enc == null ? SqlJetEncoding.UTF8 : enc);
        this.type = (enc == null ? SqlJetValueType.BLOB : SqlJetValueType.TEXT);

    }

    @Override
	public void setInt64(long val) {
        reset();
        i = val;
        flags = EnumSet.noneOf(SqlJetVdbeMemFlags.class);
        type = SqlJetValueType.INTEGER;
    }

    @Override
	public double realValue() {
        if (isReal()) {
            return r;
        } else if (isInt()) {
            return i;
        } else if (isString() || isBlob()) {
            this.flags.add(SqlJetVdbeMemFlags.Str);
            try {
                changeEncoding(SqlJetEncoding.UTF8);
            } catch (SqlJetException e) {
                return 0.0;
            }
            assert (this.z != null);
            return SqlJetUtility.atof(this.z);
        } else {
            return 0.0;
        }
    }

    /**
     * Convert pMem so that it is of type MEM_Real. Invalidate any prior
     * representations.
     */
	private void realify() {
		setDouble(realValue());
    }

    @Override
	public void setTypeFlag(SqlJetVdbeMemFlags f) {
        final Iterator<SqlJetVdbeMemFlags> iterator = flags.iterator();
        while (iterator.hasNext()) {
            final SqlJetVdbeMemFlags flag = iterator.next();
            if (flag.ordinal() < SqlJetVdbeMemFlags.TypeMask.ordinal()) {
				iterator.remove();
			}
        }
        if (f!=null) {
			flags.add(f);
		}
    }

    @Override
	public void setDouble(double val) {
        if (Double.isNaN(val)) {
            this.setNull();
        } else {
            this.reset();
            this.r = val;
            this.flags = EnumSet.noneOf(SqlJetVdbeMemFlags.class);
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
    	return flags.contains(SqlJetVdbeMemFlags.Str);
    }
    
    @Override
    public boolean isBlob() {
    	return flags.contains(SqlJetVdbeMemFlags.Blob);
    }

    @Override
	public SqlJetValueType getType() {
        return type;
    }

    @Override
	public ISqlJetMemoryPointer valueBlob() throws SqlJetException {
        if (isString() || isBlob()) {
            flags.remove(SqlJetVdbeMemFlags.Str);
            flags.add(SqlJetVdbeMemFlags.Blob);
            z.limit(n);
            return z;
        }
        return SqlJetUtility.fromString(valueString(), SqlJetEncoding.UTF8);
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
            assert (affinity == SqlJetTypeAffinity.INTEGER || affinity == SqlJetTypeAffinity.REAL || affinity == SqlJetTypeAffinity.NUMERIC);
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
        if (!isNumber()) {
            String zStr = SqlJetUtility.toString(z, enc);
			if (isString() && SqlJetUtility.isNumber(zStr)) {
                changeEncoding(SqlJetEncoding.UTF8);
                if (SqlJetUtility.isRealNumber(zStr)) {
                	realify();
                } else {
                	i = Long.parseLong(SqlJetUtility.toString(z));
                	setTypeFlag(null);
                	type = SqlJetValueType.INTEGER;
                }
            }
        }
    }

    @Override
	public int serialGet(ISqlJetMemoryPointer buf, int serial_type) {
        return serialGet(buf, 0, serial_type);
    }

    @Override
	public int serialGet(ISqlJetMemoryPointer buf, int offset, int serial_type) {
        switch (serial_type) {
        case 10: /* Reserved for future use */
        case 11: /* Reserved for future use */
        case 0:  /* NULL */
        	setNull();
            break;
        case 1:  /* 1-byte signed integer */
        	setInt64(buf.getByte(offset));
            return 1;
        case 2:  /* 2-byte signed integer */
        	setInt64(SqlJetUtility
                    .fromUnsigned((buf.getByteUnsigned(offset) << 8) | buf.getByteUnsigned(offset + 1)));
            return 2;
        case 3:  /* 3-byte signed integer */
        	setInt64((buf.getByte(offset) << 16) | (buf.getByteUnsigned(offset + 1) << 8)
                    | buf.getByteUnsigned(offset + 2));
            return 3;
        case 4:  /* 4-byte signed integer */
        	setInt64(SqlJetUtility.fromUnsigned(buf.getIntUnsigned(offset)));
            return 4;
        case 5: { /* 6-byte signed integer */
            long x = (buf.getByteUnsigned(offset) << 8) | buf.getByteUnsigned(offset + 1);
            long y = buf.getIntUnsigned(offset + 2);
        	setInt64(((long) (short) x << 32) | y);
            return 6;
        }
        case 6: /* 8-byte signed integer */
        case 7: { /* IEEE floating point */
            long x = buf.getIntUnsigned(offset);
            long y = buf.getIntUnsigned(offset + 4);
            x = ((long) (int) x << 32) | y;
            if (serial_type == 6) {
            	setInt64(x);
            } else {
                // assert( sizeof(x)==8 && sizeof(pMem->r)==8 );
                // swapMixedEndianFloat(x);
                // memcpy(&pMem->r, &x, sizeof(x));
                // pMem.r = ByteBuffer.allocate(8).putLong(x).getDouble();
                this.r = Double.longBitsToDouble(x);
                this.flags = EnumSet.noneOf(SqlJetVdbeMemFlags.class);
                this.type = this.r == Double.NaN ? SqlJetValueType.NULL : SqlJetValueType.FLOAT;
            }
            return 8;
        }
        case 8: /* Integer 0 */
        case 9: /* Integer 1 */
        	setInt64(serial_type - 8);
            return 0;
        default:
            int len = (serial_type - 12) / 2;
            this.z = buf.pointer(offset);
            this.z.limit(len);
            this.n = len;
            if ((serial_type & 0x01) != 0) {
                this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Str, SqlJetVdbeMemFlags.Ephem);
                this.type = SqlJetValueType.TEXT;
            } else {
                this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Blob, SqlJetVdbeMemFlags.Ephem);
                this.type = SqlJetValueType.BLOB;
            }
            return len;
        }
        return 0;
    }

}
