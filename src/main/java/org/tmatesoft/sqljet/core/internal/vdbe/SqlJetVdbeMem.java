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

import static org.tmatesoft.sqljet.core.internal.SqlJetUtility.memcpy;
import static org.tmatesoft.sqljet.core.internal.SqlJetUtility.memmove;
import static org.tmatesoft.sqljet.core.internal.SqlJetUtility.mutexHeld;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtreeCursor;
import org.tmatesoft.sqljet.core.internal.ISqlJetCallback;
import org.tmatesoft.sqljet.core.internal.ISqlJetDbHandle;
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

    /** Used when bit MEM_Zero is set in flags */
    protected int nZero;

    /** Real value */
    private double r;

    /** The associated database connection */
    protected ISqlJetDbHandle db;

    /** String or BLOB value */
    protected ISqlJetMemoryPointer z;

    /** Number of characters in string value, excluding '\0' */
    protected int n;

    /** Some combination of MEM_Null, MEM_Str, MEM_Dyn, etc. */
    protected Set<SqlJetVdbeMemFlags> flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Null);

    /** One of SQLITE_NULL, SQLITE_TEXT, SQLITE_INTEGER, etc */
    private SqlJetValueType type = SqlJetValueType.NULL;

    /** SQLITE_UTF8, SQLITE_UTF16BE, SQLITE_UTF16LE */
    protected SqlJetEncoding enc;

    /** If not null, call this function to delete Mem.z */
    private ISqlJetCallback xDel;

    /** Dynamic buffer allocated by sqlite3_malloc() */
    protected ISqlJetMemoryPointer zMalloc;

    private static final SqlJetVdbeMemPool pool = new SqlJetVdbeMemPool();
    
    public static SqlJetVdbeMem obtainInstance() {
        return pool.obtain();
    }

    protected SqlJetVdbeMem() {
        this.db = null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.internal.vdbe.ISqlJetVdbeMem#release()
     */
    @Override
	public void reset() {
        // releaseExternal();
        // sqlite3DbFree(p->db, p->zMalloc);
        z = null;
        zMalloc = null;
        xDel = null;
    }
    
    @Override
	public void release() {
        i = 0;
        nZero = 0;
        r = 0;
        db = null;
        z = null;
        n = 0;
        flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Null);
        type = SqlJetValueType.NULL;
        enc = null;
        xDel = null;
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
    public static int compare(SqlJetVdbeMem pMem1, SqlJetVdbeMem pMem2) throws SqlJetException {
        /*
         * Interchange pMem1 and pMem2 if the collating sequence specifies* DESC
         * order.
         */
        Set<SqlJetVdbeMemFlags> f1 = pMem1.flags;
        Set<SqlJetVdbeMemFlags> f2 = pMem2.flags;
        Set<SqlJetVdbeMemFlags> combinedFlags = EnumSet.copyOf(f1);
        combinedFlags.addAll(f2);

        /*
         * If one value is NULL, it is less than the other. If both values* are
         * NULL, return 0.
         */
        if (combinedFlags.contains(SqlJetVdbeMemFlags.Null)) {
            return (f2.contains(SqlJetVdbeMemFlags.Null) ? 1 : 0) - (f1.contains(SqlJetVdbeMemFlags.Null) ? 1 : 0);
        }

        /*
         * If one value is a number and the other is not, the number is less.*
         * If both are numbers, compare as reals if one is a real, or as
         * integers* if both values are integers.
         */
        if (combinedFlags.contains(SqlJetVdbeMemFlags.Int) || combinedFlags.contains(SqlJetVdbeMemFlags.Real)) {
            if (!(f1.contains(SqlJetVdbeMemFlags.Int) || f1.contains(SqlJetVdbeMemFlags.Real))) {
                return 1;
            }
            if (!(f2.contains(SqlJetVdbeMemFlags.Int) || f2.contains(SqlJetVdbeMemFlags.Real))) {
                return -1;
            }
            if (f1.contains(SqlJetVdbeMemFlags.Real) || f2.contains(SqlJetVdbeMemFlags.Real)) {
                // one is real.
                double r1, r2;
                if (!f1.contains(SqlJetVdbeMemFlags.Real)) {
                    r1 = pMem1.i;
                } else {
                    r1 = pMem1.r;
                }
                if (!f2.contains(SqlJetVdbeMemFlags.Real)) {
                    r2 = pMem2.i;
                } else {
                    r2 = pMem2.r;
                }
                if (r1 < r2) {
					return -1;
				}
                if (r1 > r2) {
					return 1;
				}
                return 0;
            } else {
                assert (f1.contains(SqlJetVdbeMemFlags.Int));
                assert (f2.contains(SqlJetVdbeMemFlags.Int));
                if (pMem1.i < pMem2.i) {
					return -1;
				}
                if (pMem1.i > pMem2.i) {
					return 1;
				}
                return 0;
            }
        }

        /*
         * If one value is a string and the other is a blob, the string is less.
         * * If both are strings, compare using the collating functions.
         */
        if (combinedFlags.contains(SqlJetVdbeMemFlags.Str)) {
            if (!f1.contains(SqlJetVdbeMemFlags.Str)) {
                return 1;
            }
            if (!f2.contains(SqlJetVdbeMemFlags.Str)) {
                return -1;
            }

            assert (pMem1.enc == pMem2.enc);
            assert (pMem1.enc.isSupported());
            /*
             * If a NULL pointer was passed as the collate function, fall
             * through to the blob case and use memcmp().
             */
        }

        /* Both values must be blobs. Compare using memcmp(). */
        int rc = SqlJetUtility.memcmp(pMem1.z, pMem2.z, (pMem1.n > pMem2.n) ? pMem2.n : pMem1.n);
        if (rc == 0) {
            rc = pMem1.n - pMem2.n;
        }
        return rc;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.ISqlJetVdbeMem#shallowCopy(org.tmatesoft.sqljet
     * .core.internal.vdbe.SqlJetVdbeMemFlags)
     */
    @Override
	public ISqlJetVdbeMem shallowCopy(SqlJetVdbeMemFlags srcType) throws SqlJetException {
        final SqlJetVdbeMem pTo = memcpy(this);
        if (this.flags.contains(SqlJetVdbeMemFlags.Dyn) || this.z == this.zMalloc) {
            pTo.flags.removeAll(SqlJetUtility.of(SqlJetVdbeMemFlags.Dyn, SqlJetVdbeMemFlags.Static,
                    SqlJetVdbeMemFlags.Ephem));
            assert (srcType == SqlJetVdbeMemFlags.Ephem || srcType == SqlJetVdbeMemFlags.Static);
            pTo.flags.add(srcType);
        }
        return pTo;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetVdbeMem#copy()
     */
    @Override
	public ISqlJetVdbeMem copy() throws SqlJetException {
        final SqlJetVdbeMem pTo = SqlJetUtility.memcpy(this);
        pTo.flags.remove(SqlJetVdbeMemFlags.Dyn);
        if (pTo.flags.contains(SqlJetVdbeMemFlags.Str) || pTo.flags.contains(SqlJetVdbeMemFlags.Blob)) {
            if (!this.flags.contains(SqlJetVdbeMemFlags.Static)) {
                pTo.flags.add(SqlJetVdbeMemFlags.Ephem);
                pTo.makeWriteable();
            }
        }
        return pTo;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.ISqlJetVdbeMem#move(org.tmatesoft.sqljet.core
     * .ISqlJetVdbeMem, org.tmatesoft.sqljet.core.ISqlJetVdbeMem)
     */
    @Override
	public ISqlJetVdbeMem move() throws SqlJetException {
        assert (db == null || mutexHeld(db.getMutex()));
        SqlJetVdbeMem pTo = SqlJetUtility.memcpy(this);
        this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Null);
        this.xDel = null;
        this.zMalloc = null;
        return pTo;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.internal.vdbe.ISqlJetVdbeMem#valueText(org.
     * tmatesoft.sqljet.core.SqlJetEncoding)
     */
    @Override
	public ISqlJetMemoryPointer valueText(SqlJetEncoding enc) throws SqlJetException {
        assert (this.db == null || mutexHeld(this.db.getMutex()));

        if (isNull()) {
            return null;
        }
        if (this.flags.contains(SqlJetVdbeMemFlags.Blob)) {
            this.flags.add(SqlJetVdbeMemFlags.Str);
        }
        this.expandBlob();
        if (this.flags.contains(SqlJetVdbeMemFlags.Str)) {
            this.changeEncoding(enc);
            /*
             * if( (enc & SQLITE_UTF16_ALIGNED)!=0 &&
             * 1==(1&SQLITE_PTR_TO_INT(pVal->z)) ){ assert( (pVal->flags &
             * (MEM_Ephem|MEM_Static))!=0 ); if(
             * sqlite3VdbeMemMakeWriteable(pVal)!=SQLITE_OK ){ return 0; } }
             */
            this.makeWriteable();
            this.nulTerminate();
        } else {
            assert (!this.flags.contains(SqlJetVdbeMemFlags.Blob));
            this.stringify(enc);
            // assert( 0==(1&SQLITE_PTR_TO_INT(pVal->z)) );
        }
        /*
         * assert(pVal->enc==(enc & ~SQLITE_UTF16_ALIGNED) || pVal->db==0 ||
         * pVal->db->mallocFailed ); if( pVal->enc==(enc &
         * ~SQLITE_UTF16_ALIGNED) ){ return pVal->z; }else{ return 0; }
         */
        return this.z;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.ISqlJetVdbeMem#stringify(org.tmatesoft.sqljet
     * .core.SqlJetEncoding)
     */
    @Override
	public void stringify(SqlJetEncoding enc) throws SqlJetException {
        final int nByte = 32;

        assert (this.db == null || mutexHeld(this.db.getMutex()));
        assert (!flags.contains(SqlJetVdbeMemFlags.Zero));
        assert (!(flags.contains(SqlJetVdbeMemFlags.Str) || flags.contains(SqlJetVdbeMemFlags.Blob)));
        assert (flags.contains(SqlJetVdbeMemFlags.Int) || flags.contains(SqlJetVdbeMemFlags.Real));

        this.grow(nByte, false);

        /*
         * For a Real or Integer, use sqlite3_mprintf() to produce the UTF-8*
         * string representation of the value. Then, if the required encoding*
         * is UTF-16le or UTF-16be do a translation.** FIX ME: It would be
         * better if sqlite3_snprintf() could do UTF-16.
         */
        if (flags.contains(SqlJetVdbeMemFlags.Int)) {
            // sqlite3_snprintf(nByte, pMem->z, "%lld", pMem->u.i);
            this.z.putBytes(Long.toString(this.i).getBytes());
        } else {
            assert (flags.contains(SqlJetVdbeMemFlags.Real));
            // sqlite3_snprintf(nByte, pMem->z, "%!.15g", pMem->r);
            this.z.putBytes(Double.toString(this.r).getBytes());
        }
        this.n = this.z.strlen30();
        this.enc = SqlJetEncoding.UTF8;
        this.flags.add(SqlJetVdbeMemFlags.Str);
        this.flags.add(SqlJetVdbeMemFlags.Term);
        this.changeEncoding(enc);
        type = SqlJetValueType.TEXT;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.internal.vdbe.ISqlJetVdbeMem#grow(int,
     * boolean)
     */
    @Override
	public void grow(int n, boolean preserve) {

        assert (1 >= ((this.zMalloc != null && this.zMalloc == this.z) ? 1 : 0)
                + ((this.flags.contains(SqlJetVdbeMemFlags.Dyn) && this.xDel != null) ? 1 : 0)
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
        if (this.flags.contains(SqlJetVdbeMemFlags.Dyn) && this.xDel != null) {
            this.xDel.call(this.z);
        }
        this.z = this.zMalloc;
        if (this.z == null) { // WTF? /sergey/
            this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Null);
        } else {
            this.flags.remove(SqlJetVdbeMemFlags.Ephem);
            this.flags.remove(SqlJetVdbeMemFlags.Static);
        }
        this.xDel = null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetVdbeMem#nulTerminate()
     */
    @Override
	public void nulTerminate() {
        assert (this.db == null || mutexHeld(this.db.getMutex()));
        if (this.flags.contains(SqlJetVdbeMemFlags.Term) || !this.flags.contains(SqlJetVdbeMemFlags.Str)) {
            return; /* Nothing to do */
        }
        this.grow(this.n + 2, true);
        this.z.putByte(this.n, (byte) 0);
        this.z.putByte(this.n + 1, (byte) 0);
        this.flags.add(SqlJetVdbeMemFlags.Term);
        this.z.limit(this.n);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.ISqlJetVdbeMem#changeEncoding(org.tmatesoft
     * .sqljet.core.SqlJetEncoding)
     */
    @Override
	public void changeEncoding(SqlJetEncoding desiredEnc) throws SqlJetException {
        assert (desiredEnc.isSupported());
        if (!this.flags.contains(SqlJetVdbeMemFlags.Str) || this.enc == desiredEnc) {
            return;
        }
        assert (this.db == null || mutexHeld(this.db.getMutex()));

        /*
         * MemTranslate() may return SQLITE_OK or SQLITE_NOMEM. If NOMEM is
         * returned, then the encoding of the value may not have changed.
         */
        this.translate(desiredEnc);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.ISqlJetVdbeMem#translate(org.tmatesoft.sqljet
     * .core.SqlJetEncoding)
     */
    @Override
	public void translate(SqlJetEncoding desiredEnc) throws SqlJetException {
        int len; /* Maximum length of output string in bytes */

        assert (this.db == null || mutexHeld(this.db.getMutex()));
        assert (this.flags.contains(SqlJetVdbeMemFlags.Str));
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
        this.flags.addAll(SqlJetUtility.of(SqlJetVdbeMemFlags.Term, SqlJetVdbeMemFlags.Dyn));
        this.z = this.zMalloc = zOut;

    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetVdbeMem#expandBlob()
     */
    @Override
	public void expandBlob() {
        if (this.flags.contains(SqlJetVdbeMemFlags.Zero)) {
            int nByte;
            assert (this.flags.contains(SqlJetVdbeMemFlags.Blob));
            assert (this.db == null || mutexHeld(this.db.getMutex()));

            /*
             * Set nByte to the number of bytes required to store the expanded
             * blob.
             */
            nByte = this.n + this.nZero;
            if (nByte <= 0) {
                nByte = 1;
            }
            this.grow(nByte, true);
            z.fill(this.n, this.nZero, (byte) 0);
            this.n += this.nZero;
            this.flags.removeAll(SqlJetUtility.of(SqlJetVdbeMemFlags.Zero, SqlJetVdbeMemFlags.Term));
        }
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
            this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Blob, SqlJetVdbeMemFlags.Dyn, SqlJetVdbeMemFlags.Term);
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
        assert (this.db == null || mutexHeld(this.db.getMutex()));
        this.expandBlob();
        if ((this.flags.contains(SqlJetVdbeMemFlags.Str) || this.flags.contains(SqlJetVdbeMemFlags.Blob))
                && this.z != this.zMalloc) {
            this.grow(this.n + 2, true);
            this.z.putByteUnsigned(this.n, (byte) 0);
            this.z.putByteUnsigned(this.n + 1, (byte) 0);
            this.flags.add(SqlJetVdbeMemFlags.Term);
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
        assert (db == null || mutexHeld(db.getMutex()));
        if (flags.contains(SqlJetVdbeMemFlags.Int)) {
            return i;
        } else if (flags.contains(SqlJetVdbeMemFlags.Real)) {
            return (long) r;
        } else if (flags.contains(SqlJetVdbeMemFlags.Str) || flags.contains(SqlJetVdbeMemFlags.Blob)) {
            /*
             * pMem->flags |= MEM_Str; if( sqlite3VdbeChangeEncoding(pMem,
             * SQLITE_UTF8) || sqlite3VdbeMemNulTerminate(pMem) ){ return 0; }
             * assert( pMem->z ); sqlite3Atoi64(pMem->z, &value);
             */
            flags.add(SqlJetVdbeMemFlags.Str);
            try {
                changeEncoding(SqlJetEncoding.UTF8);
                nulTerminate();
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
        flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Null);
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

        assert (db == null || mutexHeld(db.getMutex()));

        int nByte = z.remaining(); /* New value for pMem->n */
        /* Maximum allowed string or blob size */
        int iLimit = ISqlJetLimits.SQLJET_MAX_LENGTH;
        /* New value for pMem->flags */
        flags = EnumSet.noneOf(SqlJetVdbeMemFlags.class);

        flags.add(enc == null ? SqlJetVdbeMemFlags.Blob : SqlJetVdbeMemFlags.Str);

        if (nByte > iLimit) {
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
        flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Int);
        type = SqlJetValueType.INTEGER;
    }

    @Override
	public double realValue() {
        assert (this.db == null || this.db.getMutex().held());
        if (this.flags.contains(SqlJetVdbeMemFlags.Real)) {
            return r;
        } else if (this.flags.contains(SqlJetVdbeMemFlags.Int)) {
            return i;
        } else if (this.flags.contains(SqlJetVdbeMemFlags.Str) || this.flags.contains(SqlJetVdbeMemFlags.Blob)) {
            this.flags.add(SqlJetVdbeMemFlags.Str);
            try {
                changeEncoding(SqlJetEncoding.UTF8);
                nulTerminate();
            } catch (SqlJetException e) {
                return 0.0;
            }
            assert (this.z != null);
            return SqlJetUtility.atof(this.z);
        } else {
            return 0.0;
        }
    }

    @Override
	public void integerAffinity() {
        assert (this.flags.contains(SqlJetVdbeMemFlags.Real));
        assert (this.db == null || this.db.getMutex().held());
        this.i = (long) this.r;
        if (this.r == this.i) {
            this.flags.add(SqlJetVdbeMemFlags.Int);
            type = SqlJetValueType.INTEGER;
        }
    }

    @Override
	public void integerify() {
        assert (this.db == null || this.db.getMutex().held());
        this.i = this.intValue();
        this.setTypeFlag(SqlJetVdbeMemFlags.Int);
        type = SqlJetValueType.INTEGER;
    }

    @Override
	public void realify() {
        assert (this.db == null || this.db.getMutex().held());
        this.r = this.realValue();
        this.setTypeFlag(SqlJetVdbeMemFlags.Real);
        type = SqlJetValueType.FLOAT;
    }

    @Override
	public void numerify() {
        assert (!(flags.contains(SqlJetVdbeMemFlags.Int) || flags.contains(SqlJetVdbeMemFlags.Real) || isNull()));
        assert (this.flags.contains(SqlJetVdbeMemFlags.Str) || this.flags.contains(SqlJetVdbeMemFlags.Blob));
        assert (this.db == null || this.db.getMutex().held());
        double r1 = this.realValue();
        long i = (long) r1;
        double r2 = i;
        if (r1 == r2) {
            this.integerify();
        } else {
            this.r = r1;
            this.setTypeFlag(SqlJetVdbeMemFlags.Real);
            type = SqlJetValueType.FLOAT;
        }
    }

    @Override
	public void setTypeFlag(SqlJetVdbeMemFlags f) {
        final Iterator<SqlJetVdbeMemFlags> iterator = flags.iterator();
        while (iterator.hasNext()) {
            final SqlJetVdbeMemFlags flag = iterator.next();
            if (flag.ordinal() < SqlJetVdbeMemFlags.TypeMask.ordinal() || flag == SqlJetVdbeMemFlags.Zero) {
				iterator.remove();
			}
        }
        flags.add(f);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetVdbeMem#setZeroBlob(int)
     */
    @Override
	public void setZeroBlob(int n) {
        this.reset();
        this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Blob, SqlJetVdbeMemFlags.Zero);
        this.type = SqlJetValueType.BLOB;
        this.n = 0;
        this.nZero = Integer.max(n, 0);
        this.enc = SqlJetEncoding.UTF8;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetVdbeMem#setDouble(double)
     */
    @Override
	public void setDouble(double val) {
        if (Double.isNaN(val)) {
            this.setNull();
        } else {
            this.reset();
            this.r = val;
            this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Real);
            this.type = SqlJetValueType.FLOAT;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetVdbeMem#isTooBig()
     */
    @Override
	public boolean isTooBig() {
        assert (this.db != null);
        if (this.flags.contains(SqlJetVdbeMemFlags.Str) || this.flags.contains(SqlJetVdbeMemFlags.Blob)) {
            int n = this.n;
            if (this.flags.contains(SqlJetVdbeMemFlags.Zero)) {
                n += this.nZero;
            }
            return n > ISqlJetLimits.SQLJET_MAX_LENGTH;
        }
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetVdbeMem#sanity()
     */
    @Override
	public void sanity() {
        assert (flags != null && flags.size() > 0); /* Must define some type */
        if (flags.contains(SqlJetVdbeMemFlags.Str) || flags.contains(SqlJetVdbeMemFlags.Blob)) {
            int x = (flags.contains(SqlJetVdbeMemFlags.Static) ? 1 : 0)
                    + (flags.contains(SqlJetVdbeMemFlags.Dyn) ? 1 : 0)
                    + (flags.contains(SqlJetVdbeMemFlags.Ephem) ? 1 : 0);
            /* Strings must define a string subtype */
            /* Only one string subtype can be defined */
            assert (x == 1);
            assert (this.z != null); /* Strings must have a value */
            /* No destructor unless there is MEM_Dyn */
            assert (this.xDel == null || flags.contains(SqlJetVdbeMemFlags.Dyn));

            if (flags.contains(SqlJetVdbeMemFlags.Str)) {
                assert (this.enc.isSupported());
                /*
                 * If the string is UTF-8 encoded and nul terminated, then
                 * pMem->n* must be the length of the string. (Later:) If the
                 * database file* has been corrupted, null characters might have
                 * been inserted* into the middle of the string. In that case,
                 * the sqlite3Strlen30()* might be less.
                 */
                if (this.enc == SqlJetEncoding.UTF8 && flags.contains(SqlJetVdbeMemFlags.Term)) {
                    assert (this.z.strlen30() <= this.n);
                    assert (this.z.getByteUnsigned(this.n) == 0);
                }
            }
        } else {
            /* Cannot define a string subtype for non-string objects */
            assert (!(this.flags.contains(SqlJetVdbeMemFlags.Static) || this.flags.contains(SqlJetVdbeMemFlags.Dyn) || this.flags
                    .contains(SqlJetVdbeMemFlags.Ephem)));
            assert (this.xDel == null);
        }
        /* MEM_Null excludes all other types */
        assert (!(this.flags.contains(SqlJetVdbeMemFlags.Static) || this.flags.contains(SqlJetVdbeMemFlags.Dyn) || this.flags
                .contains(SqlJetVdbeMemFlags.Ephem)) || !isNull());
        /* If the MEM is both real and integer, the values are equal */
        assert (this.flags.contains(SqlJetVdbeMemFlags.Int) && this.flags.contains(SqlJetVdbeMemFlags.Real) && this.r == this.i);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.ISqlJetVdbeMem#valueBytes(org.tmatesoft.sqljet
     * .core.SqlJetEncoding)
     */
    @Override
	public int valueBytes(SqlJetEncoding enc) throws SqlJetException {
        if (flags.contains(SqlJetVdbeMemFlags.Blob) || valueText(enc) != null) {
            if (flags.contains(SqlJetVdbeMemFlags.Zero)) {
                return n + nZero;
            } else {
                return n;
            }
        }
        return 0;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetVdbeMem#handleBom()
     */
    @Override
	public void handleBom() {
        SqlJetEncoding bom = null;

        if (this.n < 0 || this.n > 1) {
            short b1 = (short) this.z.getByteUnsigned(0);
            short b2 = (short) this.z.getByteUnsigned(1);
            if (b1 == 0xFE && b2 == 0xFF) {
                bom = SqlJetEncoding.UTF16BE;
            }
            if (b1 == 0xFF && b2 == 0xFE) {
                bom = SqlJetEncoding.UTF16LE;
            }
        }

        if (null != bom) {
            this.makeWriteable();
            this.n -= 2;
            memmove(this.z, 0, this.z, 2, this.n);
            this.z.putByteUnsigned(this.n, (byte) 0);
            this.z.putByteUnsigned(this.n + 1, (byte) 0);
            this.flags.add(SqlJetVdbeMemFlags.Term);
            this.enc = bom;
        }
    }

    @Override
	public Set<SqlJetVdbeMemFlags> getFlags() {
        return flags;
    }

    @Override
	public boolean isNull() {
        return flags.contains(SqlJetVdbeMemFlags.Null);
    }

    @Override
	public SqlJetValueType getType() {
        return type;
    }

    @Override
	public ISqlJetMemoryPointer valueBlob() throws SqlJetException {
        if (flags.contains(SqlJetVdbeMemFlags.Str) || flags.contains(SqlJetVdbeMemFlags.Blob)) {
            expandBlob();
            flags.remove(SqlJetVdbeMemFlags.Str);
            flags.add(SqlJetVdbeMemFlags.Blob);
            z.limit(n);
            return z;
        } else {
            return valueText(SqlJetEncoding.UTF8);
        }
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
            if (!flags.contains(SqlJetVdbeMemFlags.Str)
                    && (flags.contains(SqlJetVdbeMemFlags.Real) || flags.contains(SqlJetVdbeMemFlags.Int))) {
                stringify(enc);
            }
            flags.remove(SqlJetVdbeMemFlags.Real);
            flags.remove(SqlJetVdbeMemFlags.Int);
        } else if (affinity != SqlJetTypeAffinity.NONE) {
            assert (affinity == SqlJetTypeAffinity.INTEGER || affinity == SqlJetTypeAffinity.REAL || affinity == SqlJetTypeAffinity.NUMERIC);
            applyNumericAffinity();
            /*
            if (flags.contains(SqlJetVdbeMemFlags.Real)) {
                applyIntegerAffinity();
            }
            */
        }
    }

    @Override
	public int serialType(int file_format) {
        if (isNull()) {
            return 0;
        }
        if (this.flags.contains(SqlJetVdbeMemFlags.Int)) {
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
        if (this.flags.contains(SqlJetVdbeMemFlags.Real)) {
            return 7;
        }
        int n = this.n;
        if (this.flags.contains(SqlJetVdbeMemFlags.Zero)) {
            n += this.nZero;
        }
        assert (n >= 0);
        return ((n * 2) + 12 + (this.flags.contains(SqlJetVdbeMemFlags.Str) ? 1 : 0));
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
            assert (this.n + (this.flags.contains(SqlJetVdbeMemFlags.Zero) ? this.nZero : 0) == SqlJetVdbeSerialType.serialTypeLen(serialType));
            assert (this.n <= nBuf);
            int len = this.n;
            buf.copyFrom(this.z, len);
            if (this.flags.contains(SqlJetVdbeMemFlags.Zero)) {
                len += this.nZero;
                if (len > nBuf) {
                    len = nBuf;
                }
                buf.fill(this.n, len - this.n, (byte) 0);
            }
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
        if (!flags.contains(SqlJetVdbeMemFlags.Real) && !flags.contains(SqlJetVdbeMemFlags.Int)) {
            nulTerminate();
            String zStr = SqlJetUtility.toString(z, enc);
			if (flags.contains(SqlJetVdbeMemFlags.Str) && SqlJetUtility.isNumber(zStr)) {
                changeEncoding(SqlJetEncoding.UTF8);
                if (!SqlJetUtility.isRealNumber(zStr)) {
                    i = Long.parseLong(SqlJetUtility.toString(z));
                    setTypeFlag(SqlJetVdbeMemFlags.Int);
                    type = SqlJetValueType.INTEGER;
                } else {
                    realify();
                }
            }
        } else if (type != SqlJetValueType.INTEGER && type != SqlJetValueType.FLOAT) {
            if (flags.contains(SqlJetVdbeMemFlags.Int)) {
                type = SqlJetValueType.INTEGER;
            } else if (flags.contains(SqlJetVdbeMemFlags.Real)) {
                type = SqlJetValueType.FLOAT;
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
        case 0: { /* NULL */
            this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Null);
            this.type = SqlJetValueType.NULL;
            break;
        }
        case 1: { /* 1-byte signed integer */
            this.i = buf.getByte(offset);
            this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Int);
            this.type = SqlJetValueType.INTEGER;
            return 1;
        }
        case 2: { /* 2-byte signed integer */
            this.i = SqlJetUtility
                    .fromUnsigned((buf.getByteUnsigned(offset) << 8) | buf.getByteUnsigned(offset + 1));
            this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Int);
            this.type = SqlJetValueType.INTEGER;
            return 2;
        }
        case 3: { /* 3-byte signed integer */
            this.i = (buf.getByte(offset) << 16) | (buf.getByteUnsigned(offset + 1) << 8)
                    | buf.getByteUnsigned(offset + 2);
            this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Int);
            this.type = SqlJetValueType.INTEGER;
            return 3;
        }
        case 4: { /* 4-byte signed integer */
            this.i = SqlJetUtility.fromUnsigned((long) ((buf.getByteUnsigned(offset) << 24)
                    | (buf.getByteUnsigned(offset + 1) << 16)
                    | (buf.getByteUnsigned(offset + 2) << 8) | buf.getByteUnsigned(offset + 3)));
            this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Int);
            this.type = SqlJetValueType.INTEGER;
            return 4;
        }
        case 5: { /* 6-byte signed integer */
            long x = (buf.getByteUnsigned(offset) << 8) | buf.getByteUnsigned(offset + 1);
            int y = (buf.getByteUnsigned(offset + 2) << 24)
                    | (buf.getByteUnsigned(offset + 3) << 16)
                    | (buf.getByteUnsigned(offset + 4) << 8)
                    | buf.getByteUnsigned(offset + 5);
            x = ((long) (short) x << 32) | SqlJetUtility.toUnsigned(y);
            this.i = x;
            this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Int);
            this.type = SqlJetValueType.INTEGER;
            return 6;
        }
        case 6: /* 8-byte signed integer */
        case 7: { /* IEEE floating point */
            long x = (buf.getByteUnsigned(offset) << 24)
                    | (buf.getByteUnsigned(offset + 1) << 16)
                    | (buf.getByteUnsigned(offset + 2) << 8)
                    | buf.getByteUnsigned(offset + 3);
            int y = (buf.getByteUnsigned(offset + 4) << 24)
                    | (buf.getByteUnsigned(offset + 5) << 16)
                    | (buf.getByteUnsigned(offset + 6) << 8)
                    | buf.getByteUnsigned(offset + 7);
            x = ((long) (int) x << 32) | SqlJetUtility.toUnsigned(y);
            if (serial_type == 6) {
                this.i = x;
                this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Int);
                this.type = SqlJetValueType.INTEGER;
            } else {
                // assert( sizeof(x)==8 && sizeof(pMem->r)==8 );
                // swapMixedEndianFloat(x);
                // memcpy(&pMem->r, &x, sizeof(x));
                // pMem.r = ByteBuffer.allocate(8).putLong(x).getDouble();
                this.r = Double.longBitsToDouble(x);
                this.flags = SqlJetUtility.of(this.r == Double.NaN ? SqlJetVdbeMemFlags.Null : SqlJetVdbeMemFlags.Real);
                this.type = this.r == Double.NaN ? SqlJetValueType.NULL : SqlJetValueType.FLOAT;
            }
            return 8;
        }
        case 8: /* Integer 0 */
        case 9: { /* Integer 1 */
            this.i = serial_type - 8;
            this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Int);
            this.type = SqlJetValueType.INTEGER;
            return 0;
        }
        default: {
            int len = (serial_type - 12) / 2;
            this.z = buf.pointer(offset);
            this.z.limit(len);
            this.n = len;
            this.xDel = null;
            if ((serial_type & 0x01) != 0) {
                this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Str, SqlJetVdbeMemFlags.Ephem);
                this.type = SqlJetValueType.TEXT;
            } else {
                this.flags = SqlJetUtility.of(SqlJetVdbeMemFlags.Blob, SqlJetVdbeMemFlags.Ephem);
                this.type = SqlJetValueType.BLOB;
            }
            return len;
        }
        }
        return 0;
    }

    /**
     * The MEM structure is already a MEM_Real. Try to also make it a MEM_Int if
     * we can.
     */
    void applyIntegerAffinity() {
        assert (flags.contains(SqlJetVdbeMemFlags.Real));
        assert (db == null || SqlJetUtility.mutexHeld(db.getMutex()));
        final Long l = SqlJetUtility.doubleToInt64(r);
        if (l != null) {
            i = l.longValue();
            flags.add(SqlJetVdbeMemFlags.Int);
            type = SqlJetValueType.INTEGER;
        }
    }
}
