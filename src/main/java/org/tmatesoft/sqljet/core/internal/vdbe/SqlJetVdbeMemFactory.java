package org.tmatesoft.sqljet.core.internal.vdbe;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtreeCursor;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetResultWithOffset;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;

public class SqlJetVdbeMemFactory {
	
	public static ISqlJetVdbeMem getNull() {
		return SqlJetVdbeMemNull.INSTANCE;
	}
	
	public static ISqlJetVdbeMem getInt(long value) {
		return new SqlJetVdbeMemInt(value);
	}
	
	public static ISqlJetVdbeMem getDouble(double value) {
		return new SqlJetVdbeMemDouble(value);
	}
	
	public static ISqlJetVdbeMem getStr(String value, SqlJetEncoding enc) {
		return new SqlJetVdbeMemString(value, enc);
	}
	
	public static ISqlJetVdbeMem getStr(ISqlJetMemoryPointer z, SqlJetEncoding enc) {
		return getStr(SqlJetUtility.toString(z, enc), enc);
	}
	
	public static ISqlJetVdbeMem getBlob(ISqlJetMemoryPointer z, SqlJetEncoding enc) {
		return new SqlJetVdbeMemBlob(z, enc);
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
        assert pCur.getCursorDb().getMutex().held();

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
        assert zData != null;

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
	
	private static final SqlJetResultWithOffset<ISqlJetVdbeMem> NULL = new SqlJetResultWithOffset<>(getNull(), 0); 
	private static final SqlJetResultWithOffset<ISqlJetVdbeMem> ZERO = new SqlJetResultWithOffset<>(getInt(0), 0); 
	private static final SqlJetResultWithOffset<ISqlJetVdbeMem> ONE = new SqlJetResultWithOffset<>(getInt(1), 0); 

	public static SqlJetResultWithOffset<ISqlJetVdbeMem> serialGet(ISqlJetMemoryPointer buf, int offset, int serialType, SqlJetEncoding enc) {
		ISqlJetVdbeMem result;
    	
        switch (serialType) {
        case 10: /* Reserved for future use */
        case 11: /* Reserved for future use */
        case 0:  /* NULL */
        	return NULL;
        case 1:  /* 1-byte signed integer */
        	result = getInt(buf.getByte(offset));
            return new SqlJetResultWithOffset<>(result, 1);
        case 2:  /* 2-byte signed integer */
        	result = getInt(SqlJetUtility
                    .fromUnsigned(buf.getByteUnsigned(offset) << 8 | buf.getByteUnsigned(offset + 1)));
            return new SqlJetResultWithOffset<>(result, 2);
        case 3:  /* 3-byte signed integer */
        	result = getInt(buf.getByte(offset) << 16 | buf.getByteUnsigned(offset + 1) << 8
                    | buf.getByteUnsigned(offset + 2));
            return new SqlJetResultWithOffset<>(result, 3);
        case 4:  /* 4-byte signed integer */
        	result = getInt(SqlJetUtility.fromUnsigned(buf.getIntUnsigned(offset)));
            return new SqlJetResultWithOffset<>(result, 4);
        case 5: { /* 6-byte signed integer */
            long x = buf.getByteUnsigned(offset) << 8 | buf.getByteUnsigned(offset + 1);
            long y = buf.getIntUnsigned(offset + 2);
        	result = getInt((long) (short) x << 32 | y);
            return new SqlJetResultWithOffset<>(result, 6);
        }
        case 6: /* 8-byte signed integer */
        case 7: { /* IEEE floating point */
            long x = buf.getIntUnsigned(offset);
            long y = buf.getIntUnsigned(offset + 4);
            x = (long) (int) x << 32 | y;
            if (serialType == 6) {
            	result = getInt(x);
            } else {
                // assert( sizeof(x)==8 && sizeof(pMem->r)==8 );
                // swapMixedEndianFloat(x);
                // memcpy(&pMem->r, &x, sizeof(x));
            	// pMem.r = ByteBuffer.allocate(8).putLong(x).getDouble();
            	double v = Double.longBitsToDouble(x);
            	result = Double.isNaN(v) ? getNull() : getDouble(v);
            }
            return new SqlJetResultWithOffset<>(result, 8);
        }
        case 8: /* Integer 0 */
        	return ZERO;
        case 9: /* Integer 1 */
        	return ONE;
        default:
            int len = (serialType - 12) / 2;
            ISqlJetMemoryPointer pointer = buf.pointer(offset, len);
            if ((serialType & 0x01) != 0) {
            	result = getStr(pointer, enc);
            } else {
            	result = getBlob(pointer, enc);
            }
            return new SqlJetResultWithOffset<>(result, len);
        }
    }

}
