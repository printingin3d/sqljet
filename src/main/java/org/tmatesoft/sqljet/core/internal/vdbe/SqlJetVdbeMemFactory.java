package org.tmatesoft.sqljet.core.internal.vdbe;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetResultWithOffset;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;

public class SqlJetVdbeMemFactory {
	
	public static ISqlJetVdbeMem getNull() {
		SqlJetVdbeMem result = SqlJetVdbeMem.obtainInstance();
		result.setNull();
		return result;
	}
	
	public static ISqlJetVdbeMem getInt(long value) {
		SqlJetVdbeMem result = SqlJetVdbeMem.obtainInstance();
		result.setInt64(value);
		return result;
	}
	
	public static ISqlJetVdbeMem getDouble(double value) {
		SqlJetVdbeMem result = SqlJetVdbeMem.obtainInstance();
		result.setDouble(value);
		return result;
	}
	
	public static ISqlJetVdbeMem getStr(String value, SqlJetEncoding enc) throws SqlJetException {
		return getStr(SqlJetUtility.fromString(value, enc), enc);
	}
	
	public static ISqlJetVdbeMem getStr(ISqlJetMemoryPointer z, SqlJetEncoding enc) {
		SqlJetVdbeMem result = SqlJetVdbeMem.obtainInstance();
		result.setStr(z, enc);
		return result;
	}
	
	public static ISqlJetVdbeMem getBlob(ISqlJetMemoryPointer z, SqlJetEncoding enc) {
		SqlJetVdbeMem result = SqlJetVdbeMem.obtainInstance();
		result.setBlob(z, enc);
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

	public static SqlJetResultWithOffset<ISqlJetVdbeMem> serialGet(ISqlJetMemoryPointer buf, int offset, int serialType, SqlJetEncoding enc) {
		ISqlJetVdbeMem result;
    	
        switch (serialType) {
        case 10: /* Reserved for future use */
        case 11: /* Reserved for future use */
        case 0:  /* NULL */
        	result = getNull();
            break;
        case 1:  /* 1-byte signed integer */
        	result = getInt(buf.getByte(offset));
            return new SqlJetResultWithOffset<>(result, 1);
        case 2:  /* 2-byte signed integer */
        	result = getInt(SqlJetUtility
                    .fromUnsigned((buf.getByteUnsigned(offset) << 8) | buf.getByteUnsigned(offset + 1)));
            return new SqlJetResultWithOffset<>(result, 2);
        case 3:  /* 3-byte signed integer */
        	result = getInt((buf.getByte(offset) << 16) | (buf.getByteUnsigned(offset + 1) << 8)
                    | buf.getByteUnsigned(offset + 2));
            return new SqlJetResultWithOffset<>(result, 3);
        case 4:  /* 4-byte signed integer */
        	result = getInt(SqlJetUtility.fromUnsigned(buf.getIntUnsigned(offset)));
            return new SqlJetResultWithOffset<>(result, 4);
        case 5: { /* 6-byte signed integer */
            long x = (buf.getByteUnsigned(offset) << 8) | buf.getByteUnsigned(offset + 1);
            long y = buf.getIntUnsigned(offset + 2);
        	result = getInt(((long) (short) x << 32) | y);
            return new SqlJetResultWithOffset<>(result, 6);
        }
        case 6: /* 8-byte signed integer */
        case 7: { /* IEEE floating point */
            long x = buf.getIntUnsigned(offset);
            long y = buf.getIntUnsigned(offset + 4);
            x = ((long) (int) x << 32) | y;
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
        case 9: /* Integer 1 */
        	result = getInt(serialType - 8);
            return new SqlJetResultWithOffset<>(result, 0);
        default:
            int len = (serialType - 12) / 2;
            ISqlJetMemoryPointer pointer = buf.pointer(offset);
            pointer.limit(len);
            if ((serialType & 0x01) != 0) {
            	result = getStr(pointer, enc);
            } else {
            	result = getBlob(pointer, enc);
            }
            return new SqlJetResultWithOffset<>(result, len);
        }
        return new SqlJetResultWithOffset<>(result, 0);
    }

}
