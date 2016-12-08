/**
 * SqlJetRawTable.java
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

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtreeCursor;
import org.tmatesoft.sqljet.core.internal.ISqlJetLimits;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.internal.memory.SqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.memory.SqlJetVarintResult32;
import org.tmatesoft.sqljet.core.internal.table.ISqlJetBtreeRecord;
import org.tmatesoft.sqljet.core.table.ISqlJetOptions;

/**
 * Implements {@link ISqlJetBtreeRecord}.
 * 
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetBtreeRecord implements ISqlJetBtreeRecord {

    private final ISqlJetBtreeCursor cursor;
    private final boolean isIndex;

    private final List<Integer> aType = new ArrayList<Integer>();
    private final List<Integer> aOffset = new ArrayList<Integer>();
    private final List<ISqlJetVdbeMem> fields = new ArrayList<ISqlJetVdbeMem>();

    private final int fileFormat;

	public SqlJetBtreeRecord(ISqlJetBtreeCursor cursor, boolean isIndex, int fileFormat) throws SqlJetException {
        this.cursor = cursor;
        this.isIndex = isIndex;
        this.fileFormat = fileFormat;
        read();
    }
    
    /**
     * @return the fields
     */
    @Override
    public List<ISqlJetVdbeMem> getFields() {
    	return Collections.unmodifiableList(fields);
    }

    public SqlJetBtreeRecord(List<ISqlJetVdbeMem> values) {
    	this.cursor = null;
        this.isIndex = false;
        this.fileFormat = ISqlJetOptions.SQLJET_DEFAULT_FILE_FORMAT;
        fields.addAll(values);
    }

    public SqlJetBtreeRecord(ISqlJetVdbeMem... values) {
    	this.cursor = null;
        this.isIndex = false;
        this.fileFormat = ISqlJetOptions.SQLJET_DEFAULT_FILE_FORMAT;
        fields.addAll(Arrays.asList(values));
    }

    public static ISqlJetBtreeRecord getRecord(SqlJetEncoding encoding, Object... values) throws SqlJetException {
        final List<ISqlJetVdbeMem> fields = new ArrayList<ISqlJetVdbeMem>(values.length);
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            final ISqlJetVdbeMem mem = SqlJetVdbeMem.obtainInstance();
            if (null == value) {
                mem.setNull();
            } else if (value instanceof String) {
                mem.setStr(SqlJetUtility.fromString((String) value, encoding), encoding);
            } else if (value instanceof Boolean) {
                mem.setInt64(((Boolean) value).booleanValue() ? 1 : 0);
            } else if (value instanceof Float) {
            	mem.setDouble(((Float) value).doubleValue());
            } else if (value instanceof Double) {
            	mem.setDouble(((Double) value).doubleValue());
            } else if (value instanceof Number) {
                mem.setInt64(((Number) value).longValue());
            } else if (value instanceof ByteBuffer) {
                mem.setBlob(SqlJetUtility.fromByteBuffer((ByteBuffer) value), encoding);
            } else if (value instanceof InputStream) {
                mem.setBlob(SqlJetUtility.streamToBuffer((InputStream) value), encoding);
            } else if ("byte[]".equalsIgnoreCase(value.getClass().getCanonicalName())) {
                mem.setBlob(SqlJetUtility.wrapPtr((byte[]) value), encoding);
            } else if (value instanceof SqlJetMemoryPointer) {
                mem.setBlob((SqlJetMemoryPointer) value, encoding);
            } else {
                throw new SqlJetException(SqlJetErrorCode.MISUSE, "Bad value #" + i + " " + value.toString());
            }
            fields.add(mem);
        }
        return new SqlJetBtreeRecord(fields);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.tmatesoft.sqljet.core.internal.vdbe.ISqlJetRecord#getFieldsCount()
     */
    @Override
	public int getFieldsCount() {
        return fields.size();
    }

    /**
     * Read and parse the table header. Store the results of the parse into the
     * record header cache fields of the cursor.
     * 
     * @throws SqlJetException
     */
    private void read() throws SqlJetException {
        cursor.enterCursor();
        try {
            /*
             * This block sets the variable payloadSize to be the total number
             * of bytes in the record.
             */
        	long payloadSize = isIndex ? cursor.getKeySize() : cursor.getDataSize(); /* Number of bytes in the record */
        	
            /* If payloadSize is 0, then just store a NULL */
            if (payloadSize == 0) {
                return;
            }

            /* For storing the record being decoded */
            SqlJetVdbeMem sMem = SqlJetVdbeMem.obtainInstance();

            int[] avail = { 0 }; /* Number of bytes of available data */

            /* Figure out how many bytes are in the header */
            ISqlJetMemoryPointer zData = isIndex ? cursor.keyFetch(avail) : cursor.dataFetch(avail); /* Part of the record being decoded */
            /*
             * The following assert is true in all cases accept when* the
             * database file has been corrupted externally.* assert( zRec!=0 ||
             * avail>=payloadSize || avail>=9 );
             */
            SqlJetVarintResult32 res = zData.getVarint32();
            int offset = res.getValue(); /* Offset into the data */
            int szHdrSz = res.getOffset(); /* Size of the header size field at start of record */

            /*
             * The KeyFetch() or DataFetch() above are fast and will get the
             * entire* record header in most cases. But they will fail to get
             * the complete* record header if the record header does not fit on
             * a single page* in the B-Tree. When that happens, use
             * sqlite3VdbeMemFromBtree() to* acquire the complete header text.
             */
            if (avail[0] < offset) {
                sMem.fromBtree(cursor, 0, offset, isIndex);
                zData = sMem.z;
            }
            ISqlJetMemoryPointer zEndHdr = zData.pointer(offset); /* Pointer to first byte after the header */
            ISqlJetMemoryPointer zIdx = zData.pointer(szHdrSz); /* Index into header */

            /*
             * Scan the header and use it to fill in the aType[] and aOffset[]*
             * arrays. aType[i] will contain the type integer for the i-th*
             * column and aOffset[i] will contain the offset from the beginning*
             * of the record to the start of the data for the i-th column
             */
            for (int i = 0; i < ISqlJetLimits.SQLJET_MAX_COLUMN && zIdx.getPointer() < zEndHdr.getPointer()
                    && offset <= payloadSize; i++) {
                aOffset.add(i, Integer.valueOf(offset));
                SqlJetVarintResult32 res2 = zIdx.getVarint32();
                int a = res2.getValue();
                zIdx.movePointer(res2.getOffset());
                aType.add(Integer.valueOf(a));
                offset += SqlJetVdbeSerialType.serialTypeLen(a);

                fields.add(getField(i));
            }
            sMem.release();

            /*
             * If we have read more header data than was contained in the
             * header,* or if the end of the last field appears to be past the
             * end of the* record, or if the end of the last field appears to be
             * before the end* of the record (when all fields present), then we
             * must be dealing* with a corrupt database.
             */
            if (zIdx.getPointer() > zEndHdr.getPointer() || offset > payloadSize
                    || (zIdx.getPointer() == zEndHdr.getPointer() && offset != payloadSize)) {
                throw new SqlJetException(SqlJetErrorCode.CORRUPT);
            }
        } finally {
            cursor.leaveCursor();
        }
    }

    /**
     * Opcode: Column P1 P2 P3 P4 *
     * 
     * Interpret the data that cursor P1 points to as a structure built using
     * the MakeRecord instruction. (See the MakeRecord opcode for additional
     * information about the format of the data.) Extract the P2-th column from
     * this record. If there are less that (P2+1) values in the record, extract
     * a NULL.
     * 
     * The value extracted is stored in register P3.
     * 
     * If the column contains fewer than P2 fields, then extract a NULL. Or, if
     * the P4 argument is a P4_MEM use the value of the P4 argument as the
     * result.
     * 
     * @param pCrsr
     *            The BTree cursor
     * @param fieldNum
     *            column number to retrieve
     * @param isIndex
     *            True if an index containing keys only - no data
     * @param aType
     *            Type values for all entries in the record
     * @param aOffset
     *            Cached offsets to the start of each columns data
     * @param pDest
     * @throws SqlJetException
     */
    private ISqlJetVdbeMem getField(int column) throws SqlJetException {

        long payloadSize; /* Number of bytes in the record */
        int len; /* The length of the serialized data for the column */
        ISqlJetMemoryPointer zData; /* Part of the record being decoded */
        /* For storing the record being decoded */
        SqlJetVdbeMem sMem = SqlJetVdbeMem.obtainInstance();
        SqlJetVdbeMem pDest = SqlJetVdbeMem.obtainInstance();
        pDest.flags = EnumSet.noneOf(SqlJetVdbeMemFlags.class);

        cursor.enterCursor();
        try {
            /*
             * This block sets the variable payloadSize to be the total number
             * of* bytes in the record.
             */
            if (isIndex) {
                payloadSize = cursor.getKeySize();
            } else {
                payloadSize = cursor.getDataSize();
            }

            /* If payloadSize is 0, then just store a NULL */
            if (payloadSize == 0) {
                sMem.release();
                return pDest;
            }

            /*
             * Get the column information. If aOffset[p2] is non-zero, then*
             * deserialize the value from the record. If aOffset[p2] is zero,*
             * then there are not enough fields in the record to satisfy the*
             * request. In this case, set the value NULL or to P4 if P4 is* a
             * pointer to a Mem object.
             */
            final Integer aOffsetColumn = aOffset.get(column);
            final Integer aTypeColumn = aType.get(column);
            if (aOffsetColumn != null && aTypeColumn != null && aOffsetColumn.intValue() != 0) {
                len = SqlJetVdbeSerialType.serialTypeLen(aTypeColumn.intValue());
                sMem.fromBtree(cursor, aOffset.get(column).intValue(), len, isIndex);
                zData = sMem.z;
                pDest.serialGet(zData, aTypeColumn.intValue());
                pDest.enc = cursor.getCursorDb().getOptions().getEncoding();
            }
        } finally {
            cursor.leaveCursor();
        }

        /*
         * If we dynamically allocated space to hold the data (in the
         * sqlite3VdbeMemFromBtree() call above) then transfer control of that
         * dynamically allocated space over to the pDest structure. This
         * prevents a memory copy.
         */
        if (sMem.zMalloc != null) {
            assert (sMem.z == sMem.zMalloc);
            assert (!(pDest.isBlob() || pDest.isString()) || pDest.z.getBuffer() == sMem.z.getBuffer());
            pDest.flags.remove(SqlJetVdbeMemFlags.Ephem);
            pDest.flags.remove(SqlJetVdbeMemFlags.Static);
            //pDest.z = sMem.z;
            //pDest.zMalloc = sMem.zMalloc;
        }

        pDest.makeWriteable();
        sMem.release();

        return pDest;

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetRecord#getStringField(int)
     */
    @Override
	public String getStringField(int field, SqlJetEncoding enc) throws SqlJetException {
        final ISqlJetVdbeMem f = fields.get(field);
        if (null == f) {
			return null;
		}
/*        final ISqlJetMemoryPointer v = f.valueText(enc);
        return SqlJetUtility.toString(v, enc);*/
        return f.valueString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetRecord#getIntField(int)
     */
    @Override
	public long getIntField(int field) {
        final ISqlJetVdbeMem f = fields.get(field);
        if (null == f) {
			return 0;
		}
        return f.intValue();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.tmatesoft.sqljet.core.internal.table.ISqlJetBtreeRecord#getRealField
     * (int)
     */
    @Override
	public double getRealField(int field) {
        final ISqlJetVdbeMem f = fields.get(field);
        if (null == f) {
			return 0;
		}
        return f.realValue();
    }

    /**
     * Assuming the record contains N fields, the record format looks like this:
     * 
     * <table border="1">
     * <tr>
     * <td>hdr-size</td>
     * <td>type 0</td>
     * <td>type 1</td>
     * <td>...</td>
     * <td>type N-1</td>
     * <td>data0</td>
     * <td>...</td>
     * <td>data N-1</td>
     * </tr>
     * </table>
     * 
     * Each type field is a varint representing the serial type of the
     * corresponding data element (see sqlite3VdbeSerialType()). The hdr-size
     * field is also a varint which is the offset from the beginning of the
     * record to data0.
     */
    @Override
	public ISqlJetMemoryPointer getRawRecord() {
        int nData = 0; /* Number of bytes of data space */
        int nHdr = 0; /* Number of bytes of header space */
        int nByte = 0; /* Data space required for this record */
        int nZero = 0; /* Number of zero bytes at the end of the record */
        int nVarint; /* Number of bytes in a varint */
        int serial_type; /* Type field */
        int i; /* Space used in zNewRecord[] */

        /*
         * Loop through the elements that will make up the record to figure* out
         * how much space is required for the new record.
         */
        for (ISqlJetVdbeMem value : fields) {
            serial_type = value.serialType(fileFormat);
            int len = SqlJetVdbeSerialType.serialTypeLen(serial_type);
            nData += len;
            nHdr += SqlJetUtility.varintLen(serial_type);
            if (len != 0) {
                nZero = 0;
            }
        }

        /* Add the initial header varint and total the size */
        nHdr += nVarint = SqlJetUtility.varintLen(nHdr);
        if (nVarint < SqlJetUtility.varintLen(nHdr)) {
            nHdr++;
        }
        nByte = nHdr + nData - nZero;

        /*
         * Make sure the output register has a buffer large enough to store* the
         * new record. The output register (pOp->p3) is not allowed to* be one
         * of the input registers (because the following call to*
         * sqlite3VdbeMemGrow() could clobber the value before it is used).
         */
        /* A buffer to hold the data for the new record */
        ISqlJetMemoryPointer zNewRecord = SqlJetUtility.memoryManager.allocatePtr(nByte);

        /* Write the record */
        i = zNewRecord.putVarint32(nHdr);
        for (ISqlJetVdbeMem value : fields) {
            SqlJetVdbeMem pRec = (SqlJetVdbeMem) value;
            serial_type = pRec.serialType(fileFormat);
            /* serial type */
            i += zNewRecord.pointer(i).putVarint32(serial_type);
        }
        for (ISqlJetVdbeMem value : fields) {
            /* serial data */
            i += value.serialPut(zNewRecord.pointer(i), nByte - i, fileFormat);
        }
        assert (i == nByte);

        return zNewRecord;
    }

    @Override
	public void release() {
        for (ISqlJetVdbeMem field : fields) {
            field.release();
        }
    }
}
