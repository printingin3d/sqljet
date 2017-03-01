/**
 * SqlJetBtreeDataTable.java
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
package org.tmatesoft.sqljet.core.internal.table;

import java.util.Set;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtree;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetAssert;
import org.tmatesoft.sqljet.core.internal.SqlJetUnpackedRecordFlags;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.internal.schema.SqlJetBaseIndexDef;
import org.tmatesoft.sqljet.core.internal.vdbe.SqlJetBtreeRecord;
import org.tmatesoft.sqljet.core.internal.vdbe.SqlJetKeyInfo;
import org.tmatesoft.sqljet.core.internal.vdbe.SqlJetUnpackedRecord;
import org.tmatesoft.sqljet.core.schema.ISqlJetIndexDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetIndexedColumn;
import org.tmatesoft.sqljet.core.schema.SqlJetSortingOrder;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetBtreeIndexTable extends SqlJetBtreeTable implements ISqlJetBtreeIndexTable {
    private final ISqlJetIndexDef indexDef;
    private final int columns;

    /**
     * Open index by name
     * 
     * @throws SqlJetException
     * 
     */
    public SqlJetBtreeIndexTable(ISqlJetBtree btree, String indexName, boolean write) throws SqlJetException {
        super(btree, ((SqlJetBaseIndexDef) btree.getSchema().getIndex(indexName)).getPage(), write, true);
        indexDef = btree.getSchema().getIndex(indexName);
        this.columns = -1;
        adjustKeyInfo();
    }

    public SqlJetBtreeIndexTable(ISqlJetBtree btree, String indexName, int columns, boolean write)
            throws SqlJetException {
        super(btree, ((SqlJetBaseIndexDef) btree.getSchema().getIndex(indexName)).getPage(), write, true);
        indexDef = btree.getSchema().getIndex(indexName);
        this.columns = columns;
        adjustKeyInfo();
    }

    /**
     * @return the indexDef
     */
    public ISqlJetIndexDef getIndexDef() {
        return indexDef;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.tmatesoft.sqljet.core.internal.table.ISqlJetBtreeIndexTable#lookup
     * (boolean, java.lang.Object[])
     */
    @Override
	public long lookup(@Nonnull Object... values) throws SqlJetException {
        return lookupSafe(false, false, values);
    }

    /**
     * @param next
     * @param values
     * @return
     * @throws SqlJetException
     */
    private long lookupSafe(boolean near, boolean last, @Nonnull Object... values) throws SqlJetException {
        final SqlJetEncoding encoding = btree.getDb().getOptions().getEncoding();
        ISqlJetBtreeRecord key = SqlJetBtreeRecord.getRecord(encoding, values);
        final ISqlJetMemoryPointer k = key.getRawRecord();
        final int moved = cursorMoveTo(k, last);
        if (moved != 0) {
            if (!last) {
                if (moved < 0) {
                    next();
                }
            } else {
                if (moved > 0) {
                    previous();
                }
            }
        }
        final ISqlJetBtreeRecord record = getRecord();
        if (null == record) {
			return 0;
		}
        if (!near && keyCompare(k, record.getRawRecord()) != 0) {
			return 0;
		}
        return getKeyRowId(record);
    }

    /**
     * @param k
     * @param last
     * @return
     * @throws SqlJetException
     */
    private int cursorMoveTo(final ISqlJetMemoryPointer pKey, boolean last) throws SqlJetException {
        clearRecordCache();
        final int nKey = pKey.remaining();
        if (!last) {
            return getCursor().moveTo(pKey, nKey, false);
        } 
        assert nKey == (long) nKey;
        SqlJetUnpackedRecord pIdxKey = getKeyInfo().recordUnpack(nKey, pKey);
        pIdxKey.getFlags().add(SqlJetUnpackedRecordFlags.INCRKEY);
        return getCursor().moveToUnpacked(pIdxKey, nKey, false);
    }

    /**
     * 
     * @param key
     * @param record
     * @return
     * 
     * @throws SqlJetException
     */
    private int keyCompare(ISqlJetMemoryPointer key, ISqlJetMemoryPointer record) throws SqlJetException {
        final SqlJetUnpackedRecord unpacked = getKeyInfo().recordUnpack(key.remaining(), key);
        final Set<SqlJetUnpackedRecordFlags> flags = unpacked.getFlags();
        flags.add(SqlJetUnpackedRecordFlags.IGNORE_ROWID);
        flags.add(SqlJetUnpackedRecordFlags.PREFIX_MATCH);
        return unpacked.recordCompare(record.remaining(), record);
    }

    @Override
	public int compareKeys(@Nonnull Object[] firstKey, @Nonnull Object[] lastKey) throws SqlJetException {
        final SqlJetEncoding encoding = btree.getDb().getOptions().getEncoding();
        final ISqlJetBtreeRecord first = SqlJetBtreeRecord.getRecord(encoding, firstKey);
        final ISqlJetBtreeRecord last = SqlJetBtreeRecord.getRecord(encoding, lastKey);
        final ISqlJetMemoryPointer firstRec = first.getRawRecord();
        final ISqlJetMemoryPointer lastRec = last.getRawRecord();
        final SqlJetUnpackedRecord unpacked = getKeyInfo().recordUnpack(firstRec.remaining(), firstRec);
        unpacked.getFlags().add(SqlJetUnpackedRecordFlags.PREFIX_MATCH);
        return unpacked.recordCompare(lastRec.remaining(), lastRec);
    }

    /**
     * @param key
     * 
     * @throws SqlJetException
     */
    @Override
	protected void adjustKeyInfo() throws SqlJetException {
    	SqlJetKeyInfo keyInfo = getKeyInfo();
		SqlJetAssert.assertNotNull(keyInfo, SqlJetErrorCode.INTERNAL);
        if(indexDef!=null) {
        	if (columns>=0) {
        		keyInfo.setNField(columns);
        	} else {
        		keyInfo.setNField(indexDef.getColumns().size());
        		int i = 0;
        		for (final ISqlJetIndexedColumn column : indexDef.getColumns()) {
        			keyInfo.setSortOrder(i++, column.getSortingOrder() == SqlJetSortingOrder.DESC);
        		}
        	}
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.tmatesoft.sqljet.core.internal.table.ISqlJetBtreeIndexTable#insert
     * (long, boolean, java.lang.Object[])
     */
    @Override
	public void insert(long rowId, boolean append, Object... key) throws SqlJetException {
        final ISqlJetBtreeRecord rec = SqlJetBtreeRecord.getRecord(btree.getDb().getOptions().getEncoding(),
                SqlJetUtility.addValueToArray(key, Long.valueOf(rowId)));
        final ISqlJetMemoryPointer zKey = rec.getRawRecord();
        getCursor().insert(zKey, zKey.remaining(), SqlJetUtility.memoryManager.allocatePtr(0), 0, 0, append);
        clearRecordCache();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.tmatesoft.sqljet.core.internal.table.ISqlJetBtreeIndexTable#delete
     * (long, java.lang.Object[])
     */
    @Override
	public boolean delete(long rowId, @Nonnull Object... key) throws SqlJetException {
        final ISqlJetBtreeRecord rec = SqlJetBtreeRecord.getRecord(btree.getDb().getOptions().getEncoding(), key);
        final ISqlJetMemoryPointer k = rec.getRawRecord();
        if (cursorMoveTo(k, false) < 0) {
            next();
        }
        do {
            final ISqlJetBtreeRecord record = getRecord();
            if (null == record) {
				return false;
			}
            if (keyCompare(k, record.getRawRecord()) != 0) {
				return false;
			}
            if (getKeyRowId(record) == rowId) {
                getCursor().delete();
                clearRecordCache();
                if (cursorMoveTo(k, false) < 0) {
                    next();
                }
                return true;
            }
        } while (next());
        return false;
    }

    private long getKeyRowId(ISqlJetBtreeRecord record) {
        if (null == record) {
			return 0;
		}
        ISqlJetVdbeMem lastRawField = record.getLastRawField();
        return lastRawField==null ? 0 : lastRawField.intValue();
    }

    @Override
	public long getKeyRowId() throws SqlJetException {
        return getKeyRowId(getRecord());
    }

    /**
     * @throws SqlJetException
     * 
     */
    public void reindex() throws SqlJetException {
        btree.clearTable(rootPage, null);
        final SqlJetBtreeDataTable dataTable = new SqlJetBtreeDataTable(btree, indexDef.getTableName(), false);
        try {
            for (dataTable.first(); !dataTable.eof(); dataTable.next()) {
                final Object[] key = dataTable.getKeyForIndex(dataTable.getValues(), indexDef);
                insert(dataTable.getRowId(), true, key);
            }
        } finally {
            dataTable.close();
        }
    }

    @Override
	public int compareKey(@Nonnull Object[] key) throws SqlJetException {
        if (eof()) {
            return 1;
        }
        final ISqlJetBtreeRecord rec = SqlJetBtreeRecord.getRecord(btree.getDb().getOptions().getEncoding(), key);
        final ISqlJetMemoryPointer keyRecord = rec.getRawRecord();
        return keyCompare(keyRecord, getRecord().getRawRecord());
    }

    @Override
	public long lookupNear(@Nonnull Object[] key) throws SqlJetException {
        return lookupSafe(true, false, key);
    }

    @Override
	public long lookupLastNear(@Nonnull Object[] key) throws SqlJetException {
        return lookupSafe(true, true, key);
    }

}
