/**
 * SqlJetTableWrapper.java
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
 */
package org.tmatesoft.sqljet.core.internal.table;

import java.util.Optional;
import java.util.Random;
import java.util.Stack;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtree;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtreeCursor;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetAssert;
import org.tmatesoft.sqljet.core.internal.SqlJetBtreeTableCreateFlags;
import org.tmatesoft.sqljet.core.internal.vdbe.SqlJetBtreeRecord;
import org.tmatesoft.sqljet.core.internal.vdbe.SqlJetKeyInfo;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetBtreeTable implements ISqlJetBtreeTable {

    protected final ISqlJetBtree btree;
    protected final int rootPage;

    protected boolean write;
    protected boolean index;

    private long priorNewRowid = 0;

    private SqlJetBtreeRecord recordCache;
    private Object[] valuesCache;

    private final Stack<State> states;

    protected static class State {

        private final ISqlJetBtreeCursor cursor;
        private final SqlJetKeyInfo keyInfo;

        public State(ISqlJetBtreeCursor cursor, SqlJetKeyInfo keyInfo) {
            this.cursor = cursor;
            this.keyInfo = keyInfo;
        }

        public ISqlJetBtreeCursor getCursor() {
            return cursor;
        }

        public SqlJetKeyInfo getKeyInfo() {
            return keyInfo;
        }

        public void close() throws SqlJetException {
            if (cursor != null) {
                cursor.closeCursor();
            }
        }
    }

    /**
     * @param db
     * @param btree
     * @param rootPage
     * @param write
     * @param index
     * @throws SqlJetException
     */
    public SqlJetBtreeTable(ISqlJetBtree btree, int rootPage, boolean write, boolean index) throws SqlJetException {
        this.states = new Stack<>();
        this.btree = btree;
        this.rootPage = rootPage;
        this.write = write;
        this.index = index;

        pushState();
        first();
    }

    private State getCurrentState() {
        assert !states.isEmpty();
        return states.peek();
    }

    protected ISqlJetBtreeCursor getCursor() {
        return getCurrentState().getCursor();
    }

    protected SqlJetKeyInfo getKeyInfo() {
        return getCurrentState().getKeyInfo();
    }

    @Override
    public void pushState() throws SqlJetException {
        SqlJetKeyInfo keyInfo = null;
        if (index) {
            keyInfo = new SqlJetKeyInfo(btree.getDb().getOptions().getEncoding());
        }
        ISqlJetBtreeCursor cursor = btree.getCursor(rootPage, write, keyInfo);
        states.push(new State(cursor, keyInfo));
        clearRecordCache();
        adjustKeyInfo();
    }

    protected void adjustKeyInfo() throws SqlJetException {
    }

    @Override
    public boolean popState() throws SqlJetException {
        if (states.size() <= 1) {
            return false;
        }
        State oldState = states.pop();
        oldState.close();
        clearRecordCache();
        return true;
    }

    @Override
    public void close() throws SqlJetException {
        for (State s : states) {
            s.close();
        }
        states.clear();

        clearRecordCache();
    }

    @Override
    public boolean eof() throws SqlJetException {
        hasMoved();
        return getCursor().eof();
    }

    @Override
    public boolean hasMoved() throws SqlJetException {
        return getCursor().cursorHasMoved();
    }

    @Override
    public boolean first() throws SqlJetException {
        clearRecordCache();
        return !getCursor().first();
    }

    @Override
    public boolean last() throws SqlJetException {
        clearRecordCache();
        return !getCursor().last();
    }

    @Override
    public boolean next() throws SqlJetException {
        clearRecordCache();
        hasMoved();
        return !getCursor().next();
    }

    @Override
    public boolean previous() throws SqlJetException {
        clearRecordCache();
        hasMoved();
        return !getCursor().previous();
    }

    @Override
    public ISqlJetBtreeRecord getRecord() throws SqlJetException {
        if (eof()) {
            return null;
        }
        if (null == recordCache) {
            recordCache = new SqlJetBtreeRecord(getCursor(), index, btree.getDb().getOptions().getFileFormat());
        }
        return recordCache;
    }

    @Override
    public @Nonnull SqlJetEncoding getEncoding() throws SqlJetException {
        return getCursor().getCursorDb().getOptions().getEncoding();
    }

    protected static boolean checkField(ISqlJetBtreeRecord record, int field) throws SqlJetException {
        return field >= 0 && record != null && field < record.getFieldsCount();
    }

    protected Optional<ISqlJetVdbeMem> getValueMem(int field) throws SqlJetException {
        final ISqlJetBtreeRecord r = getRecord();
        if (!checkField(r, field)) {
            return Optional.empty();
        }
        return Optional.of(r.getRawField(field));
    }

    @Override
    public Object getValue(int field) throws SqlJetException {
        return getValueMem(field).map(ISqlJetVdbeMem::toObject).orElse(null);
    }

    @Override
    public int getFieldsCount() throws SqlJetException {
        final ISqlJetBtreeRecord r = getRecord();
        if (null == r) {
            return 0;
        }
        return r.getFieldsCount();
    }

    @Override
    public boolean isNull(int field) throws SqlJetException {
        return getValueMem(field).map(ISqlJetVdbeMem::isNull).orElse(Boolean.TRUE).booleanValue();
    }

    @Override
    public String getString(int field) throws SqlJetException {
        return getValueMem(field).map(ISqlJetVdbeMem::stringValue).orElse(null);
    }

    @Override
    public long getInteger(int field) throws SqlJetException {
        return getValueMem(field).map(ISqlJetVdbeMem::intValue).orElse(Long.valueOf(0)).longValue();
    }

    @Override
    public double getFloat(int field) throws SqlJetException {
        return getValueMem(field).map(ISqlJetVdbeMem::realValue).orElse(Double.valueOf(0.0)).doubleValue();
    }

    @Override
    public SqlJetValueType getFieldType(int field) throws SqlJetException {
        return getValueMem(field).map(ISqlJetVdbeMem::getType).orElse(SqlJetValueType.NULL);
    }

    @Override
    public Optional<ISqlJetMemoryPointer> getBlob(int field) throws SqlJetException {
        return getValueMem(field).map(ISqlJetVdbeMem::blobValue);
    }

    @Override
    public @Nonnull Object[] getValues() throws SqlJetException {
        if (valuesCache != null) {
            return valuesCache;
        }
        final ISqlJetBtreeRecord record = getRecord();
        final int fieldsCount = record.getFieldsCount();
        Object[] values = new Object[fieldsCount];
        for (int i = 0; i < fieldsCount; i++) {
            values[i] = getValue(i);
        }
        this.valuesCache = values;
        return values;
    }

    @Override
    public long newRowId() throws SqlJetException {
        return newRowId(0);
    }

    /**
     * Get a new integer record number (a.k.a "rowid") used as the key to a
     * table. The record number is not previously used as a key in the database
     * table that cursor P1 points to. The new record number is written written
     * to register P2.
     * 
     * Prev is the largest previously generated record number. No new record
     * numbers are allowed to be less than this value. When this value reaches
     * its maximum, a SQLITE_FULL error is generated. This mechanism is used to
     * help implement the AUTOINCREMENT feature.
     * 
     * @param prev
     * @return
     * @throws SqlJetException
     */
    @Override
    public long newRowId(long prev) throws SqlJetException {
        /*
         * The next rowid or record number (different terms for the same thing)
         * is obtained in a two-step algorithm. First we attempt to find the
         * largest existing rowid and add one to that. But if the largest
         * existing rowid is already the maximum positive integer, we have to
         * fall through to the second probabilistic algorithm. The second
         * algorithm is to select a rowid at random and see if it already exists
         * in the table. If it does not exist, we have succeeded. If the random
         * rowid does exist, we select a new one and try again, up to 1000
         * times.For a table with less than 2 billion entries, the probability
         * of not finding a unused rowid is about 1.0e-300. This is a non-zero
         * probability, but it is still vanishingly small and should never cause
         * a problem. You are much, much more likely to have a hardware failure
         * than for this algorithm to fail.
         * 
         * To promote locality of reference for repetitive inserts, the first
         * few attempts at choosing a random rowid pick values just a little
         * larger than the previous rowid. This has been shown experimentally to
         * double the speed of the COPY operation.
         */

        int flags = getCursor().flags();
        SqlJetAssert.assertTrue(SqlJetBtreeTableCreateFlags.INTKEY.hasFlag(flags)
                && !SqlJetBtreeTableCreateFlags.ZERODATA.hasFlag(flags), SqlJetErrorCode.CORRUPT);

        boolean useRandomRowid = false;
        long v = 0;
        int res = 0;
        int cnt = 0;

        long MAX_ROWID = 0x7fffffff;

        final boolean last = getCursor().last();

        if (last) {
            v = 1;
        } else {
            v = getCursor().getKeySize();
            if (v == MAX_ROWID) {
                useRandomRowid = true;
            } else {
                v++;
            }

            if (prev != 0) {
                if (prev == MAX_ROWID || useRandomRowid) {
                    throw new SqlJetException(SqlJetErrorCode.FULL);
                }
                if (v < prev) {
                    v = prev + 1;
                }
            }

            if (useRandomRowid) {
                v = priorNewRowid;
                Random random = new Random();
                /* SQLITE_FULL must have occurred prior to this */
                assert prev == 0;
                cnt = 0;
                do {
                    if (cnt == 0 && (v & 0xffffff) == v) {
                        v++;
                    } else {
                        v = random.nextInt();
                        if (cnt < 5) {
                            v &= 0xffffff;
                        }
                    }
                    if (v == 0) {
                        continue;
                    }
                    res = getCursor().moveToUnpacked(null, v, false);
                    cnt++;
                } while (cnt < 100 && res == 0);
                priorNewRowid = v;
                if (res == 0) {
                    throw new SqlJetException(SqlJetErrorCode.FULL);
                }
            }
        }
        return v;
    }

    protected void clearRecordCache() {
        recordCache = null;
        valuesCache = null;
    }

    @Override
    public void clear() throws SqlJetException {
        btree.clearTable(rootPage, null);
    }

    @Override
    public long getKeySize() throws SqlJetException {
        return getCursor().getKeySize();
    }

    @Override
    public int moveTo(ISqlJetMemoryPointer pKey, long nKey, boolean bias) throws SqlJetException {
        clearRecordCache();
        return getCursor().moveTo(pKey, nKey, bias);
    }

    /**
     * @param object
     * @param rowId
     * @param pData
     * @param remaining
     * @param i
     * @param b
     * @throws SqlJetException
     */
    @Override
    public void insert(ISqlJetMemoryPointer pKey, long nKey, ISqlJetMemoryPointer pData, int nData, int nZero,
            boolean bias) throws SqlJetException {
        clearRecordCache();
        getCursor().insert(pKey, nKey, pData, nData, nZero, bias);
    }

    /**
     * @throws SqlJetException
     * 
     */
    @Override
    public void delete() throws SqlJetException {
        clearRecordCache();
        getCursor().delete();
    }
}
