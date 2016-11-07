/**
 * SqlJetTable.java
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

/**
 * Base implementation of {@link ISqlJetCursor}.
 * 
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public abstract class SqlJetCursor implements ISqlJetCursor {

    protected final ISqlJetBtreeTable btreeTable;
    protected final SqlJetDb db;

    SqlJetCursor(ISqlJetBtreeTable table, SqlJetDb db) throws SqlJetException {
        if (db.isInTransaction()) {
            this.btreeTable = table;
            this.db = db;
        } else {
            throw new SqlJetException(SqlJetErrorCode.MISUSE, "Cursor requires active transaction");
        }
    }

    @Override
	public void close() throws SqlJetException {
        db.runReadTransaction(db -> {
                btreeTable.close();
                return null;
        });
    }

    @Override
	public boolean eof() throws SqlJetException {
        return db.runReadTransaction(db -> Boolean.valueOf(btreeTable.eof())).booleanValue();
    }

    @Override
	public boolean first() throws SqlJetException {
        return db.runReadTransaction(db -> Boolean.valueOf(btreeTable.first())).booleanValue();
    }

    @Override
	public boolean last() throws SqlJetException {
        return db.runReadTransaction(db -> Boolean.valueOf(btreeTable.last())).booleanValue();
    }

    @Override
	public boolean next() throws SqlJetException {
        return db.runReadTransaction(db -> Boolean.valueOf(btreeTable.next())).booleanValue();
    }

    @Override
	public boolean previous() throws SqlJetException {
        return db.runReadTransaction(db -> Boolean.valueOf(btreeTable.previous())).booleanValue();
    }

    @Override
	public int getFieldsCount() throws SqlJetException {
        return db.runReadTransaction(db -> Integer.valueOf(btreeTable.getFieldsCount())).intValue();
    }

    @Override
	public SqlJetValueType getFieldType(final int field) throws SqlJetException {
        return db.runReadTransaction(db -> btreeTable.getFieldType(field));
    }

    @Override
	public boolean isNull(final int field) throws SqlJetException {
        return db.runReadTransaction(db -> Boolean.valueOf(btreeTable.isNull(field))).booleanValue();
    }

    @Override
	public String getString(final int field) throws SqlJetException {
        return db.runReadTransaction(db -> btreeTable.getString(field));
    }

    @Override
	public long getInteger(final int field) throws SqlJetException {
        return db.runReadTransaction(db -> Long.valueOf(btreeTable.getInteger(field))).longValue();
    }

    @Override
	public double getFloat(final int field) throws SqlJetException {
        return db.runReadTransaction(db -> Double.valueOf(btreeTable.getFloat(field))).doubleValue();
    }

    @Override
	public byte[] getBlobAsArray(final int field) throws SqlJetException {
        return db.runReadTransaction(db -> {
                ISqlJetMemoryPointer buffer = btreeTable.getBlob(field);
                return buffer != null ? SqlJetUtility.readByteBuffer(buffer) : null;
        });
    }

    @Override
	public InputStream getBlobAsStream(final int field) throws SqlJetException {
        return db.runReadTransaction(db -> {
                ISqlJetMemoryPointer buffer = btreeTable.getBlob(field);
                return buffer != null ? new ByteArrayInputStream(SqlJetUtility.readByteBuffer(buffer)) : null;
        });
    }

    @Override
	public Object getValue(final int field) throws SqlJetException {
        return db.runReadTransaction(db -> {
                Object value = btreeTable.getValue(field);
                if (value instanceof ISqlJetMemoryPointer) {
                    return new ByteArrayInputStream(SqlJetUtility.readByteBuffer((ISqlJetMemoryPointer) value));
                }
                return value;
        });
    }

    @Override
	public boolean getBoolean(final int field) throws SqlJetException {
        return db.runReadTransaction(db -> Boolean.valueOf(btreeTable.getInteger(field) != 0)).booleanValue();
    }

    @Override
	public ISqlJetCursor reverse() throws SqlJetException {
        return new SqlJetReverseOrderCursor(this);
    }

}
