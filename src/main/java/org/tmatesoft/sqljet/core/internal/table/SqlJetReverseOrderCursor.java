/**
 * SqlJetReverseOrderCursor.java
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

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.schema.SqlJetConflictAction;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class SqlJetReverseOrderCursor implements ISqlJetCursor {

    private ISqlJetCursor cursor;
    private boolean eof;

    public SqlJetReverseOrderCursor(ISqlJetCursor cursor) throws SqlJetException {
        this.cursor = cursor;
        first();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#close()
     */
    @Override
    public void close() throws SqlJetException {
        cursor.close();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#delete()
     */
    @Override
    public void delete() throws SqlJetException {
        if (!eof) {
            cursor.delete();
            eof = cursor.eof();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#eof()
     */
    @Override
    public boolean eof() throws SqlJetException {
        return eof;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#first()
     */
    @Override
    public boolean first() throws SqlJetException {
        return !(eof = !cursor.last());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getBlobAsArray(int)
     */
    @Override
    public Optional<byte[]> getBlobAsArray(int field) throws SqlJetException {
        return cursor.getBlobAsArray(field);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.table.ISqlJetCursor#getBlobAsArray(java.lang
     * .String)
     */
    @Override
    public Optional<byte[]> getBlobAsArray(String fieldName) throws SqlJetException {
        return cursor.getBlobAsArray(fieldName);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getBlobAsStream(int)
     */
    @Override
    public Optional<InputStream> getBlobAsStream(int field) throws SqlJetException {
        return cursor.getBlobAsStream(field);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.table.ISqlJetCursor#getBlobAsStream(java.lang
     * .String)
     */
    @Override
    public Optional<InputStream> getBlobAsStream(String fieldName) throws SqlJetException {
        return cursor.getBlobAsStream(fieldName);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getBoolean(int)
     */
    @Override
    public boolean getBoolean(int field) throws SqlJetException {
        return cursor.getBoolean(field);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.table.ISqlJetCursor#getBoolean(java.lang.String
     * )
     */
    @Override
    public boolean getBoolean(String fieldName) throws SqlJetException {
        return cursor.getBoolean(fieldName);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getFieldType(int)
     */
    @Override
    public SqlJetValueType getFieldType(int field) throws SqlJetException {
        return cursor.getFieldType(field);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.table.ISqlJetCursor#getFieldType(java.lang.
     * String)
     */
    @Override
    public SqlJetValueType getFieldType(String fieldName) throws SqlJetException {
        return cursor.getFieldType(fieldName);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getFieldsCount()
     */
    @Override
    public int getFieldsCount() throws SqlJetException {
        return cursor.getFieldsCount();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getFloat(int)
     */
    @Override
    public double getFloat(int field) throws SqlJetException {
        return cursor.getFloat(field);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.table.ISqlJetCursor#getFloat(java.lang.String)
     */
    @Override
    public double getFloat(String fieldName) throws SqlJetException {
        return cursor.getFloat(fieldName);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getInteger(int)
     */
    @Override
    public long getInteger(int field) throws SqlJetException {
        return cursor.getInteger(field);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.table.ISqlJetCursor#getInteger(java.lang.String
     * )
     */
    @Override
    public long getInteger(String fieldName) throws SqlJetException {
        return cursor.getInteger(fieldName);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getRowId()
     */
    @Override
    public long getRowId() throws SqlJetException {
        return cursor.getRowId();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getString(int)
     */
    @Override
    public String getString(int field) throws SqlJetException {
        return cursor.getString(field);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.table.ISqlJetCursor#getString(java.lang.String)
     */
    @Override
    public String getString(String fieldName) throws SqlJetException {
        return cursor.getString(fieldName);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getValue(int)
     */
    @Override
    public Object getValue(int field) throws SqlJetException {
        return cursor.getValue(field);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.table.ISqlJetCursor#getValue(java.lang.String)
     */
    @Override
    public Object getValue(String fieldName) throws SqlJetException {
        return cursor.getValue(fieldName);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#goTo(long)
     */
    @Override
    public boolean goTo(long rowId) throws SqlJetException {
        return cursor.goTo(rowId);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#isNull(int)
     */
    @Override
    public boolean isNull(int field) throws SqlJetException {
        return cursor.isNull(field);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.table.ISqlJetCursor#isNull(java.lang.String)
     */
    @Override
    public boolean isNull(String fieldName) throws SqlJetException {
        return cursor.isNull(fieldName);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#last()
     */
    @Override
    public boolean last() throws SqlJetException {
        return !(eof = !cursor.first());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#next()
     */
    @Override
    public boolean next() throws SqlJetException {
        return !(eof = !cursor.previous());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#previous()
     */
    @Override
    public boolean previous() throws SqlJetException {
        return !(eof = !cursor.next());
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.table.ISqlJetCursor#update(java.lang.Object[])
     */
    @Override
    public void update(Object... values) throws SqlJetException {
        cursor.update(values);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.table.ISqlJetCursor#update(org.tmatesoft.sqljet
     * .core.schema.SqlJetConflictAction, java.lang.Object[])
     */
    @Override
    public void updateOr(SqlJetConflictAction onConflict, Object... values) throws SqlJetException {
        cursor.updateOr(onConflict, values);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.table.ISqlJetCursor#updateByFieldNames(java
     * .util.Map)
     */
    @Override
    public void updateByFieldNames(Map<String, Object> values) throws SqlJetException {
        cursor.updateByFieldNames(values);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.table.ISqlJetCursor#updateByFieldNames(org.
     * tmatesoft.sqljet.core.schema.SqlJetConflictAction, java.util.Map)
     */
    @Override
    public void updateByFieldNamesOr(SqlJetConflictAction onConflict, Map<String, Object> values)
            throws SqlJetException {
        cursor.updateByFieldNamesOr(onConflict, values);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#updateWithRowId(long,
     * java.lang.Object[])
     */
    @Override
    public long updateWithRowId(long rowId, Object... values) throws SqlJetException {
        return cursor.updateWithRowId(rowId, values);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#updateWithRowId(org.
     * tmatesoft .sqljet.core.schema.SqlJetConflictAction, long,
     * java.lang.Object[])
     */
    @Override
    public long updateWithRowIdOr(SqlJetConflictAction onConflict, long rowId, Object... values)
            throws SqlJetException {
        return cursor.updateWithRowIdOr(onConflict, rowId, values);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#reverse()
     */

    @Override
    public @Nonnull ISqlJetCursor reverse() throws SqlJetException {
        return new SqlJetReverseOrderCursor(this);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getCurrentRow()
     */
    @Override
    public long getRowIndex() throws SqlJetException {
        return cursor.getRowIndex();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getRowCount()
     */
    @Override
    public long getRowCount() throws SqlJetException {
        return cursor.getRowCount();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#goToRow(long)
     */
    @Override
    public boolean goToRow(long rowIndex) throws SqlJetException {
        return cursor.goToRow(rowIndex);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getLimit()
     */
    @Override
    public long getLimit() {
        return cursor.getLimit();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#setLimit(int)
     */
    @Override
    public void setLimit(long limit) throws SqlJetException {
        cursor.setLimit(limit);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.table.ISqlJetCursor#getRowValues()
     */
    @Override
    public @Nonnull Object[] getRowValues() throws SqlJetException {
        return cursor.getRowValues();
    }
}
