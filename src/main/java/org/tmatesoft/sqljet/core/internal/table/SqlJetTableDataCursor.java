/**
 * SqlJetTableDataCursor.java
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
import java.util.Map;
import java.util.Optional;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.schema.SqlJetConflictAction;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

/**
 * Implementation of cursor which allow access to all table's rows.
 *
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class SqlJetTableDataCursor extends SqlJetRowNumCursor {

    public SqlJetTableDataCursor(ISqlJetBtreeDataTable table, SqlJetDb db) throws SqlJetException {
        super(table, db);
        super.first();
    }

    protected ISqlJetBtreeDataTable getBtreeDataTable() {
        return (ISqlJetBtreeDataTable) btreeTable;
    }

    @Override
	public long getRowId() throws SqlJetException {
        return db.runReadTransaction(db -> {
                final ISqlJetBtreeDataTable table = getBtreeDataTable();
                if (table.eof()) {
                    throw new SqlJetException(SqlJetErrorCode.MISUSE,
                            "Table is empty or the current record doesn't point to a data row");
                }
                return Long.valueOf(table.getRowId());
        }).longValue();
    }

    @Override
	public boolean goTo(final long rowId) throws SqlJetException {
        return db.runReadTransaction(db -> {
                final ISqlJetBtreeDataTable table = getBtreeDataTable();
                return Boolean.valueOf(table.goToRow(rowId));
        }).booleanValue();
    }

    private int getFieldSafe(String fieldName) throws SqlJetException {
        final ISqlJetBtreeDataTable table = getBtreeDataTable();
        if (eof()) {
            throw new SqlJetException(SqlJetErrorCode.MISUSE,
                    "Table is empty or the current record doesn't point to a data row");
        }
        if (fieldName == null) {
            throw new SqlJetException(SqlJetErrorCode.MISUSE, "Field name is null");
        }
        final int field = table.getDefinition().getColumnNumber(fieldName);
        if (field < 0) {
            throw new SqlJetException(SqlJetErrorCode.MISUSE, "Field not found: " + fieldName);
        }
        return field;
    }

    @Override
	public SqlJetValueType getFieldType(final String fieldName) throws SqlJetException {
        return db.runReadTransaction(db -> getBtreeDataTable().getFieldType(getFieldSafe(fieldName)));
    }

    @Override
	public boolean isNull(final String fieldName) throws SqlJetException {
        return db.runReadTransaction(db -> Boolean.valueOf(getBtreeDataTable().isNull(getFieldSafe(fieldName)))).booleanValue();
    }

    @Override
	public String getString(final String fieldName) throws SqlJetException {
        return db.runReadTransaction(db -> getBtreeDataTable().getString(getFieldSafe(fieldName)));
    }

    @Override
	public long getInteger(final String fieldName) throws SqlJetException {
        return db.runReadTransaction(db -> {
                if (SqlJetBtreeDataTable.isFieldNameRowId(fieldName)) {
                    return Long.valueOf(getBtreeDataTable().getRowId());
                } else {
                    return Long.valueOf(getBtreeDataTable().getInteger(getFieldSafe(fieldName)));
                }
        }).longValue();
    }

    @Override
	public double getFloat(final String fieldName) throws SqlJetException {
        return db.runReadTransaction(db -> Double.valueOf(getBtreeDataTable().getFloat(getFieldSafe(fieldName)))).doubleValue();
    }

    @Override
	public Optional<byte[]> getBlobAsArray(final String fieldName) throws SqlJetException {
        return db.runReadTransaction(db -> getBtreeDataTable().getBlob(getFieldSafe(fieldName)).map(ISqlJetMemoryPointer::getBytes));
    }

    @Override
	public Optional<InputStream> getBlobAsStream(final String fieldName) throws SqlJetException {
        return db.runReadTransaction(db -> 
                getBtreeDataTable().getBlob(getFieldSafe(fieldName)).map(
                		buffer -> new ByteArrayInputStream(buffer.getBytes())));
    }

    @Override
	public Object getValue(final String fieldName) throws SqlJetException {
        return db.runReadTransaction(db -> {
                if (SqlJetBtreeDataTable.isFieldNameRowId(fieldName)) {
                    return Long.valueOf(getBtreeDataTable().getRowId());
                } else {
                    return getBtreeDataTable().getValue(getFieldSafe(fieldName));
                }
        });
    }

    @Override
	public boolean getBoolean(final String fieldName) throws SqlJetException {
        return db.runReadTransaction(db -> Boolean.valueOf(getBoolean(getFieldSafe(fieldName)))).booleanValue();
    }

    @Override
	public void update(final Object... values) throws SqlJetException {
        updateOr(null, values);
    }

    @Override
	public void updateOr(final SqlJetConflictAction onConflict, final Object... values) throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                final ISqlJetBtreeDataTable table = getBtreeDataTable();
                if (table.eof()) {
                    throw new SqlJetException(SqlJetErrorCode.MISUSE,
                            "Table is empty or current record doesn't't point to data row");
                }
                table.updateCurrent(onConflict, values);
        });
    }

    @Override
	public long updateWithRowId(final long rowId, final Object... values) throws SqlJetException {
        return updateWithRowIdOr(null, rowId, values);
    }

    @Override
	public long updateWithRowIdOr(final SqlJetConflictAction onConflict, final long rowId, final Object... values)
            throws SqlJetException {
        return db.runWriteTransaction(db -> {
                final ISqlJetBtreeDataTable table = getBtreeDataTable();
                if (table.eof()) {
                    throw new SqlJetException(SqlJetErrorCode.MISUSE,
                            "Table is empty or current record doesn't't point to data row");
                }
                return Long.valueOf(table.updateCurrentWithRowId(onConflict, rowId, values));
        }).longValue();
    }

    @Override
	public void updateByFieldNames(final Map<String, Object> values) throws SqlJetException {
        updateByFieldNamesOr(null, values);
    }

    @Override
	public void updateByFieldNamesOr(final SqlJetConflictAction onConflict, final Map<String, Object> values)
            throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                final ISqlJetBtreeDataTable table = getBtreeDataTable();
                if (table.eof()) {
                    throw new SqlJetException(SqlJetErrorCode.MISUSE,
                            "Table is empty or current record doesn't point to data row");
                }
                table.update(onConflict, values);
        });
    }

    @Override
	public void delete() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                final ISqlJetBtreeDataTable table = getBtreeDataTable();
                if (table.eof()) {
                    throw new SqlJetException(SqlJetErrorCode.MISUSE,
                            "Table is empty or current record doesn't point to data row");
                }
                table.delete();
        });
        super.delete();
    }

    @Override
	public Object[] getRowValues() throws SqlJetException {
        return db.runReadTransaction(db -> {
                Object[] values = getBtreeDataTable().getValues();
                return values.clone();
        });
    }

}