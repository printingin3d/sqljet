/**
 * SqlJetIndexOrderCursor.java
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

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetIndexOrderCursor extends SqlJetTableDataCursor implements ISqlJetCursor {
    protected final ISqlJetBtreeIndexTable indexTable;

    /**
     * @param table
     * @param db
     * @throws SqlJetException
     */
    public SqlJetIndexOrderCursor(ISqlJetBtreeDataTable table, SqlJetDb db, String indexName) throws SqlJetException {
        super(table, db);
        String newIndexName = indexName != null ? indexName : table.getPrimaryKeyIndex();
        this.indexTable = (newIndexName != null) ? table.getIndexesTables().get(newIndexName) : null;
        first();
    }

    @Override
    public boolean first() throws SqlJetException {
        return db.runReadTransaction(db -> {
                if (indexTable == null) {
                    return Boolean.valueOf(SqlJetIndexOrderCursor.super.first());
                } else {
                    if (indexTable.first()) {
                        return Boolean.valueOf(firstRowNum(goTo(indexTable.getKeyRowId())));
                    }
                }
                return Boolean.FALSE;
        }).booleanValue();
    }

    @Override
    public boolean next() throws SqlJetException {
        return db.runReadTransaction(db -> {
                if (indexTable == null) {
                    return Boolean.valueOf(SqlJetIndexOrderCursor.super.next());
                } else {
                    if (indexTable.next()) {
                        return Boolean.valueOf(nextRowNum(goTo(indexTable.getKeyRowId())));
                    }
                }
                return Boolean.FALSE;
        }).booleanValue();
    }

    @Override
    public boolean eof() throws SqlJetException {
        return db.runReadTransaction(db -> {
                if (indexTable == null) {
                    return Boolean.valueOf(SqlJetIndexOrderCursor.super.eof());
                } else {
                    return Boolean.valueOf(indexTable.eof());
                }
        }).booleanValue();
    }

    @Override
    public boolean last() throws SqlJetException {
        return db.runReadTransaction(db -> {
                if (indexTable == null) {
                    return Boolean.valueOf(SqlJetIndexOrderCursor.super.last());
                } else {
                    if (indexTable.last()) {
                        return Boolean.valueOf(lastRowNum(goTo(indexTable.getKeyRowId())));
                    }
                }
                return Boolean.FALSE;
        }).booleanValue();
    }

    @Override
    public boolean previous() throws SqlJetException {
        return db.runReadTransaction(db -> {
                if (indexTable == null) {
                    return Boolean.valueOf(SqlJetIndexOrderCursor.super.previous());
                } else {
                    if (indexTable.previous()) {
                        return Boolean.valueOf(previousRowNum(goTo(indexTable.getKeyRowId())));
                    }
                }
                return Boolean.FALSE;
        }).booleanValue();
    }

    @Override
    public void delete() throws SqlJetException {
        if (indexTable != null) {
            goTo(indexTable.getKeyRowId());
        }
        super.delete();
        if (indexTable != null) {
            goTo(indexTable.getKeyRowId());
        }
    }

    @Override
    protected void computeRows(boolean current) throws SqlJetException {
        if (indexTable != null) {
            db.runReadTransaction(db -> {
                    indexTable.pushState();
                    return null;
            });
        } 
        try {
            super.computeRows(current);
        } finally {
            if (indexTable != null) {
                db.runReadTransaction(db -> {
                        indexTable.popState();
                        return null;
                });
            }
        }
    }
}
