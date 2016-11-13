/**
 * SqlJetMapTableCursor.java
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
package org.tmatesoft.sqljet.core.internal.map;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtree;
import org.tmatesoft.sqljet.core.internal.SqlJetAssert;
import org.tmatesoft.sqljet.core.map.ISqlJetMapCursor;
import org.tmatesoft.sqljet.core.map.ISqlJetMapIndexCursor;
import org.tmatesoft.sqljet.core.map.ISqlJetMapTableCursor;
import org.tmatesoft.sqljet.core.map.SqlJetMapDb;
import org.tmatesoft.sqljet.core.schema.ISqlJetIndexDef;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetMapCursor implements ISqlJetMapCursor {

    private final SqlJetMapDb mapDb;
    private final ISqlJetBtree btree;
    private final SqlJetMapDef mapDef;
    private final boolean writable;
    private ISqlJetMapTableCursor mapTable;
    private ISqlJetMapIndexCursor mapIndex;

    /**
     * @param mapDb
     * @param btree
     * @param mapDef
     * @param writable
     * @throws SqlJetException 
     */
    public SqlJetMapCursor(final SqlJetMapDb mapDb, ISqlJetBtree btree, SqlJetMapDef mapDef, boolean writable) throws SqlJetException {
    	SqlJetAssert.assertTrue(mapDb.isInTransaction(), SqlJetErrorCode.MISUSE, "Cursor requires active transaction");
    	
        this.mapDb = mapDb;
        this.btree = btree;
        this.mapDef = mapDef;
        this.writable = writable;
    }

    /**
     * @return
     * @throws SqlJetException
     */
    @Override
	public synchronized ISqlJetMapTableCursor getMapTable() throws SqlJetException {
        if (mapTable == null) {
            mapTable = new SqlJetMapTableCursor(mapDb, btree, mapDef, writable);
        }
        return mapTable;
    }

    @Override
	public synchronized ISqlJetMapIndexCursor getMapIndex() throws SqlJetException {
        if (mapIndex == null) {
            final ISqlJetIndexDef indexDef = mapDef.getIndexDef();
            mapIndex = new SqlJetMapIndexCursor(mapDb, btree, indexDef, writable);
        }
        return mapIndex;
    }

    /**
     * @throws SqlJetException
     */
    @Override
	public synchronized void close() throws SqlJetException {
        if (mapTable != null) {
            mapTable.close();
            mapTable = null;
        }
        if (mapIndex != null) {
            mapIndex.close();
            mapIndex = null;
        }
    }

    /**
     * @return
     * @throws SqlJetException
     */
    @Override
	public Object[] getKey() throws SqlJetException {
        return getMapIndex().getKey();
    }

    /**
     * @return
     * @throws SqlJetException
     */
    @Override
	public Object[] getValue() throws SqlJetException {
        return getMapTable().getValue();
    }

    /**
     * @param key
     * @return
     * @throws SqlJetException
     */
    @Override
	public boolean goToKey(Object[] key) throws SqlJetException {
        if (getMapIndex().goToKey(key)) {
            return getMapTable().goToKey(getMapIndex().getValue().longValue());
        } else {
            return false;
        }
    }

    /**
     * @param key
     * @param value
     * 
     * @throws SqlJetException
     */
    @Override
	public void put(Object[] key, Object[] value) throws SqlJetException {
        getMapIndex().put(key, Long.valueOf(getMapTable().put(0, value)));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.map.ISqlJetMapIterator#eof()
     */
    @Override
	public boolean eof() throws SqlJetException {
        return getMapIndex().eof();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.map.ISqlJetMapIterator#first()
     */
    @Override
	public boolean first() throws SqlJetException {
        return getMapIndex().first();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.map.ISqlJetMapIterator#last()
     */
    @Override
	public boolean last() throws SqlJetException {
        return getMapIndex().last();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.map.ISqlJetMapIterator#next()
     */
    @Override
	public boolean next() throws SqlJetException {
        return getMapIndex().next();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.map.ISqlJetMapIterator#previous()
     */
    @Override
	public boolean previous() throws SqlJetException {
        return getMapIndex().previous();
    }

}
