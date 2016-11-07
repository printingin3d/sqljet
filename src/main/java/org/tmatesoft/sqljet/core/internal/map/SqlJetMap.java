/**
 * SqlJetMapTable.java
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

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtree;
import org.tmatesoft.sqljet.core.map.ISqlJetMap;
import org.tmatesoft.sqljet.core.map.ISqlJetMapCursor;
import org.tmatesoft.sqljet.core.map.ISqlJetMapIndex;
import org.tmatesoft.sqljet.core.map.ISqlJetMapTable;
import org.tmatesoft.sqljet.core.map.SqlJetMapDb;
import org.tmatesoft.sqljet.core.schema.ISqlJetIndexDef;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetMap implements ISqlJetMap {

    private final SqlJetMapDb mapDb;
    private final ISqlJetBtree btree;
    private final SqlJetMapDef mapDef;
    private boolean writable;
    private ISqlJetMapTable mapTable;
    private ISqlJetMapIndex mapIndex;

    /**
     * @param mapDb
     * @param btree
     * @param mapDef
     * @param writable
     */
    public SqlJetMap(final SqlJetMapDb mapDb, final ISqlJetBtree btree, final SqlJetMapDef mapDef,
            boolean writable) {
        this.mapDb = mapDb;
        this.btree = btree;
        this.mapDef = mapDef;
        this.writable = writable;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.map.ISqlJetMapTable#open()
     */
    @Override
	public ISqlJetMapCursor getCursor() throws SqlJetException {
        return mapDb.runSynchronized(engine -> new SqlJetMapCursor(mapDb, btree, mapDef, writable));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.map.ISqlJetMapTable#put(long,
     * java.lang.Object[])
     */
    @Override
	public void put(final Object[] key, final Object[] value) throws SqlJetException {
        mapDb.runWriteTransaction(mapDb -> {
                final ISqlJetMapCursor cursor = getCursor();
                try {
                    cursor.put(key, value);
                    return null;
                } finally {
                    cursor.close();
                }
        });
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.map.ISqlJetMapTable#get(long)
     */
    @Override
	public Object[] get(final Object[] key) throws SqlJetException {
        return mapDb.runReadTransaction(mapDb -> {
                final ISqlJetMapCursor cursor = getCursor();
                try {
                    if (cursor.goToKey(key)) {
                        return cursor.getValue();
                    } else {
                        return null;
                    }
                } finally {
                    cursor.close();
                }
        });
    }

    /**
     * @return
     * @throws SqlJetException
     */
    @Override
	public synchronized ISqlJetMapTable getMapTable() throws SqlJetException {
        if (mapTable == null) {
            mapTable = new SqlJetMapTable(mapDb, btree, mapDef, writable);
        }
        return mapTable;
    }

    @Override
	public synchronized ISqlJetMapIndex getMapIndex() throws SqlJetException {
        if (mapIndex == null) {
            final ISqlJetIndexDef indexDef = mapDef.getIndexDef();
            mapIndex = new SqlJetMapIndex(mapDb, btree, indexDef, writable);
        }
        return mapIndex;
    }

}
