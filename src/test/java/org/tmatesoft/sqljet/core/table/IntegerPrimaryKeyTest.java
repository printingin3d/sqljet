/**
 * IntegerPrimaryKeyTest.java
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
package org.tmatesoft.sqljet.core.table;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.fs.util.SqlJetFileUtil;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class IntegerPrimaryKeyTest {

    private static final Integer ONE = Integer.valueOf(1);
	private static final String ID = "id";
    private static final String ROWID = "ROWID";

    private File file;
    private SqlJetDb db;
    private ISqlJetTable table, table2;
    private Map<String, Object> values;
    private boolean success, t2;
    private long rowId = 1L;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        file = File.createTempFile("test", null);
        file.deleteOnExit();
        db = SqlJetDb.open(file, true);
        db.write().asVoid(db -> {
                db.createTable("create table t(id integer primary key);");
                db.createTable("create table t2(id integer);");
                db.createTable("create table t3(id integer, a integer, primary key(id));");
        });
        table = db.getTable("t");
        table2 = db.getTable("t2");
        values = new HashMap<>();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        try {
            if (success) {
                db.read().asVoid(db -> {
                        final ISqlJetCursor c = t2 ? table2.open() : table
                                .lookup(table.getPrimaryKeyIndexName(), Long.valueOf(rowId));
                        Assert.assertTrue(!c.eof());
                        Assert.assertEquals(rowId, c.getInteger(ID));
                        Assert.assertEquals(rowId, c.getInteger(ROWID));
                        Assert.assertEquals(Long.valueOf(rowId), c.getValue(ID));
                        Assert.assertEquals(Long.valueOf(rowId), c.getValue(ROWID));
                        Assert.assertEquals(rowId, c.getRowId());
                });
            }
        } finally {
            try {
                db.close();
            } finally {
                SqlJetFileUtil.deleteFile(file);
            }
        }
    }

    @Test
    public void integerPrimaryKey3() throws SqlJetException {
        db.write().asVoid(db -> table.insert());
        success = true;
    }

    @Test
    public void integerPrimaryKey4() throws SqlJetException {
        db.write().asVoid(db -> table.insert((Object[]) null));
        success = true;
    }

    @Test
    public void integerPrimaryKey4_1() throws SqlJetException {
        db.write().asVoid(db -> table.insert(new Object[] { null }));
        success = true;
    }

    @Test
    public void integerPrimaryKey6() throws SqlJetException {
        db.write().asVoid(db -> table.insert(ONE));
        success = true;
    }

    @Test
    public void integerPrimaryKey8() throws SqlJetException {
        rowId = 2;
        db.write().asVoid(db -> table.insert(Long.valueOf(rowId)));
        success = true;
    }

    @Test
    public void integerPrimaryKey10() throws SqlJetException {
        db.write().asVoid(db -> table.insertByFieldNames(values));
        success = true;
    }

    @Test
    public void integerPrimaryKey12() throws SqlJetException {
        values.put(ID, null);
        db.write().asVoid(db -> table.insertByFieldNames(values));
        success = true;
    }

    @Test
    public void integerPrimaryKey14() throws SqlJetException {
        values.put(ROWID, null);
        db.write().asVoid(db -> table.insertByFieldNames(values));
        success = true;
    }

    @Test
    public void integerPrimaryKey16() throws SqlJetException {
        rowId = 2;
        values.put(ID, Long.valueOf(rowId));
        db.write().asVoid(db -> table.insertByFieldNames(values));
        success = true;
    }

    @Test
    public void integerPrimaryKey18() throws SqlJetException {
        rowId = 2;
        values.put(ROWID, Long.valueOf(rowId));
        db.write().asVoid(db -> table.insertByFieldNames(values));
        success = true;
    }

    @Test
    public void integerPrimaryKey20() throws SqlJetException {
        rowId = 2;
        db.write().asVoid(db -> {
                table.insert(ONE);
                table.open().update(Integer.valueOf(2));
        });
        success = true;
    }

    @Test
    public void integerPrimaryKey21() throws SqlJetException {
        rowId = 2;
        values.put(ID, Long.valueOf(rowId));
        db.write().asVoid(db -> {
                table.insert(ONE);
                table.open().updateByFieldNames(values);
        });
        success = true;
    }

    @Test
    public void integerPrimaryKey22() throws SqlJetException {
        rowId = 2;
        values.put(ROWID, Long.valueOf(rowId));
        db.write().asVoid(db -> {
                table.insert(ONE);
                table.open().updateByFieldNames(values);
        });
        success = true;
    }

    @Test
    public void insertWithRowId1() throws SqlJetException {
        rowId = 2;
        t2 = true;
        db.write().asVoid(db -> table2.insertWithRowId(rowId, Long.valueOf(rowId)));
        success = true;
    }

    @Test
    public void insertWithRowId2() throws SqlJetException {
        rowId = 2;
        db.write().asVoid(db -> table.insertWithRowId(0, Long.valueOf(rowId)));
        success = true;
    }

    @Test
    public void insertWithRowId3() throws SqlJetException {
        db.write().asVoid(db -> table.insertWithRowId(0));
        success = true;
    }

    @Test
    public void insertWithRowId4() throws SqlJetException {
        db.write().asVoid(db -> table.insertWithRowId(0, new Object[] { null }));
        success = true;
    }

    @Test(expected = SqlJetException.class)
    public void insertWithRowId5() throws SqlJetException {
        db.write().asVoid(db -> {
                table.insertWithRowId(1);
                table.insertWithRowId(1);
        });
        success = true;
    }

    @Test
    public void insertWithRowId6() throws SqlJetException {
        t2 = true;
        db.write().asVoid(db -> table2.insertWithRowId(0, Long.valueOf(rowId)));
        success = true;
    }

    @Test(expected = SqlJetException.class)
    public void insertWithRowId7() throws SqlJetException {
        t2 = true;
        db.write().asVoid(db -> {
                table2.insertWithRowId(rowId, Long.valueOf(rowId));
                table2.insertWithRowId(rowId, Long.valueOf(rowId));
        });
        success = true;
    }

    @Test
    public void updateWithRowId1() throws SqlJetException {
        rowId = 2;
        db.write().asVoid(db -> {
                table.insert();
                table.open().updateWithRowId(rowId);
        });
        success = true;
    }

    @Test
    public void updateWithRowId2() throws SqlJetException {
        rowId = 2;
        db.write().asVoid(db -> {
                table.insert();
                table.open().updateWithRowId(rowId, ONE);
        });
        success = true;
    }

    @Test
    public void updateWithRowId3() throws SqlJetException {
        rowId = 2;
        db.write().asVoid(db -> {
                table.insert();
                table.open().updateWithRowId(rowId, Long.valueOf(rowId));
        });
        success = true;
    }

    @Test
    public void updateWithRowId4() throws SqlJetException {
        rowId = 2;
        db.write().asVoid(db -> {
                table.insert();
                table.open().updateWithRowId(rowId, (Object[]) null);
        });
        success = true;
    }

    @Test
    public void updateWithRowId5() throws SqlJetException {
        rowId = 2;
        db.write().asVoid(db -> {
                table.insert();
                table.open().updateWithRowId(rowId, new Object[] { null });
        });
        success = true;
    }

    @Test
    public void updateWithRowId6() throws SqlJetException {
        rowId = 2;
        db.write().asVoid(db -> {
                table.insert();
                table.open().updateWithRowId(0, Long.valueOf(rowId));
        });
        success = true;
    }

    @Test
    public void updateWithRowId7() throws SqlJetException {
        rowId = 2;
        db.write().asVoid(db -> {
                table.insert();
                table.open().updateWithRowId(0, new Object[] { Long.valueOf(rowId) });
        });
        success = true;
    }

    @Test
    public void updateWithRowId8() throws SqlJetException {
        rowId = 2;
        t2 = true;
        db.write().asVoid(db -> {
                table2.insert();
                table2.open().updateWithRowId(rowId, Long.valueOf(rowId));
        });
        success = true;
    }

    @Test
    public void updateWithRowId9() throws SqlJetException {
        rowId = 2;
        t2 = true;
        db.write().asVoid(db -> {
                table2.insert(ONE);
                table2.open().updateWithRowId(rowId, Long.valueOf(rowId));
        });
        success = true;
    }

    @Test
    public void updateWithRowId10() throws SqlJetException {
        t2 = true;
        db.write().asVoid(db -> {
                table2.insertWithRowId(rowId, Long.valueOf(rowId));
                rowId = 2;
                table2.open().updateWithRowId(rowId, Long.valueOf(rowId));
        });
        success = true;
    }

    @Test
    public void updateByFieldNames1() throws SqlJetException {
        t2 = true;
        rowId = 2;
        values.put(ROWID, Long.valueOf(rowId));
        values.put(ID, Long.valueOf(rowId));
        db.write().asVoid(db -> {
                table2.insert();
                table2.open().updateByFieldNames(values);
        });
        success = true;
    }

    @Test
    public void otherWay() throws SqlJetException {
        success = false;
        final ISqlJetTable table3 = db.getTable("t3");
        Assert.assertNotNull(table3);
        Assert.assertNull(table3.getPrimaryKeyIndexName());
        db.write().asVoid(db -> table3.insert());
        db.read().asVoid(db -> {
                Assert.assertEquals(Long.valueOf(1L), table3.open().getValue(ID));
        });
        db.write().asVoid(db -> table3.insertWithRowId(0, Long.valueOf(2L), null));
        db.read().asVoid(db -> {
                Assert.assertTrue(!table3.lookup(null, Long.valueOf(2L)).eof());
        });
    }

}
