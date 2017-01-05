/**
 * AutoCommitTest.java
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

import static org.tmatesoft.sqljet.core.IntConstants.EIGHT;
import static org.tmatesoft.sqljet.core.IntConstants.FIVE;
import static org.tmatesoft.sqljet.core.IntConstants.FOUR;
import static org.tmatesoft.sqljet.core.IntConstants.ONE;
import static org.tmatesoft.sqljet.core.IntConstants.SEVEN;
import static org.tmatesoft.sqljet.core.IntConstants.SIX;
import static org.tmatesoft.sqljet.core.IntConstants.THREE;
import static org.tmatesoft.sqljet.core.IntConstants.TWO;

import java.util.Map;
import java.util.TreeMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.internal.SqlJetAssert;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class AutoCommitTest extends AbstractNewDbTest {
    private static final String B = "b";
    private static final String A = "a";

    private ISqlJetTable table;
    private boolean success = false;

    /**
     * @throws java.lang.Exception
     */
	@Before
    public void setUpTable() throws Exception {
        db.runVoidWriteTransaction(db -> db.createTable("create table t(a integer primary key, b integer);"));
        table = db.getTable("t");
    }

    /**
     * @throws java.lang.Exception
     */
	@After
    public void tearDownTable() throws Exception {
        if (success) {
            db.runVoidReadTransaction(db -> {
                    final ISqlJetCursor lookup1 = table.lookup(null, ONE);
                    Assert.assertTrue(!lookup1.eof());
                    Assert.assertEquals(Long.valueOf(2L), lookup1.getValue(B));
                    lookup1.close();

                    final ISqlJetCursor lookup3 = table.lookup(null, THREE);
                    Assert.assertTrue(!lookup3.eof());
                    Assert.assertEquals(Long.valueOf(4L), lookup3.getValue(B));
                    lookup3.close();
            });
        }
    }

    private Map<String, Object> map(Object... values) throws SqlJetException {
        if (values == null) {
			return null;
		}
        if (values.length % 2 != 0) {
			throw new SqlJetException(SqlJetErrorCode.MISUSE);
		}
        final Map<String, Object> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        String name = null;
        for (Object value : values) {
            if (name != null) {
                map.put(name, value);
                name = null;
            } else {
                if (value != null && value instanceof String) {
                    name = (String) value;
                } else {
					throw new SqlJetException(SqlJetErrorCode.MISUSE);
				}
            }
        }
        SqlJetAssert.assertTrue(name == null, SqlJetErrorCode.MISUSE);
        return map;
    }

    @Test
    public void insertAutoCommit() throws SqlJetException {
        table.insert(ONE, TWO);
        table.insert(THREE, FOUR);
        success = true;
    }

    @Test
    public void insertTransaction() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                table.insert(ONE, TWO);
                table.insert(THREE, FOUR);
        });
        success = true;
    }

    @Test
    public void insertMixed() throws SqlJetException {
        table.insert(ONE, TWO);
        db.runVoidWriteTransaction(db -> {
                table.insert(THREE, FOUR);
                table.insert(FIVE, SIX);
        });
        table.insert(SEVEN, EIGHT);
        success = true;
    }

    @Test
    public void insertWithRowIdAutoCommit() throws SqlJetException {
        table.insertWithRowId(1, null, TWO);
        table.insertWithRowId(3, null, FOUR);
        success = true;
    }

    @Test
    public void insertWithRowIdTransaction() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                table.insertWithRowId(1, null, TWO);
                table.insertWithRowId(3, null, FOUR);
        });
        success = true;
    }

    @Test
    public void insertWithRowIdMixed() throws SqlJetException {
        table.insertWithRowId(1, null, TWO);
        db.runVoidWriteTransaction(db -> {
                table.insertWithRowId(3, null, FOUR);
                table.insertWithRowId(5, null, SIX);
        });
        table.insertWithRowId(7, null, EIGHT);
        success = true;
    }

    @Test
    public void insertWithNamesAutoCommit() throws SqlJetException {
        table.insertByFieldNames(map(A, ONE, B, TWO));
        table.insertByFieldNames(map(A, THREE, B, FOUR));
        success = true;
    }

    @Test
    public void insertWithNamesTransaction() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                table.insertByFieldNames(map(A, ONE, B, TWO));
                table.insertByFieldNames(map(A, THREE, B, FOUR));
        });
        success = true;
    }

    @Test
    public void insertWithNamesMixed() throws SqlJetException {
        table.insertByFieldNames(map(A, ONE, B, TWO));
        db.runVoidWriteTransaction(db -> {
                table.insertByFieldNames(map(A, THREE, B, FOUR));
                table.insertByFieldNames(map(A, FIVE, B, SIX));
        });
        table.insertByFieldNames(map(A, SEVEN, B, EIGHT));
        success = true;
    }

    @Test
    public void updateAutoCommit() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                table.insert(ONE, ONE);
                table.insert(THREE, THREE);
                table.lookup(null, ONE).update(null, TWO);
                table.lookup(null, THREE).update(null, FOUR);
        });
        success = true;
    }

    @Test
    public void updateTransaction() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                table.insert(ONE, TWO);
                table.insert(THREE, FOUR);
                table.lookup(null, ONE).update(null, TWO);
                table.lookup(null, THREE).update(null, FOUR);
        });
        success = true;
    }

    @Test
    public void updateMixed() throws SqlJetException {
        table.insert(ONE, TWO);
        db.runVoidWriteTransaction(db -> {
                table.insert(THREE, FOUR);
                table.insert(FIVE, SIX);
                table.lookup(null, ONE).update(null, TWO);
        });
        db.runVoidWriteTransaction(db -> {
                table.insert(SEVEN, EIGHT);
                table.lookup(null, THREE).update(null, FOUR);
        });
        success = true;
    }

    @Test
    public void updateWithRowIdAutoCommit() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                table.insertWithRowId(1, null, ONE);
                table.insertWithRowId(3, null, THREE);
                table.lookup(null, ONE).updateWithRowId(0, null, TWO);
                table.lookup(null, THREE).updateWithRowId(0, null, FOUR);
        });
        success = true;
    }

    @Test
    public void updateWithRowIdTransaction() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                table.insertWithRowId(1, null, ONE);
                table.insertWithRowId(3, null, THREE);
                table.lookup(null, ONE).updateWithRowId(0, null, TWO);
                table.lookup(null, THREE).updateWithRowId(0, null, FOUR);
        });
        success = true;
    }

    @Test
    public void updateWithRowIdMixed() throws SqlJetException {
        table.insertWithRowId(1, null, ONE);
        db.runVoidWriteTransaction(db -> {
                table.insertWithRowId(3, null, THREE);
                table.insertWithRowId(5, null, SIX);
                table.lookup(null, ONE).updateWithRowId(0, null, TWO);
        });
        db.runVoidWriteTransaction(db -> {
                table.insertWithRowId(7, null, EIGHT);
                table.lookup(null, THREE).updateWithRowId(0, null, FOUR);
        });
        success = true;
    }

    @Test
    public void updateByFieldNamesAutoCommit() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                table.insertByFieldNames(map(A, ONE, B, ONE));
                table.insertByFieldNames(map(A, THREE, B, THREE));
                table.lookup(null, ONE).updateByFieldNames(map(B, TWO));
                table.lookup(null, THREE).updateByFieldNames(map(B, FOUR));
        });
        success = true;
    }

    @Test
    public void updateByFieldNamesTransaction() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                table.insertByFieldNames(map(A, ONE, B, ONE));
                table.insertByFieldNames(map(A, THREE, B, THREE));
                table.lookup(null, ONE).updateByFieldNames(map(B, TWO));
                table.lookup(null, THREE).updateByFieldNames(map(B, FOUR));
        });
        success = true;
    }

    @Test
    public void updateByFieldNamesMixed() throws SqlJetException {
        table.insertByFieldNames(map(A, ONE, B, ONE));
        db.runVoidWriteTransaction(db -> {
                table.insertByFieldNames(map(A, THREE, B, THREE));
                table.insertByFieldNames(map(A, FIVE, B, SIX));
                table.lookup(null, ONE).updateByFieldNames(map(B, TWO));
        });
        db.runVoidWriteTransaction(db -> {
                table.insertByFieldNames(map(A, SEVEN, B, EIGHT));
                table.lookup(null, THREE).updateByFieldNames(map(B, FOUR));
        });
        success = true;
    }

    @Test
    public void deleteAutoCommit() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                table.insert(ONE, ONE);
                table.insert(THREE, THREE);
                table.lookup(null, ONE).delete();
                table.lookup(null, THREE).delete();
                table.insert(ONE, TWO);
                table.insert(THREE, FOUR);
        });
        success = true;
    }

    @Test
    public void deleteTransaction() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                table.insert(ONE, ONE);
                table.insert(THREE, THREE);
                table.lookup(null, ONE).delete();
                table.lookup(null, THREE).delete();
                table.insert(ONE, TWO);
                table.insert(THREE, FOUR);
        });
        success = true;
    }

    @Test
    public void deleteMixed() throws SqlJetException {
        table.insert(ONE, ONE);
        db.runVoidWriteTransaction(db -> {
                table.insert(THREE, THREE);
                table.lookup(null, ONE).delete();
        });
        db.runVoidWriteTransaction(db -> {
                table.lookup(null, THREE).delete();
                table.insert(ONE, TWO);
                table.insert(THREE, FOUR);
                success = true;
        });
    }

    @Test
    public void createTableAutocommit() throws SqlJetException {
        db.createTable("create table t1(a,b)");
        Assert.assertNotNull(db.getTable("t1"));
    }

    @Test
    public void createIndexAutocommit() throws SqlJetException {
        table.insert(ONE, ONE);
        db.createIndex("create index idx on t(a,b)");
        db.runVoidWriteTransaction(db -> {
                Assert.assertTrue(!db.getTable("t").lookup("idx", ONE, ONE).eof());
        });
    }

    @Test
    public void dropTableAutocommit() throws SqlJetException {
        db.createTable("create table t1(a,b)");
        Assert.assertNotNull(db.getSchema().getTable("t1"));
        db.close();
        db = SqlJetDb.open(file, true);
        db.dropTable("t1");
        Assert.assertNull(db.getSchema().getTable("t1"));
    }

    @Test
    public void dropIndexAutocommit() throws SqlJetException {
        db.createIndex("create index idx on t(a,b)");
        Assert.assertNotNull(db.getSchema().getIndex("idx"));
        db.close();
        db = SqlJetDb.open(file, true);
        db.dropIndex("idx");
        Assert.assertNull(db.getSchema().getIndex("idx"));
    }

    @Test
    public void dropTableTransaction() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                db.createTable("create table t1(a,b)");
                Assert.assertNotNull(db.getSchema().getTable("t1"));
        });
        db.close();
        db = SqlJetDb.open(file, true);
        db.runVoidWriteTransaction(db -> {
                db.dropTable("t1");
                Assert.assertNull(db.getSchema().getTable("t1"));
        });
    }

    @Test
    public void dropIndexTransaction() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
                db.createIndex("create index idx on t(a,b)");
                Assert.assertNotNull(db.getSchema().getIndex("idx"));
        });
        db.close();
        db = SqlJetDb.open(file, true);
        db.runVoidWriteTransaction(db -> {
                db.dropIndex("idx");
                Assert.assertNull(db.getSchema().getIndex("idx"));
        });
    }

    @Test
    public void createTableManaged() throws SqlJetException {
        db.beginTransaction(SqlJetTransactionMode.WRITE);
        boolean commited = false;
        try {
            db.createTable("create table t1(a,b)");
            db.commit();
            commited = true;
        } finally {
            if (!commited) {
                db.rollback();
            }
        }
        Assert.assertNotNull(db.getTable("t1"));
    }

    @Test
    public void beginReadMany() throws SqlJetException {
        db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
        db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
        db.beginTransaction(SqlJetTransactionMode.WRITE);
        db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
    }

    @Test
    public void beginWriteMany() throws SqlJetException {
        db.beginTransaction(SqlJetTransactionMode.WRITE);
        db.beginTransaction(SqlJetTransactionMode.WRITE);
        db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
        db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
    }

    @Test
    public void commitTwice() throws SqlJetException {
        db.commit();
        db.commit();
    }

    @Test
    public void rollbackTwice() throws SqlJetException {
        db.rollback();
        db.rollback();
    }

}
