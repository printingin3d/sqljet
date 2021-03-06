/**
 * SqlJetIndexTest.java
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.tmatesoft.sqljet.core.IntConstants.ONE;
import static org.tmatesoft.sqljet.core.IntConstants.TEN;
import static org.tmatesoft.sqljet.core.IntConstants.TWO;
import static org.tmatesoft.sqljet.core.IntConstants._11;
import static org.tmatesoft.sqljet.core.IntConstants._12;
import static org.tmatesoft.sqljet.core.IntConstants._13;
import static org.tmatesoft.sqljet.core.IntConstants._20;
import static org.tmatesoft.sqljet.core.IntConstants._23;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.SqlJetException;

/**
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 * 
 */
public class SqlJetIndexTest {

    private SqlJetDb db;

    @Before
    public void setUp() throws Exception {
        File fileDb = File.createTempFile("indexTest", null);
        fileDb.deleteOnExit();
        db = SqlJetDb.open(fileDb, true);
    }

    @After
    public void tearDown() throws Exception {
        if (db != null) {
            db.close();
        }
    }

	@Test
    public void createTableT() throws SqlJetException {
        db.write().asVoid(db -> {
                db.createTable("create table t (a text, b text, c int, d int)");
                ISqlJetTable t = db.getTable("t");
                t.insert("n", "y", TEN, _20);
                t.insert("x", "z", _11, _12);
                t.insert("a", "b", TEN, _13);
                t.insert("c", "b", TEN, _23);
        });
    }

    @Test
    public void testSingleColumnIndex() throws Exception {
        createTableT();
        db.write().asVoid(db -> db.createIndex("create index tb on t (b)"));
        db.read().asVoid(db -> {
                ISqlJetTable t = db.getTable("t");
                assertNotNull(t);

                ISqlJetCursor c = t.lookup("tb", "z");
                assertTrue(!c.eof());
                assertEquals("x", c.getString("a"));
                c.next();
                assertTrue(c.eof());
                c.close();

                Set<String> values = new HashSet<>();
                c = t.lookup("tb", "b");
                while (!c.eof()) {
                    values.add(c.getString("a"));
                    c.next();
                }
                c.close();
                assertTrue(values.size() == 2);
                assertTrue(values.contains("a"));
                assertTrue(values.contains("c"));
        });
    }

    @Test
    public void testMultiColumnIndex() throws Exception {
        createTableT();
        db.write().asVoid(db -> db.createIndex("create index tbc on t (b,c)"));
        db.read().asVoid(db -> {
                ISqlJetTable t = db.getTable("t");
                assertNotNull(t);
                ISqlJetCursor c = t.lookup("tbc", "y", TEN);
                assertTrue(!c.eof());
                assertEquals("n", c.getString("a"));
                c.next();
                assertTrue(c.eof());
                c.close();

                Set<String> values = new HashSet<>();
                c = t.lookup("tbc", "b", TEN);
                while (!c.eof()) {
                    values.add(c.getString("a"));
                    c.next();
                }
                c.close();
                assertTrue(values.size() == 2);
                assertTrue(values.contains("a"));
                assertTrue(values.contains("c"));
        });
    }

    @Test
    public void testAutomaticIntConversion() throws Exception {
        db.write().asVoid(db -> {
                db.createTable("create table t (a int)");
                db.createIndex("create index ta on t (a)");
                ISqlJetTable t = db.getTable("t");
                t.insert(Long.valueOf(10L));
                t.insert(Long.valueOf(20L));
        });
        db.read().asVoid(db -> {
                ISqlJetTable t = db.getTable("t");
                // 32bit integer, not long -> should be promoted to long
                ISqlJetCursor c = t.lookup("ta", _20);
                assertFalse(c.eof());
                assertEquals(20L, c.getInteger(0));
                c.next();
                assertTrue(c.eof());
        });
    }
    
    @Test
    public void testAutomaticFloatConversion() throws Exception {
        db.write().asVoid(db -> {
                db.createTable("create table t (a real)");
                db.createIndex("create index ta on t (a)");
                ISqlJetTable t = db.getTable("t");
                t.insert(Double.valueOf(0.1D));
                t.insert(Double.valueOf(0.2D));
        });
        db.read().asVoid(db -> {
                ISqlJetTable t = db.getTable("t");
                assertEquals(0.1D, t.open().getFloat(0), 1E-10);
                // float, not double -> should be promoted to double
                ISqlJetCursor c = t.lookup("ta", Float.valueOf(0.2F));
                assertFalse(c.eof());
                assertEquals(0.2D, c.getFloat(0), 1E-10);
                c.next();
                assertTrue(c.eof());
        });
    }
    
    @Test
    public void testReadUsingColumnPK() throws Exception {
        db.write().asVoid(db -> {
                db.createTable("create table t (a int primary key, b text)");
                ISqlJetTable t = db.getTable("t");
                t.insert(ONE, "zzz");
                t.insert(TWO, "www");
        });
        db.read().asVoid(db -> {
                ISqlJetTable t = db.getTable("t");
                assertNotNull(t.getPrimaryKeyIndexName());
                ISqlJetCursor c = t.lookup(t.getPrimaryKeyIndexName(), ONE);
                assertFalse(c.eof());
                assertEquals("zzz", c.getString(1));
                c.next();
                assertTrue(c.eof());
        });
    }

    @Test
    public void testReadUsingColumnPKAutoinc() throws Exception {
        db.write().asVoid(db -> {
                db.createTable("create table t (a integer primary key autoincrement, b text)");
                ISqlJetTable t = db.getTable("t");
                // primary key has 'autoincrement' constraint - you are not
                // supposed
                // to provide it's value: it should be generated automatically
                t.insert(null, "zzz");
                t.insert(null, "www");
        });
        db.read().asVoid(db -> {
                ISqlJetTable t = db.getTable("t");
                assertNull(t.getPrimaryKeyIndexName());
                ISqlJetCursor c = t.lookup(t.getPrimaryKeyIndexName(), TWO);
                assertFalse(c.eof());
                assertEquals("www", c.getString(1));
                c.next();
                assertTrue(c.eof());
        });
    }

    @Test
    public void testReadUsingSecondColumnPK() throws Exception {
        db.write().asVoid(db -> {
                db.createTable("create table t (a text, b int primary key)");
                ISqlJetTable t = db.getTable("t");
                t.insert("zzz", ONE);
                t.insert("www", TWO);
        });
        db.read().asVoid(db -> {
                ISqlJetTable t = db.getTable("t");
                assertNotNull(t.getPrimaryKeyIndexName());
                ISqlJetCursor c = t.lookup(t.getPrimaryKeyIndexName(), TWO);
                assertFalse(c.eof());
                assertEquals("www", c.getString(0));
                c.next();
                assertTrue(c.eof());
        });
    }

    @Test
    public void testNoRowidPK() throws Exception {
        db.write().asVoid(db -> {
                db.createTable("create table t (a integer primary key, b text)");
                ISqlJetTable t = db.getTable("t");
                t.insert(null, "zzz");
                t.insert(null, "www");
        });
        ISqlJetTable t = db.getTable("t");
        assertNull(t.getPrimaryKeyIndexName());
    }

    @Test
    public void testReadUsingSingleColumnTablePK() throws Exception {
        db.write().asVoid(db -> {
                db.createTable("create table t (a text, b text, primary key (a))");
                ISqlJetTable t = db.getTable("t");
                t.insert("set", "in");
                t.insert("get", "out");
                t.insert("bet", "down");
        });
        db.read().asVoid(db -> {
                ISqlJetTable t = db.getTable("t");
                assertNotNull(t.getPrimaryKeyIndexName());
                ISqlJetCursor c = t.lookup(t.getPrimaryKeyIndexName(), "get");
                assertFalse(c.eof());
                assertEquals("out", c.getString(1));
                c.next();
                assertTrue(c.eof());
        });
    }

    @Test
    public void testReadUsingMultiColumnTablePK() throws Exception {
        db.write().asVoid(db -> {
                db.createTable("create table t (a text, b text, primary key (a,b))");
                ISqlJetTable t = db.getTable("t");
                t.insert("get", "in");
                t.insert("get", "out");
                t.insert("get", "down");
        });
        db.read().asVoid(db -> {
                ISqlJetTable t = db.getTable("t");
                assertNotNull(t.getPrimaryKeyIndexName());
                ISqlJetCursor c = t.lookup(t.getPrimaryKeyIndexName(), "get", "out");
                assertFalse(c.eof());
                assertEquals("get", c.getString(0));
                assertEquals("out", c.getString(1));
                c.next();
                assertTrue(c.eof());
        });
    }
}
