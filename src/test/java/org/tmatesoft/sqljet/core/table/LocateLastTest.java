/**
 * LocateLastTest.java
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

import static org.tmatesoft.sqljet.core.IntConstants.FOUR;
import static org.tmatesoft.sqljet.core.IntConstants.ONE;
import static org.tmatesoft.sqljet.core.IntConstants.THREE;
import static org.tmatesoft.sqljet.core.IntConstants.TWO;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class LocateLastTest extends AbstractNewDbTest {

    /**
     * @throws java.lang.Exception
     */
    @Override
	@Before
    public void setUp() throws Exception {
        super.setUp();
        db.write().asVoid(db -> {
                db.createTable("create table t(a integer primary key, b integer)");
                db.createIndex("create index b on t(b,a)");
                db.createIndex("create index d on t(b desc, a desc)");
                final ISqlJetTable t = db.getTable("t");
                t.insert(ONE, ONE);
                t.insert(TWO, TWO);
                t.insert(THREE, THREE);
                t.insert(FOUR, TWO);
        });
    }

    @Test
    public void order() throws SqlJetException {
        db.read().asVoid(db -> {
                final ISqlJetTable t = db.getTable("t");
                final ISqlJetCursor b = t.order("b");
                Assert.assertTrue(b.last());
                Assert.assertEquals(3L, b.getInteger("b"));
                Assert.assertTrue(b.previous());
                Assert.assertEquals(2L, b.getInteger("b"));
                Assert.assertTrue(b.previous());
                Assert.assertEquals(2L, b.getInteger("b"));
                Assert.assertTrue(b.previous());
                Assert.assertEquals(1L, b.getInteger("b"));
                Assert.assertFalse(b.previous());
        });
    }

    @Test
    public void scope() throws SqlJetException {
        db.read().asVoid(db -> {
                final ISqlJetTable t = db.getTable("t");
                final ISqlJetCursor b = t.scope("b", new Object[] { TWO }, new Object[] { TWO });
                Assert.assertTrue(b.last());
                Assert.assertEquals(2L, b.getInteger("b"));
                Assert.assertEquals(4L, b.getInteger("a"));
                Assert.assertTrue(b.previous());
                Assert.assertEquals(2L, b.getInteger("b"));
                Assert.assertEquals(2L, b.getInteger("a"));
                Assert.assertFalse(b.previous());
        });
    }

    @Test
    public void orderDesc() throws SqlJetException {
        db.read().asVoid(db -> {
                final ISqlJetTable t = db.getTable("t");
                final ISqlJetCursor b = t.order("d");
                Assert.assertTrue(b.last());
                Assert.assertEquals(1L, b.getInteger("b"));
                Assert.assertEquals(1L, b.getInteger("a"));
                Assert.assertTrue(b.previous());
                Assert.assertEquals(2L, b.getInteger("b"));
                Assert.assertEquals(2L, b.getInteger("a"));
                Assert.assertTrue(b.previous());
                Assert.assertEquals(2L, b.getInteger("b"));
                Assert.assertEquals(4L, b.getInteger("a"));
                Assert.assertTrue(b.previous());
                Assert.assertEquals(3L, b.getInteger("b"));
                Assert.assertEquals(3L, b.getInteger("a"));
                Assert.assertFalse(b.previous());
        });
    }

    @Test
    public void scopeDesc() throws SqlJetException {
        db.read().asVoid(db -> {
                final ISqlJetTable t = db.getTable("t");
                final ISqlJetCursor b = t.scope("d", new Object[] { TWO }, new Object[] { ONE });
                Assert.assertTrue(b.last());
                Assert.assertEquals(1L, b.getInteger("b"));
                Assert.assertEquals(1L, b.getInteger("a"));
                Assert.assertTrue(b.previous());
                Assert.assertEquals(2L, b.getInteger("b"));
                Assert.assertEquals(2L, b.getInteger("a"));
                Assert.assertTrue(b.previous());
                Assert.assertEquals(2L, b.getInteger("b"));
                Assert.assertEquals(4L, b.getInteger("a"));
                Assert.assertFalse(b.previous());
        });
    }

}
