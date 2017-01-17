/**
 * DefaultValuesTest.java
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

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetLimits;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class DefaultValuesTest extends AbstractNewDbTest {

    /**
     * @throws java.lang.Exception
     */
    @Override
	@Before
    public void setUp() throws Exception {
        super.setUp();
        db.getOptions().setFileFormat(ISqlJetLimits.SQLJET_MAX_FILE_FORMAT);
        db.write().asVoid(db -> db.createTable("create table t(a integer primary key, b integer default 1)"));
    }

    @Test
    public void insert() throws SqlJetException {
        final ISqlJetTable t = db.getTable("t");
        t.insert();
        db.read().asVoid(db -> {
                final ISqlJetCursor c = t.open();
                Assert.assertFalse(c.isNull("b"));
                Assert.assertEquals(1L, c.getInteger("b"));
        });
    }

    @Test
    public void insertText() throws SqlJetException {
        db.write().asVoid(db -> db.createTable("create table t2(a integer primary key, b text default 'abc def')"));
        final ISqlJetTable t2 = db.getTable("t2");
        t2.insert();
        db.read().asVoid(db -> {
                final ISqlJetCursor c = t2.open();
                Assert.assertFalse(c.isNull("b"));
                Assert.assertEquals("abc def", c.getString("b"));
        });
    }

    @Test
    public void insertTextDouble() throws SqlJetException {
        db.write().asVoid(db -> db.createTable("create table t2(a integer primary key, b text default \"abc def\")"));
        final ISqlJetTable t2 = db.getTable("t2");
        t2.insert();
        db.read().asVoid(db -> {
                final ISqlJetCursor c = t2.open();
                Assert.assertFalse(c.isNull("b"));
                Assert.assertEquals("abc def", c.getString("b"));
        });
    }

    @Test
    public void insertByNames() throws SqlJetException {
        final ISqlJetTable t = db.getTable("t");
        t.insertByFieldNames(new HashMap<String, Object>());
        db.read().asVoid(db -> {
                final ISqlJetCursor c = t.open();
                Assert.assertFalse(c.isNull("b"));
                Assert.assertEquals(1L, c.getInteger("b"));
        });
    }

    @Test
    public void insertAffinity() throws SqlJetException {
        final ISqlJetTable t = db.getTable("t");
        t.insert();
        db.read().asVoid(db -> {
                final ISqlJetCursor c = t.open();
                Assert.assertFalse(c.isNull("b"));
                Assert.assertEquals(Long.valueOf(1L), c.getValue("b"));
        });
    }

}
