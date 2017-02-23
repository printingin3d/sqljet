/**
 * MultiColumnPrimaryKeyTest.java
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

import static org.tmatesoft.sqljet.core.IntConstants.ONE;
import static org.tmatesoft.sqljet.core.IntConstants.TWO;

import java.io.File;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.SqlJetException;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class MultiColumnPrimaryKeyTest {

	private File file;
    private SqlJetDb db;
    private ISqlJetTable table;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        file = File.createTempFile("test", null);
        file.deleteOnExit();
        db = SqlJetDb.open(file, true);
        db.write().asVoid(db -> db.createTable("create table t(a integer, b integer, c integer, primary key(a,b), unique(b,c));"));
        table = db.getTable("t");
        db.write().asVoid(db -> {
                table.insert(ONE, ONE, ONE);
                table.insert(ONE, TWO, ONE);
                table.insert(TWO, ONE, TWO);
        });
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        db.close();
    }

    @Test
    public void eof() throws SqlJetException {
        db.read().asVoid(db -> {
                Assert.assertTrue(!table.lookup(null, ONE, ONE).eof());
                Assert.assertTrue(!table.lookup(null, ONE, TWO).eof());
                Assert.assertTrue(!table.lookup(null, TWO, ONE).eof());
                Assert.assertTrue(table.lookup(null, TWO, TWO).eof());
        });
    }

    @Test(expected = SqlJetException.class)
    public void insert() throws SqlJetException {
        db.write().asVoid(db -> table.insert(ONE, ONE, TWO));
    }

    @Test(expected = SqlJetException.class)
    public void insertFail() throws SqlJetException {
        db.write().asVoid(db -> table.insert(TWO, TWO, ONE));
    }

    @Test
    public void update() throws SqlJetException {
        db.write().asVoid(db -> table.lookup(null, ONE, ONE).update(TWO, TWO, TWO));
        db.read().asVoid(db -> {
                Assert.assertTrue(table.lookup(null, ONE, ONE).eof());
                Assert.assertTrue(!table.lookup(null, ONE, TWO).eof());
                Assert.assertTrue(!table.lookup(null, TWO, ONE).eof());
                Assert.assertTrue(!table.lookup(null, TWO, TWO).eof());
        });
    }

    @Test(expected = SqlJetException.class)
    public void updateFail() throws SqlJetException {
        db.write().asVoid(db -> table.lookup(null, ONE, ONE).update(ONE, TWO, TWO));
    }

    @Test
    public void delete() throws SqlJetException {
        db.write().asVoid(db -> table.lookup(null, ONE, ONE).delete());
        db.read().asVoid(db -> {
                Assert.assertTrue(table.lookup(null, ONE, ONE).eof());
                Assert.assertTrue(!table.lookup(null, ONE, TWO).eof());
                Assert.assertTrue(!table.lookup(null, TWO, ONE).eof());
        });
    }

}
