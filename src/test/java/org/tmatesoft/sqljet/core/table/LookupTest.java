/**
 * LookupTest.java
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

import org.junit.Assert;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class LookupTest extends AbstractNewDbTest {

    private ISqlJetTable table;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        db.createTable("create table t(a integer primary key, b text)");
        db.createIndex("create index i on t(b)");
        table = db.getTable("t");
        table.insert(ONE, "a");
        table.insert(TWO, null);
    }

    @Test
    public void lookup() throws SqlJetException {
        db.read().asVoid(db -> {
                final ISqlJetCursor c = table.lookup("i", "a");
                Assert.assertNotNull(c);
                Assert.assertFalse(c.eof());
                final long a = c.getInteger("a");
                Assert.assertEquals(1, a);
                Assert.assertFalse(c.next());
                Assert.assertTrue(c.eof());
        });
    }

    @Test
    public void lookupIsNull() throws SqlJetException {
        db.read().asVoid(db -> {
                final ISqlJetCursor c = table.lookup("i", new Object[] { null });
                Assert.assertNotNull(c);
                Assert.assertFalse(c.eof());
                final long a = c.getInteger("a");
                Assert.assertEquals(2, a);
                Assert.assertFalse(c.next());
                Assert.assertTrue(c.eof());
        });
    }

}
