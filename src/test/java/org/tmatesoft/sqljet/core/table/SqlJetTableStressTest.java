/**
 * SqlJetTableStressTest.java
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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractInMemoryTest;

/**
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 * 
 */
public class SqlJetTableStressTest extends AbstractInMemoryTest {

    private static final String THE_VALUE = "The quick brown fox jumps over the lazy dog!";

    @Before
    public void setUp() throws Exception {
        db.write().asVoid(db -> db.createTable("create table t (c1 text, c2 int)"));
    }

    @Test
    public void testInsert100000Records() throws Exception {
        final ISqlJetTable t = db.getTable("t");
        db.write().asVoid(db -> {
            for (int i = 0; i < 100000; i++) {
                t.insert(THE_VALUE, Integer.valueOf(i));
            }
        });

        db.read().asVoid(db -> {
            ISqlJetCursor c = t.open();
            c.last();
            c.previous();
            Assert.assertEquals(THE_VALUE, c.getString(0));
            Assert.assertEquals(99998, c.getInteger(1));
        });
    }
}
