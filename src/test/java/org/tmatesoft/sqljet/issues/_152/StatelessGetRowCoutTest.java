/**
 * StatelessGetRowCoutTest.java
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
package org.tmatesoft.sqljet.issues._152;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;
import org.tmatesoft.sqljet.core.table.SqlJetScope;

import static org.tmatesoft.sqljet.core.IntConstants.*;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class StatelessGetRowCoutTest extends AbstractNewDbTest {

    @Before
    public void setup() throws SqlJetException {
        db.createTable("CREATE TABLE IF NOT EXISTS table (name TEXT, count INTEGER)");
        db.createIndex("CREATE UNIQUE INDEX IF NOT EXISTS names_idx ON table(name)");

        db.createTable("CREATE TABLE IF NOT EXISTS pairs (x INTEGER, y INTEGER)");
        db.createIndex("CREATE INDEX IF NOT EXISTS pairs_idx ON pairs(x, y)");
        
        db.runVoidWriteTransaction(db -> {
                db.getTable("table").insert("XYZ", ONE);
                db.getTable("table").insert("XYZZ", ONE);
                db.getTable("table").insert("ABC", ONE);
                db.getTable("table").insert("ABCD", ONE);
                db.getTable("table").insert("ABCDEF", ONE);
                db.getTable("table").insert("A", ONE);
                
                db.getTable("pairs").insert(ONE, TWO);
                db.getTable("pairs").insert(ONE, TWO);
                db.getTable("pairs").insert(ONE, TWO);
                db.getTable("pairs").insert(ONE, TWO);
        });
    }
    
    @Test
    public void testRowCountIsStateless() throws SqlJetException {
        db.runVoidReadTransaction(db -> {
                SqlJetScope scope = new SqlJetScope(new Object[] {"AB"}, new Object[] {"BC"});
                ISqlJetCursor cursor = db.getTable("table").scope("names_idx", scope);
                Assert.assertTrue(!cursor.eof());
                long count = cursor.getRowCount();
                Assert.assertTrue(count > 0);
                Assert.assertTrue(!cursor.eof());
                Assert.assertTrue(!cursor.eof());
                Assert.assertTrue(cursor.getRowCount() > 0);
                cursor.close();

                cursor = db.getTable("table").scope("names_idx", scope);
                // now at 0.
                
                count = cursor.getRowCount();
                Assert.assertTrue(count > 0);
                Assert.assertTrue(!cursor.eof());
                Assert.assertTrue(cursor.getRowCount() > 0);
                Assert.assertTrue(!cursor.eof());
                
                Assert.assertTrue(cursor.next());
                // now at 1.
                Assert.assertFalse(cursor.eof());
                Assert.assertTrue(cursor.getRowCount() == 3);
                Assert.assertTrue(cursor.next());
                
                // now at 2.
                Assert.assertFalse(cursor.eof());
                Assert.assertFalse(cursor.eof());
                Assert.assertTrue(cursor.getRowCount() == 3);
                Assert.assertFalse(cursor.next());
                // now at eof
                Assert.assertTrue(cursor.eof());

                Assert.assertTrue(cursor.previous());
                // at 2.
                Assert.assertTrue(cursor.getRowCount() == 3);
                Assert.assertFalse(cursor.eof());

                Assert.assertTrue(cursor.previous());
                // at 1.
                Assert.assertTrue(cursor.getRowCount() == 3);
                Assert.assertFalse(cursor.eof());
                Assert.assertTrue(cursor.previous());
                // at 0.
                Assert.assertFalse(cursor.previous());
                Assert.assertTrue(cursor.eof());
                Assert.assertTrue(cursor.getRowCount() == 3);
                
                Assert.assertTrue(cursor.first());
                Assert.assertFalse(cursor.eof());
                Assert.assertTrue(cursor.getRowCount() == 3);
                Assert.assertTrue(cursor.last());
                Assert.assertFalse(cursor.eof());
                Assert.assertTrue(cursor.getRowCount() == 3);
        });
    }
    
    @Test
    public void testRowCountIsStatelessWhenIndexIsNotUnique() throws SqlJetException {
        db.runVoidReadTransaction(db -> {
                SqlJetScope scope = new SqlJetScope(new Object[] {ONE, TWO}, true, new Object[] {ONE, TWO}, true);
                ISqlJetCursor cursor = db.getTable("pairs").scope("pairs_idx", scope);
        
                Assert.assertTrue(!cursor.eof());
                Assert.assertTrue(cursor.next());
                Assert.assertTrue(cursor.next());
                Assert.assertEquals(cursor.getRowCount(), 4);
                Assert.assertTrue(cursor.next());
                Assert.assertFalse(cursor.eof());
                Assert.assertFalse(cursor.next());
                Assert.assertTrue(cursor.eof());
        });
        
    }
}
