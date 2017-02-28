/**
 * IndexOrderTest.java
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
import static org.tmatesoft.sqljet.core.IntConstants.NINE;
import static org.tmatesoft.sqljet.core.IntConstants.ONE;
import static org.tmatesoft.sqljet.core.IntConstants.TEN;
import static org.tmatesoft.sqljet.core.IntConstants.THREE;
import static org.tmatesoft.sqljet.core.IntConstants.TWO;

import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.schema.ISqlJetIndexDef;
import org.tmatesoft.sqljet.core.table.SqlJetScope.SqlJetScopeBound;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class IndexScopeTest extends AbstractNewDbTest {
    private ISqlJetTable table, table1, table2, table3, table4, table5;

    /**
     * @throws java.lang.Exception
     */
    @Override
	@Before
    public void setUp() throws Exception {
        super.setUp();

        db.write().asVoid(db -> {
                db.createTable("create table t(a integer primary key, b integer)");
                db.createIndex("create index b on t(b)");
                db.createIndex("create index ab on t(a,b)");

                db.createTable("create table t1(a integer primary key, b integer)");
                db.createIndex("create index b1 on t1(b)");

                db.createTable("create table t2(a integer, b integer, primary key(a,b))");
                db.createTable("create table t3(a integer primary key)");

                db.createTable("create table t4(a text not null primary key, b integer not null)");
                db.createIndex("create index b4 on t4(b)");

                db.createTable("create table t5(a integer)");

                table = db.getTable("t");

                for (int i = 10; i > 0; i--) {
                    table.insert(null, Integer.valueOf(i));
                }

                table1 = db.getTable("t1");
                table1.insert(null, THREE);
                table1.insert(null, FIVE);
                table1.insert(null, Integer.valueOf(7));
                table1.insert(null, NINE);

                table2 = db.getTable("t2");

                for (int i = 10; i > 0; i--) {
                    table2.insert(Integer.valueOf(i), Integer.valueOf(i));
                }

                table3 = db.getTable("t3");
                table3.insert(THREE);
                table3.insert(FIVE);
                table3.insert(Integer.valueOf(7));
                table3.insert(NINE);

                table4 = db.getTable("t4");
                table4.insert("s", TEN);
                table4.insert("q", FOUR);
                table4.insert("l", THREE);
                table4.insert("j", TWO);
                table4.insert("e", ONE);
                table4.insert("t", EIGHT);

                table5 = db.getTable("t5");
        });
    }

    @Test
    public void scope() throws SqlJetException {
    	scopeTest(table, "b", "b", 2, 4, new long[] {2L, 3L, 4L});
    }

    @Test
    public void scopeFirst() throws SqlJetException {
    	scopeTest(table, "b", "b", new Object[] { FIVE }, null, new long[] {5L, 6L, 7L, 8L, 9L, 10L});
    }

    @Test
    public void scopeLast() throws SqlJetException {
    	scopeTest(table, "b", "b", null, new Object[] { FIVE }, new long[] {1L, 2L, 3L, 4L, 5L});
    }

    @Test
    public void scopeNull() throws SqlJetException {
    	scopeTest(table, "b", "b", null, null, new long[] {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L});
    }

    @Test
    public void scopeNear() throws SqlJetException {
    	scopeTest(table1, "b1", "b", 4, 8, new long[] {5L, 7L});
    }

    @Test
    public void scopeMulti1() throws SqlJetException {
        db.read().asVoid(db -> {
                final ISqlJetCursor c = table.scope("ab", new Object[] { TWO, NINE }, new Object[] { NINE, TWO });
                Assert.assertTrue(!c.eof());
                Assert.assertEquals(2L, c.getInteger("a"));
                Assert.assertEquals(9L, c.getInteger("b"));
                Assert.assertTrue(c.next());
                Assert.assertEquals(3L, c.getInteger("a"));
                Assert.assertEquals(8L, c.getInteger("b"));
        });
    }

    @Test
    public void scopePrimary() throws SqlJetException {
    	scopeTest(table2, null, "a", 2, 4, new long[] {2L, 3L, 4L});
    }

    @Test
    public void scopeRowId() throws SqlJetException {
    	scopeTest(table, null, "a", 2, 4, new long[] {2L, 3L, 4L});
    }

    @Test
    public void scopeRowIdNear() throws SqlJetException {
    	scopeTest(table3, null, "a", 4, 8, new long[] {5L, 7L});
    }
    
    private void scopeTest(ISqlJetTable t, String indexName, String fieldName, int start, int end, 
    		long[] expected) throws SqlJetException {
    	scopeTest(t, indexName, fieldName, 
    			new Object[] { Integer.valueOf(start) }, new Object[] { Integer.valueOf(end) }, expected);
    }
    
    private void scopeTest(ISqlJetTable t, String indexName, String fieldName, Object[] start, Object[] end, 
    		long[] expected) throws SqlJetException {
    	db.read().asVoid(db -> {
    		ISqlJetCursor c = t.scope(indexName, start, end);
    		Assert.assertTrue(!c.eof());
    		for (int i=0;i<expected.length;i++) {
    			if (i>0) {
					Assert.assertTrue(c.next());
				}
    			Assert.assertEquals(expected[i], c.getInteger(fieldName));
    		}
    		Assert.assertTrue(!c.next());
    		Assert.assertTrue(c.eof());
    	});
    }

    @Test
    public void scopeDeleteInScope() throws SqlJetException {
        db.write().asVoid(db -> {
                final ISqlJetCursor c = table4.scope("b4", new Object[] { Long.valueOf(7L) }, new Object[] { Long.valueOf(20L) });
                // should get two rows, one with 8, another with 10.
                Assert.assertTrue(!c.eof());
                Assert.assertEquals(8L, c.getInteger("b"));
                c.delete();
                Assert.assertTrue(!c.eof());
                Assert.assertEquals(10L, c.getInteger("b"));
                c.delete();
                Assert.assertTrue(c.eof());
        });
    }

    @Test
    public void scopeReverse() throws SqlJetException {
    	scopeTest(table, "b", "b", 4, 2, new long[] {4L, 3L, 2L});
    }

    @Test
    public void scopeReverse2() throws SqlJetException {
        db.read().asVoid(db -> {
                ISqlJetCursor c = table.scope("b", new Object[] { TWO }, new Object[] { FOUR });
                c = c.reverse();
                Assert.assertTrue(!c.eof());
                Assert.assertEquals(4L, c.getInteger("b"));
                Assert.assertTrue(c.next());
                Assert.assertEquals(3L, c.getInteger("b"));
                Assert.assertTrue(c.next());
                Assert.assertEquals(2L, c.getInteger("b"));
                Assert.assertTrue(!c.next());
                Assert.assertTrue(c.eof());
        });
    }

    @Test
    public void scopePrimaryReverse() throws SqlJetException {
    	scopeTest(table2, null, "a", 4, 2, new long[] {4L, 3L, 2L});
    }

    @Test
    public void scopePrimaryReverse2() throws SqlJetException {
        db.read().asVoid(db -> {
                ISqlJetCursor c = table2.scope(null, new Object[] { TWO }, new Object[] { FOUR });
                c = c.reverse();
                Assert.assertTrue(!c.eof());
                Assert.assertEquals(4L, c.getInteger("a"));
                Assert.assertTrue(c.next());
                Assert.assertEquals(3L, c.getInteger("a"));
                Assert.assertTrue(c.next());
                Assert.assertEquals(2L, c.getInteger("a"));
                Assert.assertTrue(!c.next());
                Assert.assertTrue(c.eof());
        });
    }

    @Test(expected = SqlJetException.class)
    public void unexistedIndexScope() throws SqlJetException {
        table.scope("unexistedIndex", new Object[] { ONE }, new Object[] { TEN });
    }

    @Test(expected = SqlJetException.class)
    public void unexistedIndexLookup() throws SqlJetException {
        table.lookup("unexistedIndex", new Object[] { TEN });
    }

    @Test(expected = SqlJetException.class)
    public void unexistedIndexOrder() throws SqlJetException {
        table.order("unexistedIndex");
    }

    @Test(expected = SqlJetException.class)
    public void unexistedIndexScope2() throws SqlJetException {
        table5.scope(null, new Object[] { ONE }, new Object[] { TEN });
    }

    @Test(expected = SqlJetException.class)
    public void unexistedIndexLookup2() throws SqlJetException {
        table5.lookup(null, new Object[] { TEN });
    }

    @Test(expected = SqlJetException.class)
    public void unexistedIndexOrder2() throws SqlJetException {
        table5.order(null);
    }
    
    @Test
    public void testNoAssertionOnGetRowId() throws SqlJetException {
        db.read().asVoid(db -> {
                for(String tableName : db.getSchema().getTableNames()) {
                    ISqlJetTable table = db.getTable(tableName);
                    Set<ISqlJetIndexDef> indices = db.getSchema().getIndexes(tableName);
                    for (ISqlJetIndexDef indexDef : indices) {
                        SqlJetScope scope = new SqlJetScope((SqlJetScopeBound) null, null);
                        ISqlJetCursor cursor = table.scope(indexDef.getName(), scope);
                        Assert.assertNotNull(cursor);
                        while(!cursor.eof()) {
                            cursor.getRowId();
                            cursor.next();
                        }
                    }
                }
        });
    }


}
