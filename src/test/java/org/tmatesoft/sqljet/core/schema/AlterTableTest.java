/**
 * AlterTableTest.java
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
package org.tmatesoft.sqljet.core.schema;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.schema.SqlJetColumnDefault;
import org.tmatesoft.sqljet.core.internal.schema.SqlJetColumnNotNull;
import org.tmatesoft.sqljet.core.simpleschema.SqlJetSimpleSchemaField;
import org.tmatesoft.sqljet.core.simpleschema.types.SqlJetSimpleBlobField;
import org.tmatesoft.sqljet.core.simpleschema.types.SqlJetSimpleIntField;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class AlterTableTest extends AbstractNewDbTest {

    private ISqlJetTable table;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final ISqlJetTableDef createTable = db.createTable("create table t(a text primary key);");
        table = db.getTable(createTable.getName());
        db.createIndex("create index i on t(a);");
        db.createTable("create table t2(a int primary key);");
    }

    @After
    public void cleanUp() throws SqlJetException {
        final SqlJetDb db2 = SqlJetDb.open(file, false);
        try {
            Assert.assertNotNull(db2);
        } finally {
            db2.close();
        }
    }
    
    @Test
    public void addFieldNoSql() throws SqlJetException {
    	final ISqlJetTableDef alterTable = db.addColumn("t", new SqlJetSimpleSchemaField("b", SqlJetSimpleIntField.getInstance(), false, 0));
        final ISqlJetTable t = db.getTable("t");
    	Assert.assertNotNull(alterTable);
    	Assert.assertNotNull(alterTable.getColumn("b"));
    	Assert.assertNotNull(table.getDefinition().getColumn("b"));
    	Assert.assertNotNull(t);
    	Assert.assertNotNull(t.getDefinition().getColumn("b"));
    	Assert.assertEquals(1, t.getDefinition().getColumn("b").getIndex());
    }
    
    @Test
    public void addField3NoSql() throws SqlJetException {
    	final ISqlJetTableDef alterTable = db.addColumn("t", 
    			SqlJetSimpleSchemaField.builder("b", SqlJetSimpleIntField.getInstance(), 0).withDefault(Integer.valueOf(0)).build());
    	final ISqlJetTable t = db.getTable("t");
    	Assert.assertNotNull(alterTable);
    	Assert.assertNotNull(alterTable.getColumn("b"));
    	Assert.assertNotNull(table.getDefinition().getColumn("b"));
    	Assert.assertNotNull(t);
    	Assert.assertNotNull(t.getDefinition().getColumn("b"));
        assertHasConstraint(SqlJetColumnDefault.class, t.getDefinition().getColumn("b").getConstraints());
    }
    
    @Test
    public void addField4NoSql() throws SqlJetException {
    	final ISqlJetTableDef alterTable = db.addColumn("t", 
    			SqlJetSimpleSchemaField.builder("b", SqlJetSimpleIntField.getInstance(), 0).withDefault(Integer.valueOf(10)).notNull().build());
    	final ISqlJetTable t = db.getTable("t");
    	Assert.assertNotNull(alterTable);
    	Assert.assertNotNull(alterTable.getColumn("b"));
    	Assert.assertNotNull(table.getDefinition().getColumn("b"));
    	Assert.assertNotNull(t);
    	Assert.assertNotNull(t.getDefinition().getColumn("b"));
    	assertHasConstraint(SqlJetColumnNotNull.class, t.getDefinition().getColumn("b").getConstraints());
    	assertHasConstraint(SqlJetColumnDefault.class, t.getDefinition().getColumn("b").getConstraints());
    }
    
    @Test(expected = SqlJetException.class)
    public void addField5NoSql() throws SqlJetException {
    	db.addColumn("t", SqlJetSimpleSchemaField.builder("b", SqlJetSimpleIntField.getInstance(), 0).notNull().build());
    }
    
    @Test(expected = SqlJetException.class)
    public void addField7NoSql() throws SqlJetException {
    	db.addColumn("t", SqlJetSimpleSchemaField.builder("b", SqlJetSimpleIntField.getInstance(), 0).unique().build());
    }
    
    @Test(expected = SqlJetException.class)
    public void addField8NoSql() throws SqlJetException {
    	db.addColumn("t", SqlJetSimpleSchemaField.builder("b", SqlJetSimpleIntField.getInstance(), 0).primaryKey().build());
    }

    @Test
    public void renameTableNoSql() throws SqlJetException {
    	final ISqlJetTableDef alterTable = db.renameTable("t", "t1");
    	Assert.assertNotNull(alterTable);
    	Assert.assertTrue("t1".equals(alterTable.getName()));
    	final ISqlJetTable t = db.getTable("t1");
    	Assert.assertNotNull(t);
    }
    
    @Test(expected = SqlJetException.class)
    public void renameTableNoSqlErrorCase1() throws SqlJetException {
    	db.renameTable(null, "t1");
    }
    
    @Test(expected = SqlJetException.class)
    public void renameTableNoSqlErrorCase2() throws SqlJetException {
    	db.renameTable("t", null);
    }

    @Test(expected = SqlJetException.class)
    public void renameTable2NoSql() throws SqlJetException {
    	db.renameTable("t", "t2");
    }

    @Test
    public void addFieldAndModifyNoSql() throws SqlJetException {
    	db.write().asVoid(x -> db.getTable("t2").insert(Long.valueOf(1L)));
    	db.write().asVoid(x -> {
    		db.addColumn("t2", new SqlJetSimpleSchemaField("b", SqlJetSimpleBlobField.getInstance(), false, 0));
    		db.getTable("t2").open().update(Long.valueOf(1L), "blob".getBytes());
    	});
    	
    	db.read().asVoid(x -> {
    		final byte[] blob = db.getTable("t2").open().getBlobAsArray("b").orElse(null);
    		Assert.assertArrayEquals("blob".getBytes(), blob);
    	});
    }

    private static void assertHasConstraint(Class<? extends ISqlJetColumnConstraint> c, List<ISqlJetColumnConstraint> constraints) {
    	Assert.assertTrue(constraints.stream().anyMatch(x -> x.getClass().equals(c)));
    }
}
