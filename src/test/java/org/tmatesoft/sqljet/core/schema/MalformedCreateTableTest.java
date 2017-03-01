/**
 * MalformedCreateTable.java
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

import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.internal.lang.SqlJetParserException;
import org.tmatesoft.sqljet.core.internal.schema.SqlJetColumnNotNull;
import org.tmatesoft.sqljet.core.simpleschema.SqlJetSimpleSchemaTable;
import org.tmatesoft.sqljet.core.simpleschema.types.SqlJetSimpleDecimalField;
import org.tmatesoft.sqljet.core.simpleschema.types.SqlJetSimpleIntField;
import org.tmatesoft.sqljet.core.simpleschema.types.SqlJetSimpleVarCharField;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class MalformedCreateTableTest extends AbstractNewDbTest {

    private static final double EPSILON = 1E-6;

	@Test
    public void malformedCreateTable() throws Exception {

        db.getOptions().setAutovacuum(true);
        db.write().asVoid(db -> db.getOptions().setUserVersion(1));
        String sql1 = "CREATE TABLE TESTXX (a int, b int, c int, " + "d int, blob blob, PRIMARY KEY (a,b,c,d))";
        String sql2 = "CREATE INDEX IND on TESTXX (a,b,c,d)";
        db.createTable(sql1);
        db.createIndex(sql2);

        db.close();
        db = SqlJetDb.open(file, true);
        Assert.assertTrue(db != null);
    }

    @Test
    public void malformedCreateTableIfNotExists() throws Exception {
        db.getOptions().setAutovacuum(true);
        db.write().asVoid(db -> db.getOptions().setUserVersion(1));
        String sql1 = "CREATE TABLE IF NOT EXISTS TESTXX (a int, b int, c int, "
                + "d int, blob blob, PRIMARY KEY (a,b,c,d))";
        String sql2 = "CREATE INDEX IF NOT EXISTS IND on TESTXX (a,b,c,d)";
        db.createTable(sql1);
        db.createIndex(sql2);

        db.close();
        db = SqlJetDb.open(file, true);
        Assert.assertTrue(db != null);

    }

    @Test
    public void malformedCreateTableExistsFail() throws Exception {
        db.getOptions().setAutovacuum(true);
        db.write().asVoid(db -> db.getOptions().setUserVersion(1));
        String sql1 = "CREATE TABLE IF NOT EXISTS TESTXX (a int, b int, c int, "
                + "d int, blob blob, PRIMARY KEY (a,b,c,d))";
        String sql2 = "CREATE INDEX IF NOT EXISTS IND on TESTXX (a,b,c,d)";
        db.createTable(sql1);
        db.createIndex(sql2);
        db.createTable(sql1);// twice
        db.createIndex(sql2);
    }

    @Test
    public void nullTableFieldConstraintTest() throws Exception {
        String sql1 = "CREATE TABLE world_countries (Name varchar(300) NULL, ID int NULL)";
        db.createTable(sql1);
        
        final ISqlJetTable table = db.getTable("world_countries");
        Assert.assertNotNull(table);
        
        assertVarchar(table, "Name", 300);
        assertInt(table, "id");
    }

    @Test
    public void fieldsSquareNamesTest() throws Exception {
        String sql1 = "CREATE TABLE [dimensions_2] ( [id] int NOT NULL, [Dimension_Name] varchar(30) NULL,"
                + "[Type_ID] int NOT NULL ) ";
        db.createTable(sql1);
        final ISqlJetTable table = db.getTable("dimensions_2");
        Assert.assertNotNull(table);
        
        assertIntNotNull(table, "id");
        assertIntNotNull(table, "Type_ID");
    }
    
    @Test
    public void doubleSizeGivenInType() throws Exception {
    	String sql1 = "CREATE TABLE test ( id int NOT NULL, val decimal(15,2) ) ";
    	db.createTable(sql1);
    	final ISqlJetTable table = db.getTable("test");
    	Assert.assertNotNull(table);
    	
        assertIntNotNull(table, "id");
    	
    	ISqlJetColumnDef valCol = table.getDefinition().getColumn("val");
    	Assert.assertEquals(Collections.singletonList("decimal"), valCol.getType().getNames());
    	Assert.assertEquals(15.0, valCol.getType().getSize1().doubleValue(), EPSILON);
    	Assert.assertEquals(2.0, valCol.getType().getSize2().doubleValue(), EPSILON);
    }
    
    @Test
    public void doubleSizeGivenInTypeSimpleSchema() throws Exception {
    	SqlJetSimpleSchemaTable schema = SqlJetSimpleSchemaTable.builder("test")
    		.withFieldBuilder("id", SqlJetSimpleIntField.getInstance()).notNull().build()
    		.withField("val", new SqlJetSimpleDecimalField(15, 2))
    		.build();
    	db.createTable(schema);
    	final ISqlJetTable table = db.getTable("test");
    	Assert.assertNotNull(table);
    	
    	assertIntNotNull(table, "id");
    	
    	ISqlJetColumnDef valCol = table.getDefinition().getColumn("val");
    	Assert.assertEquals(Collections.singletonList("decimal"), valCol.getType().getNames());
    	Assert.assertEquals(15.0, valCol.getType().getSize1().doubleValue(), EPSILON);
    	Assert.assertEquals(2.0, valCol.getType().getSize2().doubleValue(), EPSILON);
    }

    @Test
    public void tableNameWithWhitespaceTest() throws Exception {
        String sql1 = "CREATE TABLE \"name with whitespace\" ( \"id\" int NOT NULL, \"Dimension_Name\" varchar(30) NULL,"
                + "\"Type_ID\" int NOT NULL ) ";
        db.createTable(sql1);
        
        final ISqlJetTable table = db.getTable("name with whitespace");
        Assert.assertNotNull(table);
        assertIntNotNull(table, "id");
		
        assertVarchar(table, "Dimension_Name", 30);
    }
    
    @Test
    public void tableNameWithWhitespaceTestSimpleSchema() throws Exception {
    	SqlJetSimpleSchemaTable s = SqlJetSimpleSchemaTable.builder("name with whitespace")
    		.withFieldBuilder("id", SqlJetSimpleIntField.getInstance()).notNull().build()
    		.withField("Dimension_Name", new SqlJetSimpleVarCharField(30))
    		.build();
    	
    	db.createTable(s);
    	final ISqlJetTable table = db.getTable("name with whitespace");
    	Assert.assertNotNull(table);
        assertIntNotNull(table, "id");
		
        assertVarchar(table, "Dimension_Name", 30);
    }

    @Test
    public void fieldsDoubleQuotesNamesTest() throws Exception {
        String sql1 = "CREATE TABLE FUSION_MAP_COUNTRIES ( ID VARCHAR2(20) NOT NULL,"
                + " \"SHORT_NAME\" VARCHAR2(20) NOT NULL," + "\"ISO_CODE\" VARCHAR2(10) NOT NULL,"
                + "\"COUNTRY_NAME\" VARCHAR2(100) NOT NULL," + "\"MAP\" VARCHAR2(100) NOT NULL,"
                + "\"DRILLDOWN\" VARCHAR2(100),"
                + "CONSTRAINT \"PK_FUSION_MAP_COUNTRIES\" PRIMARY KEY (\"ID\", \"MAP\")" + ")";
        db.createTable(sql1);
        final ISqlJetTable table = db.getTable("FUSION_MAP_COUNTRIES");
        Assert.assertNotNull(table);
        
        assertVarcharNotNull(table, "ID", 20);
        assertVarcharNotNull(table, "SHORT_NAME", 20);
        assertVarcharNotNull(table, "ISO_CODE", 10);
        assertVarcharNotNull(table, "COUNTRY_NAME", 100);
        assertVarcharNotNull(table, "MAP", 100);
        assertVarchar(table, "DRILLDOWN", 100);
    }

    @Test
    public void fieldSize() throws Exception {
        final String sql = "CREATE TABLE SITE_VARS (SITEID VARCHAR (10) NOT NULL,"
                + " VARNAME VARCHAR (50) NOT NULL, VALUE VARCHAR (500))";
        final ISqlJetTableDef t = db.createTable(sql);
        final String sql2 = t.toSQL();
        Assert.assertEquals(sql, sql2);
        
        final ISqlJetTable table = db.getTable("SITE_VARS");
        Assert.assertNotNull(table);
        
        assertVarcharNotNull(table, "SITEID", 10);
        assertVarcharNotNull(table, "VARNAME", 50);
        assertVarchar(table, "VALUE", 500);
    }

    @Test
    public void dollarInName() throws Exception {
        final String sql = "create table my$table(a$ integer PRIMARY KEY AUTOINCREMENT, b$ integer)";
        final ISqlJetTableDef def = db.createTable(sql);
        Assert.assertNotNull(def);
        final ISqlJetTable t = db.getTable("my$table");
        Assert.assertNotNull(t);
        
        assertInt(t, "a$");
        assertInt(t, "b$");
        
        Assert.assertTrue(t.getDefinition().isAutoincremented());
    }
    
    @Test
    public void dollarInNameSimpleSchema() throws Exception {
    	SqlJetSimpleSchemaTable schema = SqlJetSimpleSchemaTable.builder("my$table")
    		.withFieldBuilder("a$", SqlJetSimpleIntField.getInstance()).primaryKeyAutoincrement().build()
    		.withField("b$", SqlJetSimpleIntField.getInstance())
    		.build();
    	final ISqlJetTableDef def = db.createTable(schema);
    	Assert.assertNotNull(def);
    	final ISqlJetTable t = db.getTable("my$table");
    	Assert.assertNotNull(t);
    	
    	assertInt(t, "a$");
    	assertInt(t, "b$");
    	
    	Assert.assertTrue(t.getDefinition().isAutoincremented());
    }

    @Test(expected = SqlJetParserException.class)
    public void dollarInNameFail() throws Exception {
        final String sql = "create table $mytable($a integer primary key, $b integer)";
        db.createTable(sql);
    }
    
    @Test
    public void parseExceptionMessage() throws Exception {
        final String sql = "it's wrong sql";
        try {
            db.createTable(sql);
            Assert.fail();
        } catch (SqlJetParserException e) {
            final String msg = e.getMessage();
            Assert.assertTrue(msg.contains(sql));
        }
    }

    @Test
    public void tableNameWithWhitespaceTest2() throws Exception {
        String sql1 = "CREATE \n TABLE \"name with whitespace\" ( \"id\" int NOT NULL,"
                + " \"Dimension Name\" varchar(30) NULL," + "\"Type ID\" int NOT NULL )  ; ";
        db.createTable(sql1);
        final ISqlJetTable table = db.getTable("name with whitespace");
        Assert.assertNotNull(table);
        
        assertIntNotNull(table, "id");
        assertVarchar(table, "Dimension Name", 30);
        assertIntNotNull(table, "Type ID");
    }
    
    @Test
    public void tableNameWithWhitespaceTest2SimpleSchema() throws Exception {
    	SqlJetSimpleSchemaTable schema = SqlJetSimpleSchemaTable.builder("name with whitespace")
    			.withFieldBuilder("id", SqlJetSimpleIntField.getInstance()).notNull().build()
    			.withField("Dimension Name", new SqlJetSimpleVarCharField(30))
    			.withFieldBuilder("Type ID", SqlJetSimpleIntField.getInstance()).notNull().build()
    			.build();
    	db.createTable(schema);
    	
    	final ISqlJetTable table = db.getTable("name with whitespace");
    	Assert.assertNotNull(table);
    	
    	assertIntNotNull(table, "id");
    	assertVarchar(table, "Dimension Name", 30);
    	assertIntNotNull(table, "Type ID");
    }

    @Test
    public void indexNameWithWhitespaceTest() throws Exception {
        db.beginTransaction(SqlJetTransactionMode.WRITE);
        String sql1 = "CREATE \n TABLE \"name with whitespace\" ( \"id\" int NOT NULL,"
                + " \"Dimension Name\" varchar(30) NULL," + "\"Type ID\" int NOT NULL )  ; ";
        db.createTable(sql1);
        String sql2 = "CREATE \n INDEX \"name with whitespace 2\" on \"name with whitespace\" ( "
                + " \"Dimension Name\")  ; ";
        db.createIndex(sql2);
        db.commit();
        final ISqlJetTable table = db.getTable("name with whitespace");
        Assert.assertNotNull(table);
        final ISqlJetIndexDef indexDef = table.getIndexDef("name with whitespace 2");
        Assert.assertNotNull(indexDef);
    }

    @Test
    public void virtualTableNameWithWhitespaceTest() throws Exception {
        db.beginTransaction(SqlJetTransactionMode.WRITE);
        String sql1 = "CREATE \n VIRTUAL TABLE \"name with whitespace\" using \"module name\" ( \"id\" int NOT NULL,"
                + " \"Dimension Name\" varchar(30) NULL," + "\"Type ID\" int NOT NULL )  ; ";
        db.createVirtualTable(sql1);
        db.commit();
        final ISqlJetSchema schema = db.getSchema();
        final ISqlJetVirtualTableDef virtualTable = schema.getVirtualTable("name with whitespace");
        Assert.assertNotNull(virtualTable);
    }

    @Test
    public void alterTableAddColumnNameWithWhitespaceTest() throws Exception {
        db.beginTransaction(SqlJetTransactionMode.WRITE);
        String sql1 = "CREATE \n TABLE \"name with whitespace\" ( \"id\" int NOT NULL,"
                + " \"Dimension Name\" varchar(30) NULL," + "\"Type ID\" int NOT NULL )  ; ";
        db.createTable(sql1);
        String sql2 = "CREATE \n INDEX \"name with whitespace 2\" on \"name with whitespace\" ( "
                + " \"Dimension Name\")  ; ";
        db.createIndex(sql2);
        db.commit();
        db.alterTable("alter table \"name with whitespace\" add column \"column with space\"");
        final ISqlJetTable table = db.getTable("name with whitespace");
        Assert.assertNotNull(table);
        final ISqlJetIndexDef indexDef = table.getIndexDef("name with whitespace 2");
        Assert.assertNotNull(indexDef);
    }

    @Test
    public void alterTableRenameNameWithWhitespaceTest() throws Exception {
        String sql1 = "CREATE \n TABLE \"name with whitespace\" ( \"id\" int NOT NULL,"
                + " \"Dimension Name\" varchar(30) NULL," + "\"Type ID\" int NOT NULL )  ; ";
        db.createTable(sql1);
        String sql2 = "CREATE \n INDEX \"name with whitespace 2\" on \"name with whitespace\" ( "
                + " \"Dimension Name\")  ; ";
        db.createIndex(sql2);
        db.alterTable("alter table \"name with whitespace\" rename to \"name with whitespace 3\"");
        final ISqlJetTable table = db.getTable("name with whitespace 3");
        Assert.assertNotNull(table);
        final ISqlJetTable table2 = db.getTable("name with whitespace 3");
        Assert.assertNotNull(table2);
        final ISqlJetIndexDef indexDef = table.getIndexDef("name with whitespace 2");
        Assert.assertNotNull(indexDef);
    }

    @Test
    public void tableNameWithWhitespaceTest3() throws Exception {
        db.beginTransaction(SqlJetTransactionMode.WRITE);
        String sql1 = "CREATE \n TABLE \" name with \n whitespace \" ( \" id \" int NOT NULL,"
                + " \" Dimension, Name\" varchar(30) NULL," + "\" Type; ID \" int NOT NULL )  ; ";
        db.createTable(sql1);
        db.commit();
        final ISqlJetTable table = db.getTable(" name with \n whitespace ");
        Assert.assertNotNull(table);
    }

    @Test
    public void tableNameQuotedApostrophe() throws Exception {
        db.beginTransaction(SqlJetTransactionMode.WRITE);
        String sql1 = "CREATE \n TABLE ` [name with \" \n whitespace] ` ( ` id ` int NOT NULL,"
                + " ` Dimension, Name ` varchar(30) NULL, ` Type; ID ` int NOT NULL )  ; ";
        db.createTable(sql1);
        db.commit();
        final ISqlJetTable table = db.getTable(" [name with \" \n whitespace] ");
        Assert.assertNotNull(table);
    }

    @Test
    public void tableNameQuotedSingle() throws Exception {
        db.beginTransaction(SqlJetTransactionMode.WRITE);
        String sql1 = "CREATE \n TABLE ' [name with \" \n whitespace] ' ( ' id ' int NOT NULL,"
                + " ' Dimension, Name ' varchar(30) NULL, ' Type; ID ' int NOT NULL )  ; ";
        db.createTable(sql1);
        db.commit();
        final ISqlJetTable table = db.getTable(" [name with \" \n whitespace] ");
        Assert.assertNotNull(table);
    }

    @Test(expected = SqlJetException.class)
    public void tableNameConflict() throws SqlJetException {
        db.createTable("create table t(a integer primary key, b text)");
        db.createTable("create table t(a integer primary key, b text)");
    }

    @Test(expected = SqlJetException.class)
    public void indexNameConflict() throws SqlJetException {
        db.createTable("create table t(a integer primary key, b text)");
        db.createIndex("create index i on t(b)");
        db.createIndex("create index i on t(b)");
    }

    @Test(expected = SqlJetException.class)
    public void tableIndexNameConflict1() throws SqlJetException {
        db.createTable("create table t(a integer primary key, b text)");
        db.createIndex("create index t on t(b)");
    }

    @Test(expected = SqlJetException.class)
    public void tableIndexNameConflict2() throws SqlJetException {
        db.createTable("create table t(a integer primary key, b text)");
        db.createIndex("create index i on t(b)");
        db.createTable("create table i(a integer primary key, b text)");
    }

    @Test(expected = SqlJetException.class)
    public void tableNameReserved() throws SqlJetException {
        db.createTable("create table sqlite_master(a integer primary key, b text)");
    }

    @Test(expected = SqlJetException.class)
    public void indexNameReserved() throws SqlJetException {
        db.createTable("create table t(b text)");
        db.createIndex("create index sqlite_autoindex_t_1 on t(b)");
    }

    @Test(expected = SqlJetException.class)
    public void tableVirtualNameReserved() throws SqlJetException {
        db.createTable("create virtual table sqlite_master using sqljetmap");
    }

    @Test(expected = SqlJetException.class)
    public void virtualTableNameConflict() throws SqlJetException {
        db.createTable("create table t(a integer primary key, b text)");
        db.createVirtualTable("create virtual table t using sqljetmap");
    }

    @Test(expected = SqlJetException.class)
    public void virtualTableNameConflict2() throws SqlJetException {
        db.createVirtualTable("create virtual table t using sqljetmap");
        db.createTable("create table t(a integer primary key, b text)");
    }

    @Test(expected = SqlJetException.class)
    public void virtualTableNameConflict3() throws SqlJetException {
        db.createVirtualTable("create virtual table t using sqljetmap");
        db.createIndex("create index t on t(b)");
    }

    @Test(expected = SqlJetException.class)
    public void virtualTableNameConflict4() throws SqlJetException {
        db.createIndex("create index t on t(b)");
        db.createVirtualTable("create virtual table t using sqljetmap");
    }

	private static void assertIntNotNull(final ISqlJetTable table, String column) throws SqlJetException {
		ISqlJetColumnDef col = table.getDefinition().getColumn(column);
		assertSingletonListIgnoreCase("int", col.getType().getNames());
		Assert.assertEquals(1, col.getConstraints().size());
		Assert.assertEquals(SqlJetColumnNotNull.class, col.getConstraints().get(0).getClass());
	}
	
	private static void assertInt(final ISqlJetTable table, String column) throws SqlJetException {
		ISqlJetColumnDef col = table.getDefinition().getColumn(column);
		assertSingletonListIgnoreCase("int", col.getType().getNames());
	}

	private static void assertVarchar(final ISqlJetTable table, String column, int length) throws SqlJetException {
		ISqlJetColumnDef col = table.getDefinition().getColumn(column);
		assertSingletonListIgnoreCase("varchar", col.getType().getNames());
    	Assert.assertEquals(length, col.getType().getSize1().doubleValue(), EPSILON);
    	Assert.assertNull(col.getType().getSize2());
	}
	
	private static void assertVarcharNotNull(final ISqlJetTable table, String column, int length) throws SqlJetException {
		ISqlJetColumnDef col = table.getDefinition().getColumn(column);
		assertSingletonListIgnoreCase("varchar", col.getType().getNames());
		Assert.assertEquals(length, col.getType().getSize1().doubleValue(), EPSILON);
		Assert.assertNull(col.getType().getSize2());
		Assert.assertEquals(1, col.getConstraints().size());
		Assert.assertEquals(SqlJetColumnNotNull.class, col.getConstraints().get(0).getClass());
	}

	private static void assertSingletonListIgnoreCase(String expected, List<String> list) {
		Assert.assertEquals(1, list.size());
		Assert.assertTrue("The given list: "+list+" doesn't contain the expected value: "+expected, list.get(0).toLowerCase().contains(expected.toLowerCase()));
	}
}
