package org.tmatesoft.sqljet.core.simpleschema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableDef;
import org.tmatesoft.sqljet.core.simpleschema.types.SqlJetSimpleIntField;
import org.tmatesoft.sqljet.core.simpleschema.types.SqlJetSimpleTextField;

public class SqlJetSimpleSchemaTableTest extends AbstractNewDbTest {
	@Test
	public void sqlTest() {
		String sql = SqlJetSimpleSchemaTable.builder("test")
			.withField("field1", SqlJetSimpleTextField.getInstance())
			.withFieldBuilder("field2", SqlJetSimpleIntField.getInstance()).indexed().build()
			.build().toSql();
		assertEquals("CREATE TABLE test(field1 text,field2 integer)", sql);
	}
	
	@Test
	public void createTableTest() throws SqlJetException {
		SqlJetSimpleSchemaTable table = SqlJetSimpleSchemaTable.builder("test")
				.withField("field1", SqlJetSimpleTextField.getInstance())
				.withFieldBuilder("field2", SqlJetSimpleIntField.getInstance()).indexed().build()
				.build();
		
		table.updateDb(db);
		
		ISqlJetTableDef testTable = db.getSchema().getTable("test");
		assertNotNull(testTable);
		assertNotNull(testTable.getColumn("field1"));
		assertNotNull(testTable.getColumn("field2"));
		assertNotNull(db.getSchema().getIndex("test__field2"));
	}
	
	@Test
	public void autoincrementTest() throws SqlJetException {
		SqlJetSimpleSchemaTable table = SqlJetSimpleSchemaTable.builder("test")
				.withFieldBuilder("field1", SqlJetSimpleIntField.getInstance()).primaryKeyAutoincrement().build()
				.withFieldBuilder("field2", SqlJetSimpleIntField.getInstance()).indexed().build()
				.build();
		
		table.updateDb(db);
		
		ISqlJetTableDef testTable = db.getSchema().getTable("test");
		assertNotNull(testTable);
		assertNotNull(testTable.getColumn("field1"));
		assertNotNull(testTable.getColumn("field2"));
		assertTrue(db.getTable("test").getDefinition().isAutoincremented());
	}
	
	@Test
	public void alterTableTest() throws SqlJetException {
		SqlJetSimpleSchemaTable table1 = SqlJetSimpleSchemaTable.builder("test")
				.withField("field1", SqlJetSimpleTextField.getInstance())
				.build();
		
		table1.updateDb(db);
		
		assertNull(db.getSchema().getTable("test").getColumn("field2"));

		SqlJetSimpleSchemaTable table2 = SqlJetSimpleSchemaTable.builder("test")
				.withField("field1", SqlJetSimpleTextField.getInstance())
				.withFieldBuilder("field2", SqlJetSimpleIntField.getInstance()).indexed().build()
				.build();

		table2.updateDb(db);
		
		assertNotNull(db.getSchema().getTable("test").getColumn("field2"));
		assertNotNull(db.getSchema().getIndex("test__field2"));
	}
	
	@Test
	public void addIndexTest() throws SqlJetException {
		SqlJetSimpleSchemaTable table1 = SqlJetSimpleSchemaTable.builder("test")
				.withField("field1", SqlJetSimpleTextField.getInstance())
				.withField("field2", SqlJetSimpleIntField.getInstance())
				.build();
		
		table1.updateDb(db);
		
		assertNotNull(db.getSchema().getTable("test").getColumn("field2"));
		assertNull(db.getSchema().getIndex("test__field2"));
		
		SqlJetSimpleSchemaTable table2 = SqlJetSimpleSchemaTable.builder("test")
				.withField("field1", SqlJetSimpleTextField.getInstance())
				.withFieldBuilder("field2", SqlJetSimpleIntField.getInstance()).indexed().build()
				.build();
		
		table2.updateDb(db);
		
		assertNotNull(db.getSchema().getTable("test").getColumn("field2"));
		assertNotNull(db.getSchema().getIndex("test__field2"));
	}
}
