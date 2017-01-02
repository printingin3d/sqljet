package org.tmatesoft.sqljet.core.simpleschema;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableDef;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

public class SqlJetSimpleSchemaField {
	private final SqlJetSimpleSchemaTable table;
	private final String name;
	private final String type;
	private final boolean indexed;
	
	private SqlJetSimpleSchemaField(SqlJetSimpleSchemaTable table, String name, String type, boolean indexed) {
		this.table = table;
		this.name = name;
		this.type = type;
		this.indexed = indexed;
	}

	public SqlJetSimpleSchemaField(String name, String type, boolean indexed) {
		this(null, name, type, indexed);
	}
	
	public SqlJetSimpleSchemaField withTable(SqlJetSimpleSchemaTable table) {
		return new SqlJetSimpleSchemaField(table, name, type, indexed);
	}
	
	public void updateDb(SqlJetDb db) throws SqlJetException {
		ISqlJetTableDef sqlJetTable = db.getSchema().getTable(table.getName());
		if (sqlJetTable.getColumn(name) == null) {
			db.alterTable("ALTER TABLE " + sqlJetTable.getName() + " ADD " + toSql());
		}
		if (indexed && db.getSchema().getIndex(indexName()) == null) {
			db.createIndex("CREATE INDEX " + indexName() + " ON " + sqlJetTable.getName() + "(" + name + ")");
		}
	}
	
	public SqlJetSimpleSchemaTable getTable() {
		return table;
	}

	public String getName() {
		return name;
	}

	public String indexName() {
		return table.getName() + "__" + name;
	}
	
	public String toSql() {
		return name + " " + type;
	}
}
