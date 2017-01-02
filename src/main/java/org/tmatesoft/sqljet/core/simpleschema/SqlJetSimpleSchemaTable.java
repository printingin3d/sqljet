package org.tmatesoft.sqljet.core.simpleschema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

public class SqlJetSimpleSchemaTable {
	private final String name;
	private final List<SqlJetSimpleSchemaField> fields;
	
	private SqlJetSimpleSchemaTable(Builder builder) {
		this.name = builder.name;
		this.fields = builder.fields.stream().map(f -> f.withTable(this)).collect(Collectors.toList());
	}
	
	public void updateDb(SqlJetDb db) throws SqlJetException {
		if (db.getSchema().getTable(name) == null) {
			db.createTable(toSql());
		}
		for (SqlJetSimpleSchemaField field : fields) {
			field.updateDb(db);
		}
	}
	
	public Optional<SqlJetSimpleSchemaField> getField(String name) {
		return fields.stream().filter(f -> name.equals(f.getName())).findFirst();
	}

	public String getName() {
		return name;
	}

	public String toSql() {
		return new StringBuilder("CREATE TABLE ")
				.append(name)
				.append('(')
				.append(fields.stream().map(SqlJetSimpleSchemaField::toSql).reduce((a,b) -> a+","+b).orElse(""))
				.append(')')
				.toString();
	}
	
	/**
	 * Creates builder to build {@link SqlJetSimpleSchemaTable}.
	 * @return created builder
	 */
	public static Builder builder(String name) {
		return new Builder(name);
	}
	/**
	 * Builder to build {@link SqlJetSimpleSchemaTable}.
	 */
	public static final class Builder {
		private final String name;
		private final List<SqlJetSimpleSchemaField> fields = new ArrayList<>();

		private Builder(String name) {
			this.name = name;
		}

		public Builder withField(String name, String type) {
			this.fields.add(new SqlJetSimpleSchemaField(name, type, false));
			return this;
		}
		
		public Builder withIndexedField(String name, String type) {
			this.fields.add(new SqlJetSimpleSchemaField(name, type, true));
			return this;
		}
		
		public SqlJetSimpleSchemaTable build() {
			return new SqlJetSimpleSchemaTable(this);
		}
	}
}
