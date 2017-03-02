package org.tmatesoft.sqljet.core.simpleschema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.simpleschema.SqlJetSimpleSchemaField.FieldWithTableBuilder;
import org.tmatesoft.sqljet.core.simpleschema.types.ISqlJetSimpleFieldType;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

public class SqlJetSimpleSchemaTable {
	private final @Nonnull String name;
	private final @Nonnull List<SqlJetSimpleSchemaField> fields;
	
	private SqlJetSimpleSchemaTable(TableBuilder builder) {
		this.name = builder.name;
		this.fields = Collections.unmodifiableList(builder.fields.stream().map(f -> f.withTable(this)).collect(Collectors.toList()));
	}
	
	public void updateDb(SqlJetDb db) throws SqlJetException {
		if (db.getSchema().getTable(name) == null) {
			db.createTable(this);
		}
		for (SqlJetSimpleSchemaField field : fields) {
			field.updateDb(db);
		}
	}
	
	public @Nonnull List<SqlJetSimpleSchemaField> getFields() {
		return fields;
	}

	public Optional<SqlJetSimpleSchemaField> getField(String name) {
		return fields.stream().filter(f -> name.equals(f.getName())).findFirst();
	}

	public @Nonnull String getName() {
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
	public static TableBuilder builder(@Nonnull String name) {
		return new TableBuilder(name);
	}
	/**
	 * Builder to build {@link SqlJetSimpleSchemaTable}.
	 */
	public static final class TableBuilder {
		private final @Nonnull String name;
		private final @Nonnull List<SqlJetSimpleSchemaField> fields = new ArrayList<>();

		private TableBuilder(@Nonnull String name) {
			this.name = name;
		}

		public @Nonnull TableBuilder withField(@Nonnull String name, @Nonnull ISqlJetSimpleFieldType type) {
			this.fields.add(new SqlJetSimpleSchemaField(name, type, false, fields.size()));
			return this;
		}
		
		protected @Nonnull TableBuilder withField(@Nonnull SqlJetSimpleSchemaField field) {
			this.fields.add(field);
			return this;
		}
		
		public @Nonnull FieldWithTableBuilder withFieldBuilder(@Nonnull String name, @Nonnull ISqlJetSimpleFieldType type) {
			return SqlJetSimpleSchemaField.builder(this, name, type, fields.size());
		}
		
		public @Nonnull SqlJetSimpleSchemaTable build() {
			return new SqlJetSimpleSchemaTable(this);
		}
	}
}
