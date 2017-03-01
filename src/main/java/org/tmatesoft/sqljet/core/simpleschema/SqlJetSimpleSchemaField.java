package org.tmatesoft.sqljet.core.simpleschema;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnConstraint;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetTypeDef;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;
import org.tmatesoft.sqljet.core.simpleschema.SqlJetSimpleSchemaTable.TableBuilder;
import org.tmatesoft.sqljet.core.simpleschema.types.ISqlJetSimpleFieldType;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

public class SqlJetSimpleSchemaField implements ISqlJetColumnDef {
	private final SqlJetSimpleSchemaTable table;
	private final @Nonnull String name;
	private final @Nonnull ISqlJetSimpleFieldType type;
	private final boolean indexed;
	private final int colNumber;
	private final @Nonnull Set<SqlJetSimpleColumnContraint> constraints;
	
	private SqlJetSimpleSchemaField(SqlJetSimpleSchemaTable table, @Nonnull String name, 
			@Nonnull ISqlJetSimpleFieldType type, boolean indexed, int colNumber,
			@Nonnull Set<SqlJetSimpleColumnContraint> constraints) {
		this.table = table;
		this.name = name;
		this.type = type;
		this.indexed = indexed;
		this.colNumber = colNumber;
		this.constraints = constraints;
	}

	public SqlJetSimpleSchemaField(@Nonnull String name, @Nonnull ISqlJetSimpleFieldType type, 
			boolean indexed, int colNumber) {
		this(null, name, type, indexed, colNumber, Collections.emptySet());
	}
	
	public SqlJetSimpleSchemaField withTable(SqlJetSimpleSchemaTable table) {
		return new SqlJetSimpleSchemaField(table, name, type, indexed, colNumber, constraints);
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

	@Override
	public @Nonnull String getName() {
		return name;
	}

	public @Nonnull String indexName() {
		return table.getName() + "__" + name;
	}
	
	public @Nonnull String toSql() {
		return name + " " + type.toSql();
	}

	
	@Override
	public String getQuotedName() {
		return getName();
	}

	@Override
	public ISqlJetTypeDef getType() {
		return type.toInnerRepresentation();
	}

	@Override
	public SqlJetTypeAffinity getTypeAffinity() {
		return type.getTypeAffinity();
	}

	@Override
	public boolean hasExactlyIntegerType() {
		return type.isInteger();
	}

	@SuppressWarnings("null")
	@Override
	public @Nonnull List<ISqlJetColumnConstraint> getConstraints() {
		return constraints.stream().map(x -> x.toColumnConstraint(this)).collect(Collectors.toList());
	}

	@Override
	public int getIndex() {
		return colNumber;
	}

	@Override
	public void setIndex(int index) {
		// do nothing - the index is given when the fields are created
	}
	
	public static @Nonnull FieldBuilder builder(@Nonnull TableBuilder tableBuilder, @Nonnull String name, 
			@Nonnull ISqlJetSimpleFieldType type, int colNumber) {
		return new FieldBuilder(tableBuilder, name, type, colNumber);
	}
	
	public static class FieldBuilder {
		private final @Nonnull TableBuilder tableBuilder; 
		private final @Nonnull String name;
		private final @Nonnull ISqlJetSimpleFieldType type;
		private boolean indexed = false;
		private final int colNumber;
		private final @Nonnull Set<SqlJetSimpleColumnContraint> constraints = EnumSet.noneOf(SqlJetSimpleColumnContraint.class);
		
		public FieldBuilder(@Nonnull TableBuilder tableBuilder, @Nonnull String name, 
				@Nonnull ISqlJetSimpleFieldType type, int colNumber) {
			this.tableBuilder = tableBuilder;
			this.name = name;
			this.type = type;
			this.colNumber = colNumber;
		}
		
		public FieldBuilder notNull() {
			constraints.add(SqlJetSimpleColumnContraint.NOT_NULL);
			return this;
		}
		
		public FieldBuilder primaryKeyAutoincrement() {
			constraints.add(SqlJetSimpleColumnContraint.AUTOINCREMENTED_PRIMARY_KEY);
			return this;
		}
		
		public FieldBuilder primaryKey() {
			constraints.add(SqlJetSimpleColumnContraint.PRIMARY_KEY);
			return this;
		}
		
		public FieldBuilder indexed() {
			this.indexed = true;
			return this;
		}
		
		public TableBuilder build() {
			return tableBuilder.withField(new SqlJetSimpleSchemaField(null, name, type, indexed, colNumber, constraints));
		}
	}
}
