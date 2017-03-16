package org.tmatesoft.sqljet.core.simpleschema;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.schema.SqlJetBlobLiteral;
import org.tmatesoft.sqljet.core.internal.schema.SqlJetColumnDefault;
import org.tmatesoft.sqljet.core.internal.schema.SqlJetFloatLiteral;
import org.tmatesoft.sqljet.core.internal.schema.SqlJetIntegerLiteral;
import org.tmatesoft.sqljet.core.internal.schema.SqlJetNullLiteral;
import org.tmatesoft.sqljet.core.internal.schema.SqlJetStringLiteral;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnConstraint;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetLiteralValue;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetTypeDef;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;
import org.tmatesoft.sqljet.core.simpleschema.SqlJetSimpleSchemaTable.TableBuilder;
import org.tmatesoft.sqljet.core.simpleschema.types.ISqlJetSimpleFieldType;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

/**
 * Immutable representation of a field.
 */
public class SqlJetSimpleSchemaField implements ISqlJetColumnDef {
    private final SqlJetSimpleSchemaTable table;
    private final @Nonnull String name;
    private final @Nonnull ISqlJetSimpleFieldType type;
    private final boolean indexed;
    private final Object defaultVal;
    private final int colNumber;
    private final @Nonnull Set<SqlJetSimpleColumnContraint> constraints;

    private SqlJetSimpleSchemaField(SqlJetSimpleSchemaTable table, @Nonnull String name,
            @Nonnull ISqlJetSimpleFieldType type, boolean indexed, int colNumber,
            @Nonnull Set<SqlJetSimpleColumnContraint> constraints, Object defaultVal) {
        this.table = table;
        this.name = name;
        this.type = type;
        this.indexed = indexed;
        this.colNumber = colNumber;
        this.constraints = constraints;
        this.defaultVal = defaultVal;
    }

    public SqlJetSimpleSchemaField(@Nonnull String name, @Nonnull ISqlJetSimpleFieldType type, boolean indexed,
            int colNumber) {
        this(null, name, type, indexed, colNumber, Collections.emptySet(), null);
    }

    public SqlJetSimpleSchemaField withTable(SqlJetSimpleSchemaTable table) {
        return new SqlJetSimpleSchemaField(table, name, type, indexed, colNumber, constraints, defaultVal);
    }

    public void updateDb(SqlJetDb db) throws SqlJetException {
        ISqlJetTableDef sqlJetTable = db.getSchema().getTable(table.getName());
        if (sqlJetTable.getColumn(name) == null) {
            db.addColumn(sqlJetTable.getName(), this);
        }
        if (indexed && db.getSchema().getIndex(indexName()) == null) {
            db.createIndex(indexName(), sqlJetTable.getName(), name, false, false);
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

    private ISqlJetLiteralValue getDefaultExpression() {
        if (defaultVal == null) {
            return SqlJetNullLiteral.getInstance();
        }
        if (defaultVal instanceof String) {
            return new SqlJetStringLiteral((String) defaultVal);
        }
        if (defaultVal instanceof Long || defaultVal instanceof Integer) {
            return new SqlJetIntegerLiteral(((Number) defaultVal).longValue());
        }
        if (defaultVal instanceof Number) {
            return new SqlJetFloatLiteral(((Number) defaultVal).doubleValue());
        }
        return new SqlJetBlobLiteral(defaultVal.toString());
    }

    @SuppressWarnings("null")
    @Override
    public @Nonnull List<ISqlJetColumnConstraint> getConstraints() {
        return Stream.concat(
                defaultVal == null ? Stream.empty()
                        : Stream.of(new SqlJetColumnDefault(this, name, getDefaultExpression())),
                constraints.stream().map(x -> x.toColumnConstraint(this))).collect(Collectors.toList());
    }

    @Override
    public int getIndex() {
        return colNumber;
    }

    @Override
    public ISqlJetColumnDef updateIndex(int index) {
        if (index == this.colNumber) {
            return this;
        }
        return new SqlJetSimpleSchemaField(table, name, type, indexed, index, constraints, defaultVal);
    }

    public static @Nonnull FieldWithTableBuilder builder(@Nonnull TableBuilder tableBuilder, @Nonnull String name,
            @Nonnull ISqlJetSimpleFieldType type, int colNumber) {
        return new FieldWithTableBuilder(tableBuilder, name, type, colNumber);
    }

    public static @Nonnull FieldBuilder builder(@Nonnull String name, @Nonnull ISqlJetSimpleFieldType type,
            int colNumber) {
        return new FieldBuilder(name, type, colNumber);
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getQuotedName());
        if (getType() != null) {
            buffer.append(' ').append(getType());
        }
        for (ISqlJetColumnConstraint c : getConstraints()) {
            buffer.append(' ').append(c);
        }
        return buffer.toString();
    }

    public static class FieldBuilder {
        private final @Nonnull String name;
        private final @Nonnull ISqlJetSimpleFieldType type;
        private boolean indexed = false;
        private final int colNumber;
        private final @Nonnull Set<SqlJetSimpleColumnContraint> constraints = EnumSet
                .noneOf(SqlJetSimpleColumnContraint.class);
        private Object defaultVal;

        public FieldBuilder(@Nonnull String name, @Nonnull ISqlJetSimpleFieldType type, int colNumber) {
            this.name = name;
            this.type = type;
            this.colNumber = colNumber;
        }

        public @Nonnull FieldBuilder notNull() {
            constraints.add(SqlJetSimpleColumnContraint.NOT_NULL);
            return this;
        }

        public @Nonnull FieldBuilder unique() {
            constraints.add(SqlJetSimpleColumnContraint.UNIQUE);
            return this;
        }

        public @Nonnull FieldBuilder primaryKeyAutoincrement() {
            constraints.add(SqlJetSimpleColumnContraint.AUTOINCREMENTED_PRIMARY_KEY);
            return this;
        }

        public @Nonnull FieldBuilder primaryKey() {
            constraints.add(SqlJetSimpleColumnContraint.PRIMARY_KEY);
            return this;
        }

        public @Nonnull FieldBuilder indexed() {
            this.indexed = true;
            return this;
        }

        public @Nonnull FieldBuilder withDefault(Object defaultVal) {
            this.defaultVal = defaultVal;
            return this;
        }

        public @Nonnull SqlJetSimpleSchemaField build() {
            return new SqlJetSimpleSchemaField(null, name, type, indexed, colNumber, constraints, defaultVal);
        }
    }

    public static class FieldWithTableBuilder {
        private final @Nonnull TableBuilder tableBuilder;
        private final @Nonnull String name;
        private final @Nonnull ISqlJetSimpleFieldType type;
        private boolean indexed = false;
        private final int colNumber;
        private final @Nonnull Set<SqlJetSimpleColumnContraint> constraints = EnumSet
                .noneOf(SqlJetSimpleColumnContraint.class);
        private Object defaultVal;

        public FieldWithTableBuilder(@Nonnull TableBuilder tableBuilder, @Nonnull String name,
                @Nonnull ISqlJetSimpleFieldType type, int colNumber) {
            this.tableBuilder = tableBuilder;
            this.name = name;
            this.type = type;
            this.colNumber = colNumber;
        }

        public @Nonnull FieldWithTableBuilder notNull() {
            constraints.add(SqlJetSimpleColumnContraint.NOT_NULL);
            return this;
        }

        public @Nonnull FieldWithTableBuilder primaryKeyAutoincrement() {
            constraints.add(SqlJetSimpleColumnContraint.AUTOINCREMENTED_PRIMARY_KEY);
            return this;
        }

        public @Nonnull FieldWithTableBuilder primaryKey() {
            constraints.add(SqlJetSimpleColumnContraint.PRIMARY_KEY);
            return this;
        }

        public @Nonnull FieldWithTableBuilder indexed() {
            this.indexed = true;
            return this;
        }

        public @Nonnull FieldWithTableBuilder withDefault(Object defaultVal) {
            this.defaultVal = defaultVal;
            return this;
        }

        public @Nonnull TableBuilder build() {
            return tableBuilder.withField(
                    new SqlJetSimpleSchemaField(null, name, type, indexed, colNumber, constraints, defaultVal));
        }
    }

}
