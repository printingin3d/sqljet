/**
 * SqlJetTableDef.java
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
 */
package org.tmatesoft.sqljet.core.internal.schema;

import static org.tmatesoft.sqljet.core.internal.SqlJetAssert.assertNotEmpty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.Nonnull;

import org.antlr.runtime.tree.CommonTree;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.lang.SqlParser;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnConstraint;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnNotNull;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnPrimaryKey;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnUnique;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableConstraint;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetTablePrimaryKey;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableUnique;

/**
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 */
public class SqlJetTableDef implements ISqlJetTableDef {

    private final @Nonnull String name;
    private final @Nonnull String quotedName;
    private final String databaseName;
    private final boolean temporary;
    private final boolean ifNotExists;
    private final @Nonnull List<ISqlJetColumnDef> columns;
    private final @Nonnull List<ISqlJetTableConstraint> constraints;

    private int page;
    private long rowId;

    private boolean rowIdPrimaryKey;
    private boolean autoincremented;
    private String primaryKeyIndexName;
    private String rowIdPrimaryKeyColumnName;
    private int rowIdPrimaryKeyColumnIndex = -1;
    private final @Nonnull List<String> primaryKeyColumns = new ArrayList<>();

    // index name -> column index constraint
    private final @Nonnull Map<String, SqlJetColumnIndexConstraint> columnConstraintsIndexCache = 
    		new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    // index name -> table index constraint
    private final @Nonnull Map<String, SqlJetTableIndexConstraint> tableConstrainsIndexCache = 
    		new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private final @Nonnull List<ISqlJetColumnDef> notNullColumnsCache = new ArrayList<>();

    @SuppressWarnings("null")
	SqlJetTableDef(@Nonnull String name, String databaseName, boolean temporary, boolean ifNotExists,
			@Nonnull List<? extends ISqlJetColumnDef> columns, @Nonnull List<ISqlJetTableConstraint> constraints, 
			int page, long rowid) throws SqlJetException {
        this.name = SqlParser.unquoteId(name);
        this.quotedName = name;
        this.databaseName = databaseName;
        this.temporary = temporary;
        this.ifNotExists = ifNotExists;
        this.constraints = Collections.unmodifiableList(constraints);
        this.page = page;
        this.rowId = rowid;;
        this.columns = Collections.unmodifiableList(reindexColumns(Collections.unmodifiableList(columns)));
        resolveConstraints();
    }

    public SqlJetTableDef(CommonTree ast, int page) throws SqlJetException {
        CommonTree optionsNode = (CommonTree) ast.getChild(0);
        temporary = hasOption(optionsNode, "temporary");
        ifNotExists = hasOption(optionsNode, "exists");

        CommonTree nameNode = (CommonTree) ast.getChild(1);
        name = assertNotEmpty(nameNode.getText(), SqlJetErrorCode.BAD_PARAMETER);
        quotedName = assertNotEmpty(SqlParser.quotedId(nameNode), SqlJetErrorCode.BAD_PARAMETER);
        databaseName = nameNode.getChildCount() > 0 ? nameNode.getChild(0).getText() : null;

        List<ISqlJetColumnDef> columns = new ArrayList<>();
        List<ISqlJetTableConstraint> constraints = new ArrayList<>();
        if (ast.getChildCount() > 2) {
            CommonTree defNode = (CommonTree) ast.getChild(2);
            if ("columns".equalsIgnoreCase(defNode.getText())) {
                for (int i = 0; i < defNode.getChildCount(); i++) {
                    columns.add(new SqlJetColumnDef((CommonTree) defNode.getChild(i)));
                }
                if (ast.getChildCount() > 3) {
                    CommonTree constraintsNode = (CommonTree) ast.getChild(3);
                    assert "constraints".equalsIgnoreCase(constraintsNode.getText());
                    for (int i = 0; i < constraintsNode.getChildCount(); i++) {
                        CommonTree constraintRootNode = (CommonTree) constraintsNode.getChild(i);
                        assert "table_constraint".equalsIgnoreCase(constraintRootNode.getText());
                        CommonTree constraintNode = (CommonTree) constraintRootNode.getChild(0);
                        String constraintType = constraintNode.getText();
                        String constraintName = constraintRootNode.getChildCount() > 1 ? constraintRootNode.getChild(1)
                                .getText() : null;
                        if ("primary".equalsIgnoreCase(constraintType)) {
                            constraints.add(new SqlJetTablePrimaryKey(constraintName, constraintNode));
                        } else if ("unique".equalsIgnoreCase(constraintType)) {
                            constraints.add(new SqlJetTableUnique(constraintName, constraintNode));
                        } else if ("check".equalsIgnoreCase(constraintType)) {
                            constraints.add(new SqlJetTableCheck(constraintName, constraintNode));
                        } else if ("foreign".equalsIgnoreCase(constraintType)) {
                            constraints.add(new SqlJetTableForeignKey(constraintName, constraintNode));
                        } else {
                            assert false;
                        }
                    }
                }
            } else {
                // TODO: handle select
            }
        }
        this.constraints = Collections.unmodifiableList(constraints);
        this.page = page;

        this.columns = Collections.unmodifiableList(reindexColumns(columns));
        resolveConstraints();
    }

    private @Nonnull List<ISqlJetColumnDef> reindexColumns(@Nonnull List<ISqlJetColumnDef> cols) throws SqlJetException {
    	List<ISqlJetColumnDef> reindexedColumns = new ArrayList<>();
    	
        int columnIndex = 0;
        for (ISqlJetColumnDef column : cols) {
            reindexedColumns.add(column.updateIndex(columnIndex));
            columnIndex++;
        }
        return reindexedColumns;
    }
    
    private void resolveConstraints() throws SqlJetException {
    	int columnIndex = 0, autoindexNumber = 0;
    	for (ISqlJetColumnDef column : columns) {
    		boolean notNull = false;
    		for (ISqlJetColumnConstraint constraint : column.getConstraints()) {
    			if (constraint instanceof ISqlJetColumnPrimaryKey) {
    				SqlJetColumnPrimaryKey pk = (SqlJetColumnPrimaryKey) constraint;
    				primaryKeyColumns.add(column.getName());
    				if (column.hasExactlyIntegerType()) {
    					rowIdPrimaryKeyColumnName = column.getName();
    					rowIdPrimaryKeyColumnIndex = columnIndex;
    					rowIdPrimaryKey = true;
    					autoincremented = pk.isAutoincremented();
    				} else {
    					pk.setIndexName(primaryKeyIndexName = generateAutoIndexName(getName(), ++autoindexNumber));
    					columnConstraintsIndexCache.put(pk.getIndexName(), pk);
    				}
    			} else if (constraint instanceof ISqlJetColumnUnique) {
    				SqlJetColumnUnique uc = (SqlJetColumnUnique) constraint;
    				uc.setIndexName(generateAutoIndexName(getName(), ++autoindexNumber));
    				columnConstraintsIndexCache.put(uc.getIndexName(), uc);
    			} else if (constraint instanceof ISqlJetColumnNotNull) {
    				notNull = true;
    			} else if (constraint instanceof SqlJetColumnDefault) {
    				if (notNull) {
    					final SqlJetColumnDefault value = (SqlJetColumnDefault) constraint;
    					notNull = null == value.getExpression().getValue();
    				}
    			}
    		}
    		if (notNull) {
    			notNullColumnsCache.add(column);
    		}
    		columnIndex++;
    	}
    	for (ISqlJetTableConstraint constraint : constraints) {
    		if (constraint instanceof ISqlJetTablePrimaryKey) {
    			boolean b = false;
    			SqlJetTablePrimaryKey pk = (SqlJetTablePrimaryKey) constraint;
    			assert primaryKeyColumns.isEmpty();
    			primaryKeyColumns.addAll(pk.getColumns());
    			if (pk.getColumns().size() == 1) {
    				final String n = pk.getColumns().get(0);
    				final ISqlJetColumnDef c = getColumn(n);
    				if (null == c) {
    					throw new SqlJetException(SqlJetErrorCode.ERROR, "Wrong column '" + n + "' in PRIMARY KEY");
    				} else if (c.hasExactlyIntegerType()) {
    					rowIdPrimaryKeyColumnName = n;
    					rowIdPrimaryKeyColumnIndex = getColumnNumber(n);
    					rowIdPrimaryKey = true;
    					b = true;
    				}
    			}
    			if (!b) {
    				pk.setIndexName(primaryKeyIndexName = generateAutoIndexName(getName(), ++autoindexNumber));
    				tableConstrainsIndexCache.put(pk.getIndexName(), pk);
    			}
    		} else if (constraint instanceof ISqlJetTableUnique) {
    			SqlJetTableUnique uc = (SqlJetTableUnique) constraint;
    			uc.setIndexName(generateAutoIndexName(getName(), ++autoindexNumber));
    			tableConstrainsIndexCache.put(uc.getIndexName(), uc);
    		}
    	}
    }

    private static String generateAutoIndexName(String tableName, int i) {
        return SqlJetSchema.generateAutoIndexName(tableName, i);
    }

    static boolean hasOption(CommonTree optionsNode, String name) {
        for (int i = 0; i < optionsNode.getChildCount(); i++) {
            CommonTree optionNode = (CommonTree) optionsNode.getChild(i);
            if (name.equalsIgnoreCase(optionNode.getText())) {
                return true;
            }
        }
        return false;
    }

    @Override
	public @Nonnull String getName() {
        return name;
    }

    @Override
	public @Nonnull String getQuotedName() {
    	return quotedName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    @Override
	public boolean isTemporary() {
        return temporary;
    }

    public boolean isKeepExisting() {
        return ifNotExists;
    }

    @Override
	public @Nonnull List<ISqlJetColumnDef> getColumns() {
        return columns;
    }

    @Override
	public ISqlJetColumnDef getColumn(String name) {
        for (ISqlJetColumnDef column : getColumns()) {
            if (column.getName().equalsIgnoreCase(name)) {
                return column;
            }
        }
        return null;
    }

    @Override
	public int getColumnNumber(String name) {
        for (ISqlJetColumnDef column : getColumns()) {
            if (column.getName().equalsIgnoreCase(name)) {
                return column.getIndex();
            }
        }
        return -1;
    }

    @Override
	public @Nonnull List<ISqlJetTableConstraint> getConstraints() {
        return constraints;
    }

    @Override
	public boolean isRowIdPrimaryKey() {
        return rowIdPrimaryKey;
    }

    @Override
	public boolean isAutoincremented() {
        return autoincremented;
    }

    // Internal API

    @Override
	public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    @Override
	public long getRowId() {
        return rowId;
    }

    public void setRowId(long rowId) {
        this.rowId = rowId;
    }

    /**
     * Returns name of the primary key index.
     */
    @Override
	public String getPrimaryKeyIndexName() {
        return primaryKeyIndexName;
    }

    public String getRowIdPrimaryKeyColumnName() {
        return rowIdPrimaryKeyColumnName;
    }

    public int getRowIdPrimaryKeyColumnIndex() {
        return rowIdPrimaryKeyColumnIndex;
    }

    public SqlJetColumnIndexConstraint getColumnIndexConstraint(String indexName) {
        return columnConstraintsIndexCache.get(indexName);
    }

    public SqlJetTableIndexConstraint getTableIndexConstraint(String indexName) {
        return tableConstrainsIndexCache.get(indexName);
    }

    /**
     * @return the notNullColumnsCache
     */
    public @Nonnull List<ISqlJetColumnDef> getNotNullColumns() {
        return notNullColumnsCache;
    }

    // Serialization

    @Override
    public String toString() {
    	StringBuilder buffer = new StringBuilder();
        buffer.append(getPage());
        buffer.append("/");
        buffer.append(getRowId());
        buffer.append(": ");
        buffer.append(toSQL(false));
        return buffer.toString();
    }

    @Override
	public String toSQL() {
        return toSQL(true);
    }

    public String toSQL(boolean schemaStrict) {
    	StringBuilder buffer = new StringBuilder();
        buffer.append("CREATE ");
        if (isTemporary()) {
            buffer.append("TEMPORARY ");
        }
        buffer.append("TABLE ");
        if (!schemaStrict) {
            if (isKeepExisting()) {
                buffer.append("IF NOT EXISTS ");
            }
            if (getDatabaseName() != null) {
                buffer.append(getDatabaseName());
                buffer.append('.');
            }
        }
        buffer.append(getQuotedName());
        buffer.append(" (");
        List<ISqlJetColumnDef> columns = getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                buffer.append(", ");
            }
            buffer.append(columns.get(i).toString());
        }
        List<ISqlJetTableConstraint> constraints = getConstraints();
        for (int i = 0; i < constraints.size(); i++) {
            buffer.append(", ");
            buffer.append(constraints.get(i).toString());
        }
        buffer.append(')');
        return buffer.toString();
    }
    
    @Override
	public SqlJetTableDef renamedTable(@Nonnull String newTableName) throws SqlJetException {
    	return new SqlJetTableDef(newTableName, databaseName, temporary, false, columns, constraints, page, rowId);
    }
}
