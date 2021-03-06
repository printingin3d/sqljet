/**
 * SqlJetDataTableCursor.java
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
package org.tmatesoft.sqljet.core.internal.table;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtree;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.schema.ISqlJetIndexDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableDef;
import org.tmatesoft.sqljet.core.schema.SqlJetConflictAction;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;
import org.tmatesoft.sqljet.core.table.ISqlJetTransaction;
import org.tmatesoft.sqljet.core.table.SqlJetDb;
import org.tmatesoft.sqljet.core.table.SqlJetScope;

/**
 * Implementation of {@link ISqlJetTable}.
 * 
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetTable implements ISqlJetTable {

    private final SqlJetDb db;
    private final ISqlJetBtree btree;
    private final String tableName;
    private final boolean write;

    public SqlJetTable(SqlJetDb db, ISqlJetBtree btree, String tableName, boolean write) throws SqlJetException {
        this.db = db;
        this.btree = btree;
        this.tableName = tableName;
        this.write = write;
        if (null == getDefinition()) {
            throw new SqlJetException(SqlJetErrorCode.ERROR, "Table not found: " + tableName);
        }
    }

    @Override
    public SqlJetDb getDataBase() {
        return db;
    }

    @Override
    public String getPrimaryKeyIndexName() throws SqlJetException {
        final ISqlJetTableDef definition = getDefinition();
        return definition.isRowIdPrimaryKey() ? null : definition.getPrimaryKeyIndexName();
    }

    @Override
    public ISqlJetTableDef getDefinition() throws SqlJetException {
        return btree.getSchema().getTable(tableName);
    };

    @Override
    public Set<ISqlJetIndexDef> getIndexesDefs() throws SqlJetException {
        return btree.getSchema().getIndexes(tableName);
    }

    @Override
    public @Nonnull Set<String> getIndexesNames() throws SqlJetException {
        return Collections.unmodifiableSet(getIndexesDefs().stream().map(ISqlJetIndexDef::getName)
                .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))));
    }

    @Override
    public ISqlJetIndexDef getIndexDef(String name) throws SqlJetException {
        String realName;
        if (null == name) {
            realName = getPrimaryKeyIndexName();
            if (null == realName) {
                return null;
            }
        } else {
            realName = name;
        }
        return getIndexesDefs().stream().filter(indexDef -> realName.equalsIgnoreCase(indexDef.getName())).findFirst()
                .orElse(null);
    }

    @Override
    public ISqlJetCursor open() throws SqlJetException {
        return db.runWithLock(db -> new SqlJetTableDataCursor(new SqlJetBtreeDataTable(btree, tableName, write), db));
    }

    @Override
    public ISqlJetCursor lookup(final String indexName, final Object... key) throws SqlJetException {
        final Object[] k = SqlJetUtility.adjustNumberTypes(key);
        return db.runWithLock(db -> {
            final SqlJetBtreeDataTable table = new SqlJetBtreeDataTable(btree, tableName, write);
            checkIndexName(indexName, table);
            return new SqlJetIndexScopeCursor(table, db, indexName, k, k);
        });
    }

    private <T> T runWriteTransaction(final ISqlJetTransaction<T, ISqlJetBtreeDataTable> op) throws SqlJetException {
        return db.write().as(db -> {
            final ISqlJetBtreeDataTable table = new SqlJetBtreeDataTable(btree, tableName, write);
            try {
                return op.run(table);
            } finally {
                table.close();
            }
        });
    }

    @Override
    public long insert(@Nonnull Object... values) throws SqlJetException {
        return insertOr(null, values);
    }

    @Override
    public long insertByFieldNames(final Map<String, Object> values) throws SqlJetException {
        return insertByFieldNamesOr(null, values);
    }

    @Override
    public long insertWithRowId(final long rowId, @Nonnull Object... values) throws SqlJetException {
        return insertWithRowIdOr(null, rowId, values);
    }

    @Override
    public long insertOr(final SqlJetConflictAction onConflict, @Nonnull Object... values) throws SqlJetException {
        return runWriteTransaction(table -> Long.valueOf(table.insert(onConflict, values))).longValue();
    }

    @Override
    public long insertByFieldNamesOr(final SqlJetConflictAction onConflict, final Map<String, Object> values)
            throws SqlJetException {
        return runWriteTransaction(table -> Long.valueOf(table.insert(onConflict, values))).longValue();
    }

    @Override
    public long insertWithRowIdOr(final SqlJetConflictAction onConflict, final long rowId, @Nonnull Object... values)
            throws SqlJetException {
        return runWriteTransaction(table -> Long.valueOf(table.insertWithRowId(onConflict, rowId, values))).longValue();
    }

    @Override
    public ISqlJetCursor order(final String indexName) throws SqlJetException {
        return db.runWithLock(db -> {
            final SqlJetBtreeDataTable table = new SqlJetBtreeDataTable(btree, tableName, write);
            checkIndexName(indexName, table);
            return new SqlJetIndexOrderCursor(table, db, indexName);
        });
    }

    @Override
    public ISqlJetCursor scope(final String indexName, final Object[] firstKey, final Object[] lastKey)
            throws SqlJetException {
        return scope(indexName, new SqlJetScope(firstKey, lastKey));
    }

    @Override
    public ISqlJetCursor scope(final String indexName, SqlJetScope scope) throws SqlJetException {
        final SqlJetScope adjustedScope = SqlJetUtility.adjustScopeNumberTypes(scope);
        return db.runWithLock(db -> {
            final SqlJetBtreeDataTable table = new SqlJetBtreeDataTable(btree, tableName, write);
            checkIndexName(indexName, table);
            if (isNeedReverse(getIndexTable(indexName, table), adjustedScope)) {
                return new SqlJetReverseOrderCursor(
                        new SqlJetIndexScopeCursor(table, db, indexName, adjustedScope.reverse()));
            } else {
                return new SqlJetIndexScopeCursor(table, db, indexName, adjustedScope);
            }
        });
    }

    @Override
    public void clear() throws SqlJetException {
        runWriteTransaction(table -> {
            table.clear();
            return null;
        });
    }

    /**
     * @param indexName
     * @param scope
     * @param reverse
     * @param table
     * @return
     * @throws SqlJetException
     */
    private boolean isNeedReverse(final ISqlJetBtreeIndexTable indexTable, SqlJetScope scope) throws SqlJetException {
        Object[] firstKey = scope.getLeftBound() != null ? scope.getLeftBound().getValue() : null;
        Object[] lastKey = scope.getRightBound() != null ? scope.getRightBound().getValue() : null;

        if (firstKey != null && lastKey != null && firstKey.length > 0 && lastKey.length > 0) {
            if (indexTable != null) {
                return indexTable.compareKeys(firstKey, lastKey) < 0;
            } else if (firstKey.length == 1 && lastKey.length == 1 && firstKey[0] instanceof Long
                    && lastKey[0] instanceof Long) {
                return ((Long) firstKey[0]).compareTo((Long) lastKey[0]) > 0;
            }
        }
        return false;
    }

    private ISqlJetBtreeIndexTable getIndexTable(final String indexName, final SqlJetBtreeDataTable table) {
        final String index = indexName == null ? table.getPrimaryKeyIndex() : indexName;
        return index != null ? table.getIndex(index) : null;
    }

    private void checkIndexName(final String indexName, final SqlJetBtreeDataTable table) throws SqlJetException {
        if (!isIndexNameValid(indexName, table)) {
            throw new SqlJetException(SqlJetErrorCode.MISUSE, String.format("Index not exists: %s", indexName));
        }
    }

    private boolean isIndexNameValid(final String indexName, final SqlJetBtreeDataTable table) {
        if (indexName != null) {
            return getIndexTable(indexName, table) != null;
        } else {
            if (table.getDefinition().isRowIdPrimaryKey()) {
                return true;
            } else {
                return table.getPrimaryKeyIndex() != null;
            }
        }
    }

}
