/**
 * SqlJetSchema.java
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
package org.tmatesoft.sqljet.core.internal.schema;

import static org.tmatesoft.sqljet.core.internal.SqlJetAssert.assertFalse;
import static org.tmatesoft.sqljet.core.internal.SqlJetAssert.assertNotEmpty;
import static org.tmatesoft.sqljet.core.internal.SqlJetAssert.assertNotNull;
import static org.tmatesoft.sqljet.core.internal.SqlJetAssert.assertTrue;
import static org.tmatesoft.sqljet.core.internal.SqlJetUtility.coalesce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.ParserRuleReturnScope;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RuleReturnScope;
import org.antlr.runtime.tree.CommonTree;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtree;
import org.tmatesoft.sqljet.core.internal.ISqlJetDbHandle;
import org.tmatesoft.sqljet.core.internal.SqlJetBtreeTableCreateFlags;
import org.tmatesoft.sqljet.core.internal.lang.SqlLexer;
import org.tmatesoft.sqljet.core.internal.lang.SqlParser;
import org.tmatesoft.sqljet.core.internal.table.ISqlJetBtreeDataTable;
import org.tmatesoft.sqljet.core.internal.table.ISqlJetBtreeSchemaTable;
import org.tmatesoft.sqljet.core.internal.table.SqlJetBtreeDataTable;
import org.tmatesoft.sqljet.core.internal.table.SqlJetBtreeIndexTable;
import org.tmatesoft.sqljet.core.internal.table.SqlJetBtreeSchemaTable;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnConstraint;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnDefault;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnNotNull;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnPrimaryKey;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnUnique;
import org.tmatesoft.sqljet.core.schema.ISqlJetIndexDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetIndexedColumn;
import org.tmatesoft.sqljet.core.schema.ISqlJetSchema;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableConstraint;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetTablePrimaryKey;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableUnique;
import org.tmatesoft.sqljet.core.schema.ISqlJetTriggerDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetViewDef;
import org.tmatesoft.sqljet.core.schema.ISqlJetVirtualTableDef;
import org.tmatesoft.sqljet.core.simpleschema.SqlJetSimpleSchemaTable;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 */
public class SqlJetSchema implements ISqlJetSchema {

    private static final String NAME_RESERVED = "Name '%s' is reserved to internal use";

    private static String AUTOINDEX_PREFIX = "sqlite_autoindex_";

    private static final String CANT_DELETE_IMPLICIT_INDEX = "Can't delete implicit index \"%s\"";

    private static final String CREATE_TABLE_SQLITE_SEQUENCE = "CREATE TABLE sqlite_sequence(name,seq)";

    private static final String SQLITE_SEQUENCE = "SQLITE_SEQUENCE";

    public static final Set<SqlJetBtreeTableCreateFlags> BTREE_CREATE_TABLE_FLAGS = 
    		EnumSet.of(SqlJetBtreeTableCreateFlags.INTKEY, SqlJetBtreeTableCreateFlags.LEAFDATA);

    public static final Set<SqlJetBtreeTableCreateFlags> BTREE_CREATE_INDEX_FLAGS = 
    		EnumSet.of(SqlJetBtreeTableCreateFlags.ZERODATA);

    private static final String TABLE_TYPE = "table";
    private static final String INDEX_TYPE = "index";
    private static final String VIEW_TYPE = "view";
    private static final String TRIGGER_TYPE = "trigger";

    private final @Nonnull ISqlJetDbHandle db;
    private final ISqlJetBtree btree;

    private final @Nonnull Map<String, ISqlJetTableDef> tableDefs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final @Nonnull Map<String, ISqlJetIndexDef> indexDefs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final @Nonnull Map<String, ISqlJetVirtualTableDef> virtualTableDefs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final @Nonnull Map<String, ISqlJetViewDef> viewDefs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final @Nonnull Map<String, ISqlJetTriggerDef> triggerDefs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private enum SqlJetSchemaObjectType {
        TABLE("table"),
        INDEX("index"),
        VIRTUAL_TABLE("virtual table"),
        VIEW("view"),
        TRIGGER("trigger");
    	
    	private final String name;

        private SqlJetSchemaObjectType(String name) {
			this.name = name;
		}

		public String getName() {
        	return name;
        }
    }

    public SqlJetSchema(@Nonnull ISqlJetDbHandle db, ISqlJetBtree btree) throws SqlJetException {
        this.db = db;
        this.btree = btree;
        init();
    }

    @Nonnull ISqlJetBtreeSchemaTable openSchemaTable(boolean write) throws SqlJetException {
        return new SqlJetBtreeSchemaTable(btree, write);
    }

    private void init() throws SqlJetException {
        if (db.getOptions().getSchemaVersion() == 0) {
			return;
		}
        final ISqlJetBtreeSchemaTable table = openSchemaTable(false);
        try {
            readShema(table);
        } finally {
            table.close();
        }
    }

    @Override
	public @Nonnull Set<String> getTableNames() throws SqlJetException {
        return Collections.unmodifiableSet(db.getMutex().run(x -> tableDefs.keySet()));
    }

    @Override
	public ISqlJetTableDef getTable(String name) throws SqlJetException {
        return db.getMutex().run(x -> tableDefs.get(name));
    }

    @Override
	public Set<String> getIndexNames() throws SqlJetException {
        return db.getMutex().run(x -> {
            final Set<String> s = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            s.addAll(indexDefs.keySet());
            return s;
        });
    }

    @Override
	public ISqlJetIndexDef getIndex(String name) throws SqlJetException {
        return db.getMutex().run(x -> indexDefs.get(name));
    }

    @Override
	public @Nonnull Set<ISqlJetIndexDef> getIndexes(String tableName) throws SqlJetException {
        return Collections.unmodifiableSet(
        		db.getMutex().run(x -> indexDefs.values().stream()
        			.filter(i -> i.getTableName().equals(tableName))
        			.collect(Collectors.toSet())));
    }

	@Override
	public @Nonnull Set<String> getVirtualTableNames() throws SqlJetException {
        return Collections.unmodifiableSet(db.getMutex().run(x -> virtualTableDefs.keySet()));
    }

    @Override
	public ISqlJetVirtualTableDef getVirtualTable(String name) throws SqlJetException {
        return db.getMutex().run(x -> virtualTableDefs.get(name));
    }

    @Override
	public ISqlJetViewDef getView(String name) throws SqlJetException {
        return db.getMutex().run(x -> viewDefs.get(name));
    }

    @Override
	public @Nonnull Set<String> getViewNames() throws SqlJetException {
        return Collections.unmodifiableSet(db.getMutex().run(x -> viewDefs.keySet()));
    }

    @Override
	public ISqlJetTriggerDef getTrigger(String name) throws SqlJetException {
        return db.getMutex().run(x -> triggerDefs.get(name));
    }

    @Override
	public @Nonnull Set<String> getTriggerNames() throws SqlJetException {
        return Collections.unmodifiableSet(db.getMutex().run(x -> triggerDefs.keySet()));
    }

    private void readShema(@Nonnull ISqlJetBtreeSchemaTable table) throws SqlJetException {
        for (table.first(); !table.eof(); table.next()) {
            final String type = table.getTypeField();
            if (null == type) {
                continue;
            }
            final String name = table.getNameField();
            if (null == name) {
                continue;
            }
            final int page = table.getPageField();

            if (TABLE_TYPE.equals(type)) {
                String sql = table.getSqlField();
                // System.err.println(sql);
                final CommonTree ast = (CommonTree) parseTable(sql).getTree();
                if (!isCreateVirtualTable(ast)) {
                    final SqlJetTableDef tableDef = new SqlJetTableDef(ast, page);
                    if (!name.equals(tableDef.getName())) {
                        throw new SqlJetException(SqlJetErrorCode.CORRUPT);
                    }
                    tableDef.setRowId(table.getRowId());
                    tableDefs.put(name, tableDef);
                } else {
                    final SqlJetVirtualTableDef virtualTableDef = new SqlJetVirtualTableDef(ast, page);
                    if (!name.equals(virtualTableDef.getTableName())) {
                        throw new SqlJetException(SqlJetErrorCode.CORRUPT);
                    }
                    virtualTableDef.setRowId(table.getRowId());
                    virtualTableDefs.put(name, virtualTableDef);
                }
            } else if (INDEX_TYPE.equals(type)) {
                final String tableName = assertNotEmpty(table.getTableField(), SqlJetErrorCode.BAD_PARAMETER);
                final String sql = table.getSqlField();
                if (null != sql) {
                    // System.err.println(sql);
                    final CommonTree ast = (CommonTree) parseIndex(sql).getTree();
                    final SqlJetIndexDef indexDef = SqlJetIndexDef.parseNode(ast, page);
                    assertTrue(name.equals(indexDef.getName()), SqlJetErrorCode.CORRUPT);
                    assertTrue(tableName.equals(indexDef.getTableName()), SqlJetErrorCode.CORRUPT);
                    indexDef.setRowId(table.getRowId());
                    indexDefs.put(name, indexDef);
                } else {
                    SqlJetBaseIndexDef indexDef = new SqlJetBaseIndexDef(name, tableName, page);
                    indexDef.setRowId(table.getRowId());
                    indexDefs.put(name, indexDef);
                }
            } else if (VIEW_TYPE.equals(type)) {
                final String viewName = table.getTableField();
                final String sql = table.getSqlField();
                final CommonTree ast = (CommonTree) parseView(sql).getTree();
                final ISqlJetViewDef viewDef = new SqlJetViewDef(sql, ast).withRowId(table.getRowId());
                viewDefs.put(viewName, viewDef);
            } else if (TRIGGER_TYPE.equals(type)) {
                final String triggerName = table.getNameField();
                final String sql = table.getSqlField();

                final CommonTree ast = (CommonTree) parseTrigger(sql).getTree();
                final SqlJetTriggerDef triggerDef = new SqlJetTriggerDef(sql, ast);
                triggerDef.setRowId(table.getRowId());
                triggerDefs.put(triggerName, triggerDef);
            }
        }

        bindIndexes();
    }

    /**
     *
     */
    private void bindIndexes() {
        for (ISqlJetIndexDef indexDef : indexDefs.values()) {
            if (indexDef instanceof SqlJetIndexDef) {
                SqlJetIndexDef i = (SqlJetIndexDef) indexDef;
                final String tableName = i.getTableName();
                final ISqlJetTableDef tableDef = tableDefs.get(tableName);
                if (tableDef != null) {
                    i.bindColumns(tableDef);
                }
            }
        }
    }

    /**
     * @param ast
     * @return
     */
    private boolean isCreateVirtualTable(CommonTree ast) {
        final CommonTree optionsNode = (CommonTree) ast.getChild(0);
        for (int i = 0; i < optionsNode.getChildCount(); i++) {
            CommonTree optionNode = (CommonTree) optionsNode.getChild(i);
            if ("virtual".equalsIgnoreCase(optionNode.getText())) {
                return true;
            }
        }
        return false;
    }

    private RuleReturnScope parseTable(String sql) throws SqlJetException {
        try {
            CharStream chars = new ANTLRStringStream(sql);
            SqlLexer lexer = new SqlLexer(chars);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SqlParser parser = new SqlParser(tokens);
            return parser.schema_create_table_stmt();
        } catch (RecognitionException re) {
            throw new SqlJetException(SqlJetErrorCode.ERROR, "Invalid sql statement: " + sql);
        }
    }

    private RuleReturnScope parseView(String sql) throws SqlJetException {
        try {
            CharStream chars = new ANTLRStringStream(sql);
            SqlLexer lexer = new SqlLexer(chars);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SqlParser parser = new SqlParser(tokens);
            return parser.create_view_stmt();
        } catch (RecognitionException re) {
            throw new SqlJetException(SqlJetErrorCode.ERROR, "Invalid sql statement: " + sql);
        }
    }

    private RuleReturnScope parseTrigger(String sql) throws SqlJetException {
        try {
            CharStream chars = new ANTLRStringStream(sql);
            SqlLexer lexer = new SqlLexer(chars);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SqlParser parser = new SqlParser(tokens);
            return parser.create_trigger_stmt();
        } catch (RecognitionException re) {
            throw new SqlJetException(SqlJetErrorCode.ERROR, "Invalid sql statement: " + sql);
        }
    }

    private ParserRuleReturnScope parseIndex(String sql) throws SqlJetException {
        try {
            CharStream chars = new ANTLRStringStream(sql);
            SqlLexer lexer = new SqlLexer(chars);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SqlParser parser = new SqlParser(tokens);
            return parser.create_index_stmt();
        } catch (RecognitionException re) {
            throw new SqlJetException(SqlJetErrorCode.ERROR, "Invalid sql statement: " + sql);
        }
    }

    @Override
    public String toString() {
        db.getMutex().enter();
        try {
        	StringBuilder buffer = new StringBuilder();
            buffer.append("Tables:\n");
            for (ISqlJetTableDef tableDef : tableDefs.values()) {
                buffer.append(tableDef.toString());
                buffer.append('\n');
            }
            buffer.append("Indexes:\n");
            for (ISqlJetIndexDef indexDef : indexDefs.values()) {
                buffer.append(indexDef.toString());
                buffer.append('\n');
            }
            return buffer.toString();
        } finally {
            db.getMutex().leave();
        }
    }

    public ISqlJetTableDef createTable(String sql) throws SqlJetException {
        return db.getMutex().run(x -> createTableSafe(sql, false));
    }
    
    public ISqlJetTableDef createTable(SqlJetSimpleSchemaTable tableDef) throws SqlJetException {
    	return db.getMutex().run(x -> createTableSafe(tableDef, false));
    }

    private ISqlJetTableDef createTableSafe(String sql, boolean internal) throws SqlJetException {
        final RuleReturnScope parseTable = parseTable(sql);
        final CommonTree ast = (CommonTree) parseTable.getTree();

        assertFalse(isCreateVirtualTable(ast), SqlJetErrorCode.ERROR);

        final SqlJetTableDef tableDef = new SqlJetTableDef(ast, 0);
        final String tableName = tableDef.getName();
        assertNotEmpty(tableName, SqlJetErrorCode.ERROR);

        if (!internal) {
            checkNameReserved(tableName);
        }

        if (tableDefs.containsKey(tableName)) {
            if (tableDef.isKeepExisting()) {
                return tableDefs.get(tableName);
            } else {
                throw new SqlJetException(SqlJetErrorCode.ERROR, "Table \"" + tableName + "\" exists already");
            }
        }

        checkNameConflict(SqlJetSchemaObjectType.TABLE, tableName);
        checkFieldNamesRepeatsConflict(tableDef.getName(), tableDef.getColumns());

        final List<ISqlJetColumnDef> columns = tableDef.getColumns();
        assertNotEmpty(columns, SqlJetErrorCode.ERROR);

        final String createTableSql = getCreateTableSql(parseTable);

        final ISqlJetBtreeSchemaTable schemaTable = openSchemaTable(true);

        try {
            db.getOptions().changeSchemaVersion();

            final int page = btree.createTable(BTREE_CREATE_TABLE_FLAGS);
            final long rowId = schemaTable.newRowId();
            schemaTable.insert(null, rowId, null, 0, 0, false);
            addConstraints(schemaTable, tableDef);

            schemaTable.updateRecord(rowId, TABLE_TYPE, tableName, tableName, page, createTableSql);


            tableDef.setPage(page);
            tableDef.setRowId(rowId);
            tableDefs.put(tableName, tableDef);
            return tableDef;
        } finally {
            schemaTable.close();
        }
    }
    
    private ISqlJetTableDef createTableSafe(SqlJetSimpleSchemaTable tableDef, boolean internal) throws SqlJetException {
    	final String tableName = tableDef.getName();
    	assertNotEmpty(tableName, SqlJetErrorCode.ERROR);
    	
    	if (!internal) {
    		checkNameReserved(tableName);
    	}

        if (tableDefs.containsKey(tableName)) {
            throw new SqlJetException(SqlJetErrorCode.ERROR, "Table \"" + tableName + "\" exists already");
        }
    	
    	checkNameConflict(SqlJetSchemaObjectType.TABLE, tableName);
    	checkFieldNamesRepeatsConflict(tableName, tableDef.getFields());
    	
    	final ISqlJetBtreeSchemaTable schemaTable = openSchemaTable(true);
    	
    	try {
    		db.getOptions().changeSchemaVersion();
    		
    		final int page = btree.createTable(BTREE_CREATE_TABLE_FLAGS);
    		final long rowId = schemaTable.newRowId();
    		schemaTable.insert(null, rowId, null, 0, 0, false);
    		
    		final SqlJetTableDef innerTableDef = new SqlJetTableDef(tableDef.getName(), 
        			null, false, false, tableDef.getFields(), Collections.emptyList(), 0, 0);
    		addConstraints(schemaTable, innerTableDef);
    		
    		schemaTable.updateRecord(rowId, TABLE_TYPE, tableName, tableName, page, tableDef.toSql());
    		
    		innerTableDef.setPage(page);
    		innerTableDef.setRowId(rowId);
    		tableDefs.put(tableName, innerTableDef);
    		return innerTableDef;
    	} finally {
    		schemaTable.close();
    	}
    }

    /**
     * @param tableDef
     * @throws SqlJetException
     */
    private void checkFieldNamesRepeatsConflict(String tableName, List<? extends ISqlJetColumnDef> columns)
            throws SqlJetException {
        final Set<String> names = new HashSet<>();
        for (ISqlJetColumnDef columnDef : columns) {
            final String name = columnDef.getName();
            if (names.contains(name)) {
                throw new SqlJetException(SqlJetErrorCode.ERROR, String.format(
                        "Definition for table '%s' has conflict of repeating fields named '%s'", tableName, name));
            } else {
                names.add(name);
            }
        }
    }

    /**
     * @param parseTable
     * @return
     */
    private String getCreateTableSql(RuleReturnScope parseTable) {
        return String.format("CREATE TABLE %s", getCoreSQL(parseTable));
    }

    /**
     * @param parseIndex
     * @return
     */
    private String getCreateIndexSql(RuleReturnScope parseIndex) {
        return String.format("CREATE INDEX %s", getCoreSQL(parseIndex));
    }

    /**
     * @param parseIndex
     * @return
     */
    private String getCreateIndexUniqueSql(RuleReturnScope parseIndex) {
        return String.format("CREATE UNIQUE INDEX %s", getCoreSQL(parseIndex));
    }

    /**
     * @param parseTable
     * @return
     */
    private String getCreateVirtualTableSql(RuleReturnScope parseTable) {
        return String.format("CREATE VIRTUAL TABLE %s", getCoreSQL(parseTable));
    }

    private String getCoreSQL(RuleReturnScope parsedSQL) {
        final CommonTree ast = (CommonTree) parsedSQL.getTree();
        final CommonToken nameToken = (CommonToken) ((CommonTree) ast.getChild(1)).getToken();
        final CharStream inputStream = nameToken.getInputStream();
        final CommonToken stopToken = (CommonToken) parsedSQL.getStop();
        return inputStream.substring(nameToken.getStartIndex(), stopToken.getStopIndex());
    }

    /**
     * @param i
     * @return
     */
    static String generateAutoIndexName(String tableName, int i) {
        return AUTOINDEX_PREFIX + tableName + "_" + Integer.toString(i);
    }

    /**
     * @param schemaTable
     * @param tableDef
     * @throws SqlJetException
     */
    private void addConstraints(ISqlJetBtreeSchemaTable schemaTable, final ISqlJetTableDef tableDef)
            throws SqlJetException {

        final String tableName = tableDef.getName();
        final List<ISqlJetColumnDef> columns = tableDef.getColumns();
        int i = 0;

        for (final ISqlJetColumnDef column : columns) {
            final List<ISqlJetColumnConstraint> constraints = column.getConstraints();
            for (final ISqlJetColumnConstraint constraint : constraints) {
                if (constraint instanceof ISqlJetColumnPrimaryKey) {
                    final ISqlJetColumnPrimaryKey pk = (ISqlJetColumnPrimaryKey) constraint;
                    if (!column.hasExactlyIntegerType()) {
                        if (pk.isAutoincremented()) {
                            throw new SqlJetException(SqlJetErrorCode.ERROR,
                                    "AUTOINCREMENT is allowed only for INTEGER PRIMARY KEY fields");
                        }
                        createAutoIndex(schemaTable, tableName, generateAutoIndexName(tableName, ++i));
                    } else if (pk.isAutoincremented()) {
                        checkSequenceTable();
                    }
                } else if (constraint instanceof ISqlJetColumnUnique) {
                    createAutoIndex(schemaTable, tableName, generateAutoIndexName(tableName, ++i));
                }
            }
        }

        final List<ISqlJetTableConstraint> constraints = tableDef.getConstraints();
        for (final ISqlJetTableConstraint constraint : constraints) {
            if (constraint instanceof ISqlJetTablePrimaryKey) {
                boolean b = false;
                final ISqlJetTablePrimaryKey pk = (ISqlJetTablePrimaryKey) constraint;
                if (pk.getColumns().size() == 1) {
                    final String n = pk.getColumns().get(0);
                    final ISqlJetColumnDef c = tableDef.getColumn(n);
                    b = c != null && c.hasExactlyIntegerType();
                }
                if (!b) {
                    createAutoIndex(schemaTable, tableName, generateAutoIndexName(tableName, ++i));
                }
            } else if (constraint instanceof ISqlJetTableUnique) {
                createAutoIndex(schemaTable, tableName, generateAutoIndexName(tableName, ++i));
            }
        }
    }

    /**
     * @param schemaTable
     * @throws SqlJetException
     */
    private void checkSequenceTable() throws SqlJetException {
        if (!tableDefs.containsKey(SQLITE_SEQUENCE)) {
            createTableSafe(CREATE_TABLE_SQLITE_SEQUENCE, true);
        }
    }

    /**
     * @throws SqlJetException
     */
    public ISqlJetBtreeDataTable openSequenceTable() throws SqlJetException {
        if (tableDefs.containsKey(SQLITE_SEQUENCE)) {
            return new SqlJetBtreeDataTable(btree, SQLITE_SEQUENCE, true);
        } else {
            return null;
        }
    }

    /**
     * @param schemaTable
     * @param generateAutoIndexName
     *
     * @throws SqlJetException
     */
    private ISqlJetIndexDef createAutoIndex(ISqlJetBtreeSchemaTable schemaTable, @Nonnull String tableName, String autoIndexName)
            throws SqlJetException {
        final int page = btree.createTable(BTREE_CREATE_INDEX_FLAGS);
        final SqlJetBaseIndexDef indexDef = new SqlJetBaseIndexDef(autoIndexName, tableName, page);
        indexDef.setRowId(schemaTable.insertRecord(INDEX_TYPE, autoIndexName, tableName, page, null));
        indexDefs.put(autoIndexName, indexDef);
        return indexDef;
    }

    public ISqlJetIndexDef createIndex(String sql) throws SqlJetException {
        return db.getMutex().run(x -> createIndexSafe(sql));
    }

    private ISqlJetIndexDef createIndexSafe(String sql) throws SqlJetException {

        final ParserRuleReturnScope parseIndex = parseIndex(sql);
        final CommonTree ast = (CommonTree) parseIndex.getTree();

        final SqlJetIndexDef indexDef = SqlJetIndexDef.parseNode(ast, 0);

        
        final String indexName = indexDef.getName();
        assertNotEmpty(indexName, SqlJetErrorCode.ERROR);

        checkNameReserved(indexName);

        if (indexDefs.containsKey(indexName)) {
            if (indexDef.isKeepExisting()) {
                return indexDefs.get(indexName);
            } else {
                throw new SqlJetException(SqlJetErrorCode.ERROR, "Index \"" + indexName + "\" exists already");
            }
        }

        checkNameConflict(SqlJetSchemaObjectType.INDEX, indexName);

        final String tableName = indexDef.getTableName();
        assertNotEmpty(tableName, SqlJetErrorCode.ERROR);

        final List<ISqlJetIndexedColumn> columns = indexDef.getColumns();
        assertNotNull(columns, SqlJetErrorCode.ERROR);

        final ISqlJetTableDef tableDef = getTable(tableName);
        assertNotNull(tableDef, SqlJetErrorCode.ERROR);

        for (final ISqlJetIndexedColumn column : columns) {
        	final String columnName = column.getName();
            assertNotEmpty(columnName, SqlJetErrorCode.ERROR);
            
            assertNotNull(tableDef.getColumn(columnName), SqlJetErrorCode.ERROR, 
            		"Column \"" + columnName + "\" not found in table \"" + tableName + "\"");
        }

        final ISqlJetBtreeSchemaTable schemaTable = openSchemaTable(true);
        final String createIndexSQL = indexDef.isUnique() ? getCreateIndexUniqueSql(parseIndex)
                : getCreateIndexSql(parseIndex);

        try {
            db.getOptions().changeSchemaVersion();

            final int page = btree.createTable(BTREE_CREATE_INDEX_FLAGS);

            final long rowId = schemaTable.insertRecord(INDEX_TYPE, indexName, tableName, page, createIndexSQL);

            indexDef.setPage(page);
            indexDef.setRowId(rowId);
            indexDef.bindColumns(tableDef);
            indexDefs.put(indexName, indexDef);

            final SqlJetBtreeIndexTable indexTable = new SqlJetBtreeIndexTable(btree, indexDef.getName(), true);
            try {
                indexTable.reindex();
            } finally {
                indexTable.close();
            }
            return indexDef;
        } finally {
            schemaTable.close();
        }
    }

    public void dropTable(String tableName) throws SqlJetException {
        db.getMutex().runVoid(x -> dropTableSafe(tableName));
    }

    private void dropTableSafe(String tableName) throws SqlJetException {
    	assertNotEmpty(tableName, SqlJetErrorCode.MISUSE, "Table name must be not empty");
    	assertTrue(tableDefs.containsKey(tableName), SqlJetErrorCode.MISUSE, "Table not found: " + tableName);
    	
        final ISqlJetTableDef tableDef = tableDefs.get(tableName);

        dropTableIndexes(tableDef);

        final ISqlJetBtreeSchemaTable schemaTable = openSchemaTable(true);

        try {
            db.getOptions().changeSchemaVersion();

            assertTrue(schemaTable.goToRow(tableDef.getRowId()) && TABLE_TYPE.equals(schemaTable.getTypeField()), 
            		SqlJetErrorCode.CORRUPT);
            final String n = schemaTable.getNameField();
            assertTrue(n != null && tableName.equals(n), SqlJetErrorCode.CORRUPT);
            schemaTable.delete();
        } finally {
            schemaTable.close();
        }

        final int page = tableDef.getPage();
        final int moved = btree.dropTable(page);
        if (moved != 0) {
            movePage(page, moved);
        }

        tableDefs.remove(tableName);

    }

    /**
     * @param schemaTable
     * @param tableDef
     * @throws SqlJetException
     */
    private void dropTableIndexes(ISqlJetTableDef tableDef) throws SqlJetException {
        final String tableName = tableDef.getName();
        final Iterator<Map.Entry<String, ISqlJetIndexDef>> iterator = indexDefs.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<String, ISqlJetIndexDef> indexDefEntry = iterator.next();
            final String indexName = indexDefEntry.getKey();
            final ISqlJetIndexDef indexDef = indexDefEntry.getValue();
            if (indexDef.getTableName().equals(tableName)) {
                if (doDropIndex(indexName, true, false)) {
                    iterator.remove();
                }
            }
        }
    }

    /**
     * @param schemaTable
     * @param name
     * @param generateAutoIndexName
     * @throws SqlJetException
     */
    private boolean doDropIndex(String indexName, boolean allowAutoIndex, boolean throwIfFial) throws SqlJetException {

        if (!indexDefs.containsKey(indexName)) {
        	assertFalse(throwIfFial, SqlJetErrorCode.MISUSE);
            return false;
        }
        final SqlJetBaseIndexDef indexDef = (SqlJetBaseIndexDef) indexDefs.get(indexName);

        if (!allowAutoIndex && indexDef.isImplicit()) {
        	assertFalse(throwIfFial, SqlJetErrorCode.MISUSE, String.format(CANT_DELETE_IMPLICIT_INDEX, indexName));
            return false;
        }

        final ISqlJetBtreeSchemaTable schemaTable = openSchemaTable(true);

        try {
            if (!schemaTable.goToRow(indexDef.getRowId()) || !INDEX_TYPE.equals(schemaTable.getTypeField())) {
                assertFalse(throwIfFial, SqlJetErrorCode.INTERNAL);
                return false;
            }
            final String n = schemaTable.getNameField();
            if (null == n || !indexName.equals(n)) {
                assertFalse(throwIfFial, SqlJetErrorCode.INTERNAL);
                return false;
            }

            if (!allowAutoIndex && schemaTable.isNull(ISqlJetBtreeSchemaTable.SQL_FIELD)) {
                assertFalse(throwIfFial, SqlJetErrorCode.INTERNAL, String.format(CANT_DELETE_IMPLICIT_INDEX, indexName));
                return false;
            }

            schemaTable.delete();
        } finally {
            schemaTable.close();
        }

        final int page = indexDef.getPage();
        final int moved = btree.dropTable(page);
        if (moved != 0) {
            movePage(page, moved);
        }

        return true;

    }

    /**
     * @param page
     * @param moved
     * @throws SqlJetException
     */
    private void movePage(final int page, final int moved) throws SqlJetException {
        final ISqlJetBtreeSchemaTable schemaTable = openSchemaTable(true);
        try {
            for (schemaTable.first(); !schemaTable.eof(); schemaTable.next()) {
                final long pageField = schemaTable.getPageField();
                if (pageField == moved) {
                    final String nameField = schemaTable.getNameField();
                    schemaTable.updateRecord(schemaTable.getRowId(), schemaTable.getTypeField(), nameField,
                            schemaTable.getTableField(), page, schemaTable.getSqlField());
                    final ISqlJetIndexDef index = getIndex(nameField);
                    if (index != null) {
                        if (index instanceof SqlJetBaseIndexDef) {
                            ((SqlJetBaseIndexDef) index).setPage(page);
                        }
                    } else {
                        final ISqlJetTableDef table = getTable(nameField);
                        if (table instanceof SqlJetTableDef) {
                            ((SqlJetTableDef) table).setPage(page);
                        }
                    }
                    return;
                }
            }
        } finally {
            schemaTable.close();
        }
    }

    public void dropIndex(String indexName) throws SqlJetException {
        db.getMutex().runVoid(x -> dropIndexSafe(indexName));
    }

    private void dropIndexSafe(String indexName) throws SqlJetException {
    	assertNotEmpty(indexName, SqlJetErrorCode.MISUSE, "Index name must be not empty");
    	assertTrue(indexDefs.containsKey(indexName), SqlJetErrorCode.MISUSE, "Index not found: " + indexName);

        if (doDropIndex(indexName, false, true)) {
            db.getOptions().changeSchemaVersion();
            indexDefs.remove(indexName);
        }

    }

    /**
     * @param tableName
     * @param newTableName
     * @param newColumnDef
     * @return
     * @throws SqlJetException
     */
    private ISqlJetTableDef alterTableSafe(@Nonnull SqlJetAlterTableDef alterTableDef) throws SqlJetException {
        String tableName = alterTableDef.getTableName();
        String tableQuotedName = alterTableDef.getTableQuotedName();
        String newTableName = alterTableDef.getNewTableName();
        ISqlJetColumnDef newColumnDef = alterTableDef.getNewColumnDef();

        assertNotNull(tableName, SqlJetErrorCode.MISUSE, "Table name isn't defined");

        assertFalse(null == newTableName && null == newColumnDef, 
        		SqlJetErrorCode.MISUSE, "Not defined any altering");

        boolean renameTable = newTableName != null;
        String newTableQuotedName = coalesce(alterTableDef.getNewTableQuotedName(), tableQuotedName);
        newTableName = coalesce(newTableName, tableName);

        assertFalse(renameTable && tableDefs.containsKey(newTableName), 
        		SqlJetErrorCode.MISUSE, String.format("Table \"%s\" already exists", newTableName));

        final SqlJetTableDef tableDef = (SqlJetTableDef) tableDefs.get(tableName);
        assertNotNull(tableDef, SqlJetErrorCode.MISUSE, String.format("Table \"%s\" not found", tableName));

        List<ISqlJetColumnDef> columns = tableDef.getColumns();
        if (null != newColumnDef) {

            final String fieldName = newColumnDef.getName();
            if (tableDef.getColumn(fieldName) != null) {
                throw new SqlJetException(SqlJetErrorCode.MISUSE, String.format(
                        "Field \"%s\" already exists in table \"%s\"", fieldName, tableName));
            }

            final List<ISqlJetColumnConstraint> constraints = newColumnDef.getConstraints();
            if (!constraints.isEmpty()) {
                boolean notNull = false;
                boolean defaultValue = false;
                for (final ISqlJetColumnConstraint constraint : constraints) {
                    if (constraint instanceof ISqlJetColumnNotNull) {
                        notNull = true;
                    } else if (constraint instanceof ISqlJetColumnDefault) {
                        defaultValue = true;
                    } else {
                        throw new SqlJetException(SqlJetErrorCode.MISUSE, String.format("Invalid constraint: %s",
                                constraint.toString()));
                    }
                }
                if (notNull && !defaultValue) {
                    throw new SqlJetException(SqlJetErrorCode.MISUSE, "NOT NULL requires to have DEFAULT value");
                }
            }

            columns = new ArrayList<>(columns);
            columns.add(newColumnDef);        
        }

        final int page = tableDef.getPage();
        final long rowId = tableDef.getRowId();

        final SqlJetTableDef alterDef = new SqlJetTableDef(newTableQuotedName, null, tableDef.isTemporary(), false, columns,
                tableDef.getConstraints(), page, rowId);

        final ISqlJetBtreeSchemaTable schemaTable = openSchemaTable(true);
        try {
        	assertTrue(schemaTable.goToRow(rowId), SqlJetErrorCode.CORRUPT);

            final String typeField = schemaTable.getTypeField();
            final String nameField = schemaTable.getNameField();
            final String tableField = schemaTable.getTableField();
            final int pageField = schemaTable.getPageField();

            assertFalse(null == typeField || !TABLE_TYPE.equals(typeField), SqlJetErrorCode.CORRUPT);
            assertFalse(null == nameField || !tableName.equals(nameField), SqlJetErrorCode.CORRUPT);
            assertFalse(null == tableField || !tableName.equals(tableField), SqlJetErrorCode.CORRUPT);
            assertFalse(0 == pageField || pageField != page, SqlJetErrorCode.CORRUPT);

            //final String alteredSql = getTableAlteredSql(schemaTable.getSqlField(), alterTableDef);
            final String alteredSql = alterDef.toSQL();

            db.getOptions().changeSchemaVersion();

            schemaTable.updateRecord(rowId, TABLE_TYPE, newTableName, newTableName, page, alteredSql);

            if (renameTable && !tableName.equals(newTableName)) {
                renameTablesIndices(schemaTable, tableName, newTableName, newTableQuotedName);
            }

            tableDefs.remove(tableName);
            tableDefs.put(newTableName, alterDef);

            return alterDef;
        } finally {
            schemaTable.close();
        }

    }

    /**
     * @param schemaTable
     * @param newTableName
     * @param tableName
     * @param string
     * @throws SqlJetException
     */
    private void renameTablesIndices(final ISqlJetBtreeSchemaTable schemaTable, String tableName, String newTableName,
            String alterTableName) throws SqlJetException {

        final Set<ISqlJetIndexDef> indexes = getIndexes(tableName);
        if (indexes.isEmpty()) {
            return;
        }

        int i = 0;
        for (final ISqlJetIndexDef index : indexes) {
            final String indexName = index.getName();
            final long rowId = index.getRowId();
            final int page = index.getPage();

            assertTrue(schemaTable.goToRow(rowId), SqlJetErrorCode.CORRUPT);

            final String typeField = schemaTable.getTypeField();
            final String nameField = schemaTable.getNameField();
            final String tableField = schemaTable.getTableField();
            final int pageField = schemaTable.getPageField();

            assertFalse(null == typeField || !INDEX_TYPE.equals(typeField), SqlJetErrorCode.CORRUPT);
            assertFalse(null == nameField || !indexName.equals(nameField), SqlJetErrorCode.CORRUPT);
            assertFalse(null == tableField || !tableName.equals(tableField), SqlJetErrorCode.CORRUPT);
            assertFalse(0 == pageField || pageField != page, SqlJetErrorCode.CORRUPT);

            index.setTableName(newTableName);

            String newIndexName = indexName;
            String alteredIndexSql = null;

            if (index.isImplicit()) {
                newIndexName = generateAutoIndexName(tableName, ++i);
                index.setName(newIndexName);
                indexDefs.remove(indexName);
                indexDefs.put(newIndexName, index);
            } else {
                alteredIndexSql = getAlteredIndexSql(schemaTable.getSqlField(), alterTableName);
            }

            schemaTable.updateRecord(rowId, INDEX_TYPE, newIndexName, newTableName, page, alteredIndexSql);
        }

    }

    /**
     * @param sql
     * @param alterTableName
     * @return
     * @throws SqlJetException
     */
    private String getAlteredIndexSql(String sql, String alterTableName) throws SqlJetException {
        final RuleReturnScope parsedSQL = parseIndex(sql);
        final CommonTree ast = (CommonTree) parsedSQL.getTree();
        final CommonToken nameToken = (CommonToken) ((CommonTree) ast.getChild(2)).getToken();
        final CharStream inputStream = nameToken.getInputStream();
        final CommonToken stopToken = (CommonToken) parsedSQL.getStop();
        final StringBuilder b = new StringBuilder();
        b.append(inputStream.substring(0, nameToken.getStartIndex() - 1));
        b.append(alterTableName);
        b.append(inputStream.substring(nameToken.getStopIndex() + 1, stopToken.getStopIndex()));
        return b.toString();
    }

    private ParserRuleReturnScope parseSqlStatement(String sql) throws SqlJetException {
        try {
            CharStream chars = new ANTLRStringStream(sql);
            SqlLexer lexer = new SqlLexer(chars);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SqlParser parser = new SqlParser(tokens);
            return parser.sql_stmt_itself();
        } catch (RecognitionException re) {
            throw new SqlJetException(SqlJetErrorCode.ERROR, "Invalid sql statement: " + sql);
        }
    }

    public ISqlJetTableDef alterTable(String sql) throws SqlJetException {
        final SqlJetAlterTableDef alterTableDef = new SqlJetAlterTableDef(parseSqlStatement(sql));

        return db.getMutex().run(x -> alterTableSafe(alterTableDef));
    }

    public ISqlJetVirtualTableDef createVirtualTable(String sql, int page) throws SqlJetException {
        return db.getMutex().run(x -> createVirtualTableSafe(sql, page));
    }

    private ISqlJetVirtualTableDef createVirtualTableSafe(String sql, int page) throws SqlJetException {

        final RuleReturnScope parseTable = parseTable(sql);
        final CommonTree ast = (CommonTree) parseTable.getTree();

        assertTrue(isCreateVirtualTable(ast), SqlJetErrorCode.ERROR);

        final SqlJetVirtualTableDef tableDef = new SqlJetVirtualTableDef(ast, 0);
        assertNotEmpty(tableDef.getTableName(), SqlJetErrorCode.ERROR);
        
        final String tableName = tableDef.getTableName();

        checkNameReserved(tableName);
        checkFieldNamesRepeatsConflict(tableDef.getTableName(), tableDef.getModuleColumns());

        assertFalse(virtualTableDefs.containsKey(tableName), SqlJetErrorCode.ERROR, "Virtual table \"" + tableName + "\" exists already");

        checkNameConflict(SqlJetSchemaObjectType.VIRTUAL_TABLE, tableName);

        final ISqlJetBtreeSchemaTable schemaTable = openSchemaTable(true);
        final String createVirtualTableSQL = getCreateVirtualTableSql(parseTable);

        try {
            db.getOptions().changeSchemaVersion();

            long rowId = schemaTable.insertRecord(TABLE_TYPE, tableName, tableName, page, createVirtualTableSQL);

            tableDef.setPage(page);
            tableDef.setRowId(rowId);
            virtualTableDefs.put(tableName, tableDef);
            return tableDef;
        } finally {
            schemaTable.close();
        }

    }

    public ISqlJetViewDef createView(String sql) throws SqlJetException {
        return db.getMutex().run(x -> createViewSafe(sql));
    }

    private ISqlJetViewDef createViewSafe(String sql) throws SqlJetException {
        final RuleReturnScope parseView = parseView(sql);
        final CommonTree ast = (CommonTree) parseView.getTree();

        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        final SqlJetViewDef viewDef = new SqlJetViewDef(sql, ast);
        final String viewName = viewDef.getName();
        assertNotEmpty(viewName, SqlJetErrorCode.ERROR);

        if (viewDefs.containsKey(viewName)) {
        	assertTrue(viewDef.isKeepExisting(), SqlJetErrorCode.ERROR, "View \"" + viewName + "\" exists already");
            return viewDefs.get(viewName);
        }
        checkNameConflict(SqlJetSchemaObjectType.VIEW, viewName);
        final ISqlJetBtreeSchemaTable schemaTable = openSchemaTable(true);

        try {
            db.getOptions().changeSchemaVersion();

            long rowId = schemaTable.insertRecord(VIEW_TYPE, viewName, viewName, 0, sql);
            ISqlJetViewDef withRowId = viewDef.withRowId(rowId);
            viewDefs.put(viewName, withRowId);
            return withRowId;
        } finally {
            schemaTable.close();
        }
    }

    /**
     * @param name
     * @throws SqlJetException
     */
    private void checkNameReserved(final String name) throws SqlJetException {
    	assertFalse(isNameReserved(name), SqlJetErrorCode.MISUSE, String.format(NAME_RESERVED, name));
    }

    /**
     * Returns true if name is reserved for internal use.
     *
     * @param name
     * @return true if name is reserved
     */
    private boolean isNameReserved(String name) {
        return name.startsWith("sqlite_");
    }

    /**
     * @param tableName
     * @throws SqlJetException
     */
    private void checkNameConflict(SqlJetSchemaObjectType objectType, final String tableName) throws SqlJetException {
        if (isNameConflict(objectType, tableName)) {
            throw new SqlJetException(String.format("Name conflict: %s named '%s' exists already",
                    objectType.getName(), tableName));
        }
    }

    private boolean isNameConflict(SqlJetSchemaObjectType objectType, String name) {
        if (objectType != SqlJetSchemaObjectType.TABLE && tableDefs.containsKey(name)) {
            return true;
        }
        if (objectType != SqlJetSchemaObjectType.INDEX && indexDefs.containsKey(name)) {
            return true;
        }
        if (objectType != SqlJetSchemaObjectType.VIRTUAL_TABLE && virtualTableDefs.containsKey(name)) {
            return true;
        }
        if (objectType != SqlJetSchemaObjectType.VIEW && viewDefs.containsKey(name)) {
            return true;
        }
        if (objectType != SqlJetSchemaObjectType.TRIGGER && triggerDefs.containsKey(name)) {
            return true;
        }
        return false;
    }

    public ISqlJetIndexDef createIndexForVirtualTable(final String virtualTableName, final String indexName)
            throws SqlJetException {
        return db.getMutex().run(x -> createIndexForVirtualTableSafe(virtualTableName, indexName));
    }

    /**
     * @param virtualTableName
     * @param indexName
     * @return
     * @throws SqlJetException
     */
    private ISqlJetIndexDef createIndexForVirtualTableSafe(String virtualTableName, String indexName)
            throws SqlJetException {

    	assertNotEmpty(virtualTableName, SqlJetErrorCode.ERROR);
    	assertNotEmpty(indexName, SqlJetErrorCode.ERROR);

        checkNameReserved(indexName);

        if (indexDefs.containsKey(indexName)) {
            throw new SqlJetException(SqlJetErrorCode.ERROR, "Index \"" + indexName + "\" exists already");
        }

        checkNameConflict(SqlJetSchemaObjectType.INDEX, indexName);

        final ISqlJetVirtualTableDef tableDef = getVirtualTable(virtualTableName);
        assertNotNull(tableDef, SqlJetErrorCode.ERROR);

        final ISqlJetBtreeSchemaTable schemaTable = openSchemaTable(true);

        try {
            db.getOptions().changeSchemaVersion();

            final ISqlJetIndexDef indexDef = createAutoIndex(schemaTable, tableDef.getTableName(), indexName);

            indexDefs.put(indexName, indexDef);

            return indexDef;
        } finally {
            schemaTable.close();
        }
    }

    public void dropView(String viewName) throws SqlJetException {
        db.getMutex().runVoid(x -> dropViewSafe(viewName));
    }

    private void dropViewSafe(String viewName) throws SqlJetException {
    	assertNotEmpty(viewName, SqlJetErrorCode.MISUSE, "View name must be not empty");
    	assertTrue(viewDefs.containsKey(viewName), SqlJetErrorCode.MISUSE, "View not found: " + viewName);
    	
        final ISqlJetViewDef viewDef = viewDefs.get(viewName);
        final ISqlJetBtreeSchemaTable schemaTable = openSchemaTable(true);

        try {
            db.getOptions().changeSchemaVersion();

            assertTrue(schemaTable.goToRow(viewDef.getRowId()) && VIEW_TYPE.equals(schemaTable.getTypeField()), 
            		SqlJetErrorCode.CORRUPT);
            final String n = schemaTable.getNameField();
            assertFalse(null == n || !viewName.equals(n), SqlJetErrorCode.CORRUPT);
            schemaTable.delete();
        } finally {
            schemaTable.close();
        }
        viewDefs.remove(viewName);
    }

    public void dropTrigger(String triggerName) throws SqlJetException {
        db.getMutex().runVoid(x -> dropTriggerSafe(triggerName));
    }

    private void dropTriggerSafe(String triggerName) throws SqlJetException {
    	assertNotEmpty(triggerName, SqlJetErrorCode.MISUSE, "Trigger name must be not empty");
    	assertTrue(triggerDefs.containsKey(triggerName), SqlJetErrorCode.MISUSE, "Trigger not found: " + triggerName);
    	
        final SqlJetTriggerDef triggerDef = (SqlJetTriggerDef) triggerDefs.get(triggerName);
        final ISqlJetBtreeSchemaTable schemaTable = openSchemaTable(true);

        try {
            db.getOptions().changeSchemaVersion();

            if (!schemaTable.goToRow(triggerDef.getRowId()) || !TRIGGER_TYPE.equals(schemaTable.getTypeField())) {
				throw new SqlJetException(SqlJetErrorCode.CORRUPT);
			}
            final String n = schemaTable.getNameField();
            if (null == n || !triggerName.equals(n)) {
				throw new SqlJetException(SqlJetErrorCode.CORRUPT);
			}
            schemaTable.delete();
        } finally {
            schemaTable.close();
        }
        triggerDefs.remove(triggerName);

    }

    public ISqlJetTriggerDef createTrigger(String sql) throws SqlJetException {
        return db.getMutex().run(x -> createTriggerSafe(sql));
    }

    private ISqlJetTriggerDef createTriggerSafe(String sql) throws SqlJetException {
        final RuleReturnScope parseView = parseTrigger(sql);
        final CommonTree ast = (CommonTree) parseView.getTree();

        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        final SqlJetTriggerDef triggerDef = new SqlJetTriggerDef(sql, ast);
        final String triggerName = triggerDef.getName();
        assertNotEmpty(triggerName, SqlJetErrorCode.ERROR);
        final String tableName = triggerDef.getTableName();
        assertNotEmpty(tableName, SqlJetErrorCode.ERROR);

        if (triggerDefs.containsKey(triggerName)) {
            if (triggerDef.isKeepExisting()) {
                return triggerDefs.get(triggerName);
            }
            throw new SqlJetException(SqlJetErrorCode.ERROR, "Trigger \"" + triggerName + "\" already exists");
        }
        checkNameConflict(SqlJetSchemaObjectType.TRIGGER, triggerName);
        final ISqlJetBtreeSchemaTable schemaTable = openSchemaTable(true);

        try {
            db.getOptions().changeSchemaVersion();

            long rowId = schemaTable.insertRecord(TRIGGER_TYPE, triggerName, tableName, 0, sql);
            triggerDef.setRowId(rowId);
            triggerDefs.put(triggerName, triggerDef);
            return triggerDef;
        } finally {
            schemaTable.close();
        }
    }

}
