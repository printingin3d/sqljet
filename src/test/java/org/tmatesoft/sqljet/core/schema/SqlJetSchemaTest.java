/**
 * SqlJetSchemaTest.java
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

import static org.tmatesoft.sqljet.core.IntConstants.FOUR;
import static org.tmatesoft.sqljet.core.IntConstants.ONE;
import static org.tmatesoft.sqljet.core.IntConstants.THREE;
import static org.tmatesoft.sqljet.core.IntConstants.TWO;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.tmatesoft.sqljet.core.AbstractDataCopyTest;
import org.tmatesoft.sqljet.core.IntConstants;
import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetLimits;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetSchemaTest extends AbstractDataCopyTest {
    private static final String SCHEMA_TEST = "SqlJetSchemaTest";

    private static final String DB = SqlJetUtility.getSysProp(SCHEMA_TEST + ".DB", "src/test/data/db/testdb.sqlite");

    private static final boolean DELETE_COPY = SqlJetUtility.getBoolSysProp(SCHEMA_TEST + ".DELETE_COPY", true);

    private static final String REP_CACHE_DB = SqlJetUtility.getSysProp(SCHEMA_TEST + ".REP_CACHE_DB",
            "src/test/data/db/rep-cache/rep-cache.db");
    private static final String REP_CACHE_TABLE = SqlJetUtility
            .getSysProp(SCHEMA_TEST + ".REP_CACHE_TABLE", "rep_cache");

    private static final byte[] TEST_UTF8 = new byte[] { (byte) 0320, (byte) 0242, (byte) 0320, (byte) 0265,
            (byte) 0321, (byte) 0201, (byte) 0321, (byte) 0202 };

    private File fileDb = new File(DB);
    private File fileDbCopy;
    private SqlJetDb db;
    
    @Rule
    public Timeout globalTimeout = Timeout.seconds(IntConstants.DEFAULT_TIMEOUT);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        fileDbCopy = copyFile(fileDb, DELETE_COPY);
        db = SqlJetDb.open(fileDbCopy, true);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        if (null != db) {
			db.close();
		}
    }

    @Test
    public void createTableTest() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
            ISqlJetTableDef createTable = db
                    .createTable("create table test( id integer primary key, name text )");
            ISqlJetTable openTable = db.getTable(createTable.getName());
            logger.info(createTable.toString());
            openTable.insert(null, "test");
        });
    }

    @Test(expected = SqlJetException.class)
    public void createTableTest1() throws SqlJetException {
        db.runVoidWriteTransaction(db -> db.createTable("create table test1( id integer primary key, name text )"));
    }

    @Test
    public void createTableTestUnique() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
            ISqlJetTableDef createTable = db
                    .createTable("create table test( id integer primary key, name text unique )");
            ISqlJetTable openTable = db.getTable(createTable.getName());
            logger.info(createTable.toString());
            openTable.insert(null, "test");
        });
    }

    @Test(expected = SqlJetException.class)
    public void createTableTestUniqueFail() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
            ISqlJetTableDef createTable = db
                    .createTable("create table test( id integer primary key, name text unique )");
            ISqlJetTable openTable = db.getTable(createTable.getName());
            logger.info(createTable.toString());
            openTable.insert("test");
            openTable.insert("test");
        });
    }

    @Test
    public void createIndexTest() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
            ISqlJetTableDef createTable = db
                    .createTable("create table test( id integer primary key, name text )");
            ISqlJetTable openTable = db.getTable(createTable.getName());
            logger.info(createTable.toString());
            openTable.insert(null, "test");
            db.createIndex("CREATE INDEX test_name_index ON test(name);");
        });
    }

    @Test(expected = SqlJetException.class)
    public void createIndexFailTable() throws SqlJetException {
        db.runVoidWriteTransaction(db -> db.createIndex("CREATE INDEX test_name_index ON test(name);"));
    }

    @Test(expected = SqlJetException.class)
    public void createIndexFailColumn() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
            ISqlJetTableDef createTable = db
                    .createTable("create table test( id integer primary key, name text )");
            ISqlJetTable openTable = db.getTable(createTable.getName());
            logger.info(createTable.toString());
            openTable.insert("test");
            db.createIndex("CREATE INDEX test_name_index ON test(test);");
        });
    }

    @Test(expected = SqlJetException.class)
    public void createIndexFailName() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
            ISqlJetTableDef createTable = db
                    .createTable("create table test( id integer primary key, name text )");
            ISqlJetTable openTable = db.getTable(createTable.getName());
            logger.info(createTable.toString());
            openTable.insert("test");
            db.createIndex("CREATE INDEX test_name_index ON test(name);");
            db.createIndex("CREATE INDEX test_name_index ON test(name);");
        });
    }

    @Test
    public void createIndexRepCache() throws SqlJetException, FileNotFoundException, IOException {
        final SqlJetDb repCache = SqlJetDb.open(copyFile(new File(REP_CACHE_DB), DELETE_COPY), true);
        repCache.runVoidWriteTransaction(db -> {
            db.createIndex("CREATE INDEX rep_cache_test_index ON " + REP_CACHE_TABLE
                    + "(hash, revision, offset, size, expanded_size);");
            repCache.getTable(REP_CACHE_TABLE).insert("test", ONE, TWO, THREE, FOUR);
        });
    }

    @Test
    public void createTableRepCache() throws SqlJetException, FileNotFoundException, IOException {
        final SqlJetDb repCache = SqlJetDb.open(copyFile(new File(REP_CACHE_DB), DELETE_COPY), true);
        repCache.runVoidWriteTransaction(db -> {
            ISqlJetTableDef createTable = db
                    .createTable("create table test( id integer primary key, name text )");
            ISqlJetTable openTable = repCache.getTable(createTable.getName());
            logger.info(createTable.toString());
            openTable.insert(null, "test");
            db.createIndex("CREATE INDEX test_index ON test(name);");
            openTable.insert(null, "test1");
        });
    }

    @Test
    public void dropTableRepCache() throws SqlJetException, FileNotFoundException, IOException {
        final SqlJetDb repCache = SqlJetDb.open(copyFile(new File(REP_CACHE_DB), DELETE_COPY), true);
        repCache.runVoidWriteTransaction(db -> db.dropTable(REP_CACHE_TABLE));
        ISqlJetTableDef openTable = repCache.getSchema().getTable(REP_CACHE_TABLE);
        Assert.assertNull(openTable);
    }

    @Test
    public void dropTableTest1() throws SqlJetException {
        db.runVoidWriteTransaction(db -> db.dropTable("test1"));
        ISqlJetTableDef openTable = db.getSchema().getTable("test1");
        Assert.assertNull(openTable);
    }

    @Test
    public void dropIndex() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
            db.dropIndex("test1_name_index");
            db.dropIndex("test1_value_index");
        });
        Assert.assertNull(db.getSchema().getIndex("test1_name_index"));
        Assert.assertNull(db.getSchema().getIndex("test1_value_index"));
    }

    @Test
    public void dropAll() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
            // /Set<String> indices = db.getSchema().getIndexNames();
            Set<String> tables = db.getSchema().getTableNames();
            for (String tableName : tables) {
                ISqlJetTableDef tableDef = db.getSchema().getTable(tableName);
                Set<ISqlJetIndexDef> tableIndices = db.getSchema().getIndexes(tableDef.getName());
                for (ISqlJetIndexDef indexDef : tableIndices) {
                    if (!indexDef.isImplicit()) {
                        db.dropIndex(indexDef.getName());
                    }
                }
                db.dropTable(tableName);
            }
        });
    }

    @Test
    public void createDataBase() throws SqlJetException, FileNotFoundException, IOException {
        File createFile = createTempFile(DELETE_COPY);

        final SqlJetDb createDb = SqlJetDb.open(createFile, true);
        createDb.runVoidWriteTransaction(db -> {
            ISqlJetTableDef createTable = db
                    .createTable("create table test( id integer primary key, name text )");
            logger.info(createTable.toString());
            db.createIndex("CREATE INDEX test_index ON test(name);");
            final ISqlJetTable openTable = createDb.getTable(createTable.getName());
            openTable.insert(null, "test");
            openTable.insert(null, "test1");
        });
    }

    @Test
    public void changeEncoding() throws SqlJetException, FileNotFoundException, IOException {
        File createFile = createTempFile(DELETE_COPY);

        final SqlJetDb createDb = SqlJetDb.open(createFile, true);
        createDb.getOptions().setEncoding(SqlJetEncoding.UTF16LE);
        createDb.runVoidWriteTransaction(db -> {
            final ISqlJetTableDef createTable = db
                    .createTable("create table test( id integer primary key, name text )");
            logger.info(createTable.toString());
            db.createIndex("CREATE INDEX test_index ON test(name);");
            final ISqlJetTable openTable = createDb.getTable(createTable.getName());
            openTable.insert(null, "test");
            openTable.insert(null, "test1");
            openTable.insert(null, new String(TEST_UTF8, StandardCharsets.UTF_8));
        });
    }

    @Test(expected = SqlJetException.class)
    public void changeEncodingFail() throws SqlJetException, FileNotFoundException, IOException {
        File createFile = createTempFile(DELETE_COPY);

        final SqlJetDb createDb = SqlJetDb.open(createFile, true);
        db.runVoidWriteTransaction(db -> {
            final ISqlJetTableDef createTable = db
                    .createTable("create table test( id integer primary key, name text )");
            logger.info(createTable.toString());
            db.createIndex("CREATE INDEX test_index ON test(name);");
            final ISqlJetTable openTable = createDb.getTable(createTable.getName());
            openTable.insert(null, "test");
            openTable.insert(null, "test1");
            openTable.insert(null, new String(TEST_UTF8, StandardCharsets.UTF_8));
            createDb.getOptions().setEncoding(SqlJetEncoding.UTF16LE);
        });
    }

    @Test
    public void changePageCacheSize() throws SqlJetException, FileNotFoundException, IOException {
        File createFile = createTempFile(DELETE_COPY);

        final SqlJetDb createDb = SqlJetDb.open(createFile, true);
        createDb.runVoidWriteTransaction(db -> {
            createDb.getOptions().setCacheSize(1000);
            final ISqlJetTableDef createTable = db
                    .createTable("create table test( id integer primary key, name text )");
            logger.info(createTable.toString());
            db.createIndex("CREATE INDEX test_index ON test(name);");
            final ISqlJetTable openTable = createDb.getTable(createTable.getName());
            openTable.insert(null, "test");
            openTable.insert(null, "test1");
            openTable.insert(null, new String(TEST_UTF8, StandardCharsets.UTF_8));
        });

        createDb.close();

        final SqlJetDb openDb = SqlJetDb.open(createFile, true);
        final int cacheSize = openDb.getOptions().getCacheSize();
        Assert.assertEquals(1000, cacheSize);

    }

    @Test
    public void changeVacuum() throws SqlJetException, FileNotFoundException, IOException {
        File createFile = createTempFile(DELETE_COPY);

        final SqlJetDb createDb = SqlJetDb.open(createFile, true);
        createDb.getOptions().setAutovacuum(true);
        createDb.getOptions().setIncrementalVacuum(true);
        createDb.runVoidWriteTransaction(db -> {
            final ISqlJetTableDef createTable = db
                    .createTable("create table test( id integer primary key, name text )");
            logger.info(createTable.toString());
            db.createIndex("CREATE INDEX test_index ON test(name);");
            final ISqlJetTable openTable = createDb.getTable(createTable.getName());
            openTable.insert(null, "test");
            openTable.insert(null, "test1");
            openTable.insert(null, new String(TEST_UTF8, StandardCharsets.UTF_8));
        });

        createDb.close();

        final SqlJetDb checkDb = SqlJetDb.open(createFile, true);
        Assert.assertTrue(checkDb.getOptions().isAutovacuum());
        Assert.assertTrue(checkDb.getOptions().isIncrementalVacuum());
        checkDb.close();
    }

    @Test
    public void changeSchemaVersion() throws SqlJetException, FileNotFoundException, IOException {
        File createFile = createTempFile(DELETE_COPY);

        final SqlJetDb createDb = SqlJetDb.open(createFile, true);
        createDb.runVoidWriteTransaction(db -> createDb.getOptions().setSchemaVersion(123));
        createDb.close();

        final SqlJetDb openDb = SqlJetDb.open(createFile, true);
        final int schemaVersion = openDb.getOptions().getSchemaVersion();
        Assert.assertEquals(123, schemaVersion);
    }

    @Test
    public void changeFileFormatMin() throws SqlJetException, FileNotFoundException, IOException {
        File createFile = createTempFile(DELETE_COPY);

        final SqlJetDb createDb = SqlJetDb.open(createFile, true);
        createDb.getOptions().setFileFormat(ISqlJetLimits.SQLJET_MIN_FILE_FORMAT);
        createDb.close();

        final SqlJetDb openDb = SqlJetDb.open(createFile, true);
        final int fileFormat = openDb.getOptions().getFileFormat();
        Assert.assertEquals(ISqlJetLimits.SQLJET_MIN_FILE_FORMAT, fileFormat);
    }

    @Test
    public void changeFileFormatMax() throws SqlJetException, FileNotFoundException, IOException {
        File createFile = createTempFile(DELETE_COPY);

        final SqlJetDb createDb = SqlJetDb.open(createFile, true);
        createDb.getOptions().setFileFormat(ISqlJetLimits.SQLJET_MAX_FILE_FORMAT);
        createDb.close();

        final SqlJetDb openDb = SqlJetDb.open(createFile, true);
        final int fileFormat = openDb.getOptions().getFileFormat();
        Assert.assertEquals(ISqlJetLimits.SQLJET_MAX_FILE_FORMAT, fileFormat);

    }

    @Test(expected = SqlJetException.class)
    public void createIndexUniqueFail() throws SqlJetException {
        db.runVoidWriteTransaction(db -> {
            final ISqlJetTableDef createTable = db
                    .createTable("create table test( id integer primary key, name text )");
            logger.info(createTable.toString());
            final ISqlJetIndexDef createIndex = db.createIndex("create unique index index_test_text on test(name)");
            logger.info(createIndex.toString());
        });
        db.close();
        db.open();
        db.runVoidWriteTransaction(db -> {
            final ISqlJetTable openTable = db.getTable("test");
            openTable.insert(null, "test");
            openTable.insert(null, "test");
        });
    }

}
