/**
 * SqlJetOptionsTest.java
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
package org.tmatesoft.sqljet.core.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetLimits;

/**
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 * 
 */
public class SqlJetOptionsTest {

    private File fileDb = new File(SqlJetTableTest.DB);
    private SqlJetDb db;

    @Before
    public void setUp() throws Exception {
        db = SqlJetDb.open(fileDb, true);
    }

    @After
    public void tearDown() throws Exception {
        if (db != null) {
            db.close();
        }
    }

    @Test
    public void testDefaultAutovacuum() throws SqlJetException {
        ISqlJetOptions options = db.getOptions();
        assertFalse(options.isAutovacuum());
        assertFalse(options.isIncrementalVacuum());
    }

    @Test
    public void testDefaultCacheSize() throws SqlJetException {
        ISqlJetOptions options = db.getOptions();
        assertEquals(2000, options.getCacheSize());
    }

    @Test
    public void testDefaultEncoding() throws SqlJetException {
        ISqlJetOptions options = db.getOptions();
        assertEquals(SqlJetEncoding.UTF8, options.getEncoding());
    }

    @Test
    public void testDefaultSchemaVersion() throws SqlJetException {
        ISqlJetOptions options = db.getOptions();
        assertEquals(6, options.getSchemaVersion());
    }

    @Test
    public void testDefaultUserVersion() throws SqlJetException {
        ISqlJetOptions options = db.getOptions();
        assertEquals(0, options.getUserVersion());
    }

    @Test
    public void testDefaultFileFormat() throws SqlJetException {
        ISqlJetOptions options = db.getOptions();
        assertEquals(ISqlJetLimits.SQLJET_MAX_FILE_FORMAT, options.getFileFormat());
    }

    @Test
    public void legacyFileFormatTrue() throws SqlJetException, FileNotFoundException, IOException {
        final File createFile = File.createTempFile(this.getClass().getSimpleName(), null);
        createFile.deleteOnExit();

        final SqlJetDb createDb = SqlJetDb.open(createFile, true);
        createDb.getOptions().setLegacyFileFormat(true);
        createDb.close();

        final SqlJetDb openDb = SqlJetDb.open(createFile, true);
        final int fileFormat = openDb.getOptions().getFileFormat();
        openDb.close();
        Assert.assertEquals(ISqlJetLimits.SQLJET_MIN_FILE_FORMAT, fileFormat);
    }

//    @Test
    public void legacyFileFormatFalse() throws SqlJetException, FileNotFoundException, IOException {
        final File createFile = File.createTempFile(this.getClass().getSimpleName(), null);
        createFile.deleteOnExit();

        final SqlJetDb createDb = SqlJetDb.open(createFile, true);
        createDb.getOptions().setLegacyFileFormat(false);
        createDb.close();

        final SqlJetDb openDb = SqlJetDb.open(createFile, true);
        final int fileFormat = openDb.getOptions().getFileFormat();
        openDb.close();
        Assert.assertNotEquals(ISqlJetLimits.SQLJET_MIN_FILE_FORMAT, fileFormat);
    }
    
}
