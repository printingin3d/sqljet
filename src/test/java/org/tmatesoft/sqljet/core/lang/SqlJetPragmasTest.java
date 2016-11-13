/**
 * SqlJetPragmasTest.java
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
package org.tmatesoft.sqljet.core.lang;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.table.SqlJetPragmasHandler;
import org.tmatesoft.sqljet.core.table.ISqlJetOptions;

/**
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 * 
 */
public class SqlJetPragmasTest {

    private TestOptions options;
    private SqlJetPragmasHandler handler;

    @Before
    public void setUp() throws Exception {
        options = new TestOptions();
        handler = new SqlJetPragmasHandler(options);
    }

    @After
    public void tearDown() throws Exception {
        options = null;
        handler = null;
    }

    @Test
    public void testAutovacuum() throws SqlJetException {
        assertFalse(options.isAutovacuum());
        assertFalse(options.isIncrementalVacuum());

        handler.pragma("pragma auto_vacuum=1;");
        assertTrue(options.isAutovacuum());
        assertFalse(options.isIncrementalVacuum());

        Object result = handler.pragma("pragma auto_vacuum;");
        assertTrue(result instanceof Integer);
        assertEquals(1, ((Integer) result).intValue());

        handler.pragma("pragma auto_vacuum = 2;");
        assertFalse(options.isAutovacuum());
        assertTrue(options.isIncrementalVacuum());

        result = handler.pragma("pragma auto_vacuum;");
        assertTrue(result instanceof Integer);
        assertEquals(2, ((Integer) result).intValue());

        handler.pragma("pragma auto_vacuum(0);");
        assertFalse(options.isAutovacuum());
        assertFalse(options.isIncrementalVacuum());

        result = handler.pragma("pragma auto_vacuum;");
        assertTrue(result instanceof Integer);
        assertEquals(0, ((Integer) result).intValue());

        handler.pragma("pragma auto_vacuum=full;");
        assertTrue(options.isAutovacuum());
        assertFalse(options.isIncrementalVacuum());

        handler.pragma("PRAGMA auto_vacuum = INCREMENTAL;");
        assertFalse(options.isAutovacuum());
        assertTrue(options.isIncrementalVacuum());

        handler.pragma("pragma auto_vacuum(NONE);");
        assertFalse(options.isAutovacuum());
        assertFalse(options.isIncrementalVacuum());

        result = handler.pragma("pragma auto_vacuum;");
        assertEquals(Integer.valueOf(0), result);
    }

    @Test(expected = SqlJetException.class)
    public void testAutovacuumError() throws SqlJetException {
        handler.pragma("pragma auto_vacuum = 3;");
    }

    @Test
    public void testCacheSize() throws SqlJetException {
        assertEquals(0, options.getCacheSize());

        handler.pragma("pragma cache_size = 100;");
        assertEquals(100, options.getCacheSize());

        Object result = handler.pragma("pragma cache_size;");
        assertTrue(result instanceof Integer);
        assertEquals(100, ((Integer) result).intValue());
    }
    
    @Test(expected = SqlJetException.class)
    public void testCacheSizeError() throws SqlJetException {
		handler.pragma("pragma cache_size = alpha;");
    }

    @Test
    public void testEncoding() throws SqlJetException {
        assertEquals(null, options.getEncoding());

        handler.pragma("pragma encoding('UTF-8');");
        assertEquals(SqlJetEncoding.UTF8, options.getEncoding());
        assertEquals(SqlJetEncoding.UTF8, handler.pragma("pragma encoding;"));

        handler.pragma("pragma encoding(\"UTF-16\");");
        assertEquals(SqlJetEncoding.UTF16, options.getEncoding());
        assertEquals(SqlJetEncoding.UTF16, handler.pragma("pragma encoding;"));

        handler.pragma("pragma encoding='UTF-16le';");
        assertEquals(SqlJetEncoding.UTF16LE, options.getEncoding());
        assertEquals(SqlJetEncoding.UTF16LE, handler.pragma("pragma encoding;"));

        handler.pragma("pragma encoding = 'UTF-16be';");
        assertEquals(SqlJetEncoding.UTF16BE, options.getEncoding());
        assertEquals(SqlJetEncoding.UTF16BE, handler.pragma("pragma encoding;"));
    }
    
    @Test(expected = SqlJetException.class)
    public void testEncodingError() throws SqlJetException {
    	handler.pragma("pragma encoding('UTF-4');");
    }

    @Test
    public void testLegacyFileFormat() throws SqlJetException {
        assertFalse(options.isLegacyFileFormat());

        handler.pragma("pragma legacy_file_format = true;");
        assertTrue(options.isLegacyFileFormat());

        Object result = handler.pragma("pragma legacy_file_format;");
        assertEquals(Boolean.TRUE, result);
    }
    
    @Test(expected = SqlJetException.class)
    public void testLegacyFileFormatError() throws SqlJetException {
        handler.pragma("pragma legacy_file_format = new;");
    }

    @Test
    public void testSchemaVersion() throws SqlJetException {
        assertEquals(0, options.getSchemaVersion());

        handler.pragma("pragma schema_version = 3;");
        assertEquals(3, options.getSchemaVersion());

        Object result = handler.pragma("pragma schema_version;");
        assertTrue(result instanceof Integer);
        assertEquals(3, ((Integer) result).intValue());
    }
    
    @Test(expected = SqlJetException.class)
    public void testSchemaVersionError() throws SqlJetException {
        handler.pragma("pragma schema_version = beta;");
    }

    @Test
    public void testUserVersion() throws SqlJetException {
        assertEquals(0, options.getUserVersion());

        handler.pragma("pragma user_version = 3;");
        assertEquals(3, options.getUserVersion());

        Object result = handler.pragma("pragma user_version;");
        assertTrue(result instanceof Integer);
        assertEquals(3, ((Integer) result).intValue());
    }

    @Test(expected = SqlJetException.class)
    public void testUserVersionError() throws SqlJetException {
        handler.pragma("pragma user_version = gamma;");
    }
    
    private static class TestOptions implements ISqlJetOptions {

        private int fileFormat;
        private boolean autovacuum, ivacuum;
        private SqlJetEncoding encoding;
        private boolean legacy;
        private int cacheSize;
        private int schemaVersion, userVersion;

        @Override
		public int getFileFormat() {
            return fileFormat;
        }

        @Override
		public void setFileFormat(int fileFormat) throws SqlJetException {
            this.fileFormat = fileFormat;
        }

        @Override
		public boolean isAutovacuum() {
            return autovacuum;
        }

        @Override
		public void setAutovacuum(boolean autovacuum) throws SqlJetException {
            this.autovacuum = autovacuum;
        }

        @Override
		public boolean isIncrementalVacuum() {
            return ivacuum;
        }

        @Override
		public void setIncrementalVacuum(boolean incrementalVacuum) throws SqlJetException {
            this.ivacuum = incrementalVacuum;
        }

        @Override
		public int getCacheSize() {
            return cacheSize;
        }

        @Override
		public void setCacheSize(int pageCacheSize) throws SqlJetException {
            this.cacheSize = pageCacheSize;
        }

        @Override
		public SqlJetEncoding getEncoding() {
            return encoding;
        }

        @Override
		public void setEncoding(SqlJetEncoding encoding) throws SqlJetException {
            this.encoding = encoding;
        }

        @Override
		public boolean isLegacyFileFormat() throws SqlJetException {
            return legacy;
        }

        @Override
		public void setLegacyFileFormat(boolean flag) throws SqlJetException {
            this.legacy = flag;
        }

        @Override
		public int getSchemaVersion() {
            return schemaVersion;
        }

        @Override
		public void setSchemaVersion(int version) {
            this.schemaVersion = version;
        }

        @Override
		public void changeSchemaVersion() throws SqlJetException {
            schemaVersion++;
        }

        @Override
		public boolean verifySchemaVersion() throws SqlJetException {
            return false;
        }

        @Override
		public int getUserVersion() {
            return userVersion;
        }

        @Override
		public void setUserVersion(int userCookie) throws SqlJetException {
            this.userVersion = userCookie;
        }
    }
}
