/**
 * SqlJetFileSystemTest.java
 * Copyright (C) 2008 TMate Software Ltd
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
package org.tmatesoft.sqljet.core.internal;

import org.junit.Assert;
import org.junit.Test;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.fs.SqlJetFileSystem;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class SqlJetFileSystemTest extends SqlJetAbstractFileSystemMockTest {

    @Override
    protected void setUpInstances() throws Exception {
        fileSystem = new SqlJetFileSystem();
        super.setUpInstances();
    }

    @Test
    public void testName(){
        final String name = fileSystem.getName();
        Assert.assertTrue("File system should have some unempty name",
                null!=name && !"".equals(name.trim()));
    }
    
    // open()
    @Test
    public void testOpenFileNullTemporary() throws Exception {
        try (ISqlJetFile f = fileSystem.open(null, SqlJetFileType.TEMP_DB, PERM_TEMPORARY)) {
	        Assert.assertNotNull("File should be opened without path if permissions include values:"
	                + " CREATE, READWRITE, DELETEONCLOSE", f);
        }
    }

    // File shouldn't be opened without path if permission is READONLY
    @Test(expected = AssertionError.class )
    public void testOpenFileNullReadonly() throws Exception {
        fileSystem.open(null, SqlJetFileType.TEMP_DB, PERM_READONLY);
    }

    @Test
    public void testOpenReadonly() throws Exception {
        Assert.assertNotNull(path);
        Assert.assertTrue(path.exists());
        try (ISqlJetFile f = fileSystem.open(path, SqlJetFileType.MAIN_DB, PERM_READONLY)) {
        	Assert.assertNotNull("File which exists should be opened with permission READONLY", f);
        }
    }

    // File which doesn't exists shouldn't be opened with permission READONLY
    @Test(expected = SqlJetException.class)
    public void testOpenNewReadonly() throws Exception {
        Assert.assertNotNull(pathNew);
        Assert.assertFalse(pathNew.exists());
        fileSystem.open(pathNew, SqlJetFileType.MAIN_DB, PERM_READONLY);
    }

    // File shouldn't be opened with permissions CREATE and READONLY
    @Test(expected = AssertionError.class)
    public void testOpenCreateReadonly() throws Exception {
        Assert.assertNotNull(pathNew);
        Assert.assertFalse(pathNew.exists());
        fileSystem.open(pathNew, SqlJetFileType.MAIN_DB, PERM_CREATE_READONLY );
    }

    @Test
    public void testOpenCreate() throws Exception {
        Assert.assertNotNull(pathNew);
        Assert.assertFalse(pathNew.exists());
        try (ISqlJetFile f = fileSystem.open(pathNew, SqlJetFileType.MAIN_DB, PERM_CREATE)) {
        	Assert.assertNotNull("File should be created with permission CREATE and READWRITE", f);
        }
    }

    // File shouldn't be created with permission EXCLUSIVE only
    @Test(expected = AssertionError.class)
    public void testOpenExclusiveOnly() throws Exception {
        Assert.assertNotNull(pathNew);
        Assert.assertFalse(pathNew.exists());
        fileSystem.open(pathNew, SqlJetFileType.MAIN_DB, PERM_EXCLUSIVE_ONLY );
    }

    @Test
    public void testOpenExclusiveCreate() throws Exception {
        Assert.assertNotNull(pathNew);
        Assert.assertFalse(pathNew.exists());
        try (ISqlJetFile f = fileSystem.open(pathNew, SqlJetFileType.MAIN_DB, PERM_EXCLUSIVE_CREATE)) {
        	Assert.assertNotNull("File should be created with permission EXCLUSIVE, CREATE and READWRITE", f);
        }
    }

    // delete()
    
    // It shouldn't delete unknown files denoted by null
    @Test(expected = AssertionError.class)
    public void testDeleteNull() throws Exception {
        fileSystem.delete(null, false);
    }
    
    @Test
    public void testDeleteExist() throws Exception {
        Assert.assertNotNull(path);
        Assert.assertTrue(path.exists());
        final boolean d = fileSystem.delete(path, false);
        Assert.assertTrue("If file exists then delete() should return true when success deletes file", d);
    }
        
    @Test
    public void testDeleteNotExist() throws Exception {
        Assert.assertNotNull(pathNew);
        Assert.assertFalse(pathNew.exists());
        final boolean d = fileSystem.delete(pathNew, false);
        Assert.assertFalse("If file doesn't exist then delete() should return false", d);
    }

    // access()
    
    @Test(expected = AssertionError.class)
    public void testAccessNull() throws Exception {
        Assert.assertNotNull(path);
        fileSystem.access(null, null);
        fileSystem.access(path, null);
        fileSystem.access(null, SqlJetFileAccesPermission.EXISTS);
        Assert.fail("It shouldn't access unknown files or permissions denoted by nulls");
    }

    @Test
    public void testAccessExists() throws Exception {
        Assert.assertNotNull(path);
        Assert.assertTrue(path.exists());
        final boolean a = fileSystem.access(path, SqlJetFileAccesPermission.EXISTS);
        Assert.assertTrue("It should be able to access file which exists", a);
    }

    @Test
    public void testAccessExistsNew() throws Exception {
        Assert.assertNotNull(pathNew);
        Assert.assertFalse(pathNew.exists());
        final boolean a = fileSystem.access(pathNew, SqlJetFileAccesPermission.EXISTS);
        Assert.assertFalse("It should be unable to access file which doesnt exist", a);        
    }

    @Test
    public void testAccessExistsReadonly() throws Exception {
        Assert.assertNotNull(pathReadonly);
        Assert.assertFalse(pathReadonly.canWrite());
        final boolean a = fileSystem.access(pathReadonly, SqlJetFileAccesPermission.EXISTS);
        Assert.assertTrue("It should be able to access file which is readonly", a);
    }
    
    @Test
    public void testAccessReadonly() throws Exception {
        Assert.assertNotNull(pathReadonly);
        Assert.assertFalse(pathReadonly.canWrite());
        final boolean a = fileSystem.access(pathReadonly, SqlJetFileAccesPermission.READONLY);
        Assert.assertTrue("It should be unable to access file which doesnt exist", a);        
    }

    @Test
    public void testAccessReadwrite() throws Exception {
        Assert.assertNotNull(path);
        Assert.assertTrue(path.canWrite());
        final boolean a = fileSystem.access(path, SqlJetFileAccesPermission.READWRITE);
        Assert.assertTrue("It should be able to write access to plain file", a);        
    }

    @Test
    public void testAccessReadwriteNew() throws Exception {
        Assert.assertNotNull(pathNew);
        Assert.assertFalse(pathNew.exists());
        final boolean a = fileSystem.access(pathNew, SqlJetFileAccesPermission.READWRITE);
        Assert.assertFalse("It should be unable to write access file which doesnt exist", a);        
    }
    
    @Test
    public void testAccessReadwriteReadonly() throws Exception {
        Assert.assertNotNull(pathReadonly);
        Assert.assertFalse(pathReadonly.canWrite());
        final boolean a = fileSystem.access(pathReadonly, SqlJetFileAccesPermission.READWRITE);
        Assert.assertFalse("It should be unable to write access file which is readonly", a);        
    }
}
