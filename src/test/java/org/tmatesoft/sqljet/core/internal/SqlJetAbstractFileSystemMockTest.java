/**
 * SqlJetAbstractFileSystemMockTest.java
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

import java.io.File;
import java.util.Set;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.tmatesoft.sqljet.core.SqlJetAbstractMockTest;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetIOErrorCode;
import org.tmatesoft.sqljet.core.SqlJetIOException;
import org.tmatesoft.sqljet.core.internal.fs.util.SqlJetFileUtil;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public abstract class SqlJetAbstractFileSystemMockTest extends SqlJetAbstractMockTest {

    /**
     * 
     */
    public static final String MOCK_FILE_SYSTEM = "MockFileSystem";
    /**
     * Permissions for read only access
     */
    public static final Set<SqlJetFileOpenPermission> PERM_READONLY = SqlJetUtility.of(SqlJetFileOpenPermission.READONLY);
    /**
     * Permissions for temporary file
     */
    public static final Set<SqlJetFileOpenPermission> PERM_TEMPORARY = SqlJetUtility.of(SqlJetFileOpenPermission.CREATE,
                    SqlJetFileOpenPermission.READWRITE, 
                    SqlJetFileOpenPermission.DELETEONCLOSE);
    /**
     * Permissions for both read only and write access - one wrong combination
     */
    public static final Set<SqlJetFileOpenPermission> PERM_READONLY_AND_WRITE = SqlJetUtility.of(SqlJetFileOpenPermission.READONLY, SqlJetFileOpenPermission.READWRITE);
    /**
     * Permissions for only create access - other wrong combination
     */
    public static final Set<SqlJetFileOpenPermission> PERM_CREATE_ONLY = SqlJetUtility.of(SqlJetFileOpenPermission.CREATE);
    /**
     * Permissions for create and readonly access - wrong
     */
    public static final Set<SqlJetFileOpenPermission> PERM_CREATE_READONLY = SqlJetUtility.of(SqlJetFileOpenPermission.CREATE, SqlJetFileOpenPermission.READONLY);
    /**
     * Permissions for create and write access - right
     */
    public static final Set<SqlJetFileOpenPermission> PERM_CREATE = SqlJetUtility.of(SqlJetFileOpenPermission.CREATE, SqlJetFileOpenPermission.READWRITE);
    /**
     * Permissions for exclusive, create and write access - right
     */
    public static final Set<SqlJetFileOpenPermission> PERM_EXCLUSIVE_CREATE = SqlJetUtility.of( SqlJetFileOpenPermission.EXCLUSIVE, SqlJetFileOpenPermission.CREATE, 
                        SqlJetFileOpenPermission.READWRITE);
    /**
     * Permissions for exclusive, create and write access - right
     */
    public static final Set<SqlJetFileOpenPermission> PERM_EXCLUSIVE_ONLY = SqlJetUtility.of( SqlJetFileOpenPermission.EXCLUSIVE);
    /**
     * Test file path;
     */
    public static final String TEST_FILE = "SqlJetFileSystemTest";
    protected File path;
    protected File pathNew;
    protected File pathReadonly;
    protected ISqlJetFileSystem fileSystem;
    protected ISqlJetFileSystemsManager fileSystemsManager;    
    
    /**
     * @return the pathPrivateExists
     */
    public File getPathPrivateExists() {
        return path;
    }

    /**
     * @param pathPrivateExists
     *            the pathPrivateExists to set
     */
    public void setPathPrivateExists(File pathPrivateExists) {
        this.path = pathPrivateExists;
    }

    /**
     * @return the pathNew
     */
    public File getPathNew() {
        return pathNew;
    }

    /**
     * @param pathNew the pathNew to set
     */
    public void setPathNew(File pathNew) {
        this.pathNew = pathNew;
    }

    /**
     * @return the fileSystem
     */
    public ISqlJetFileSystem getFileSystem() {
        return fileSystem;
    }

    /**
     * @param fileSystem
     *            the fileSystem to set
     */
    public void setFileSystem(ISqlJetFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    /**
     * @return the fileSystemsManager
     */
    public ISqlJetFileSystemsManager getFileSystemsManager() {
        return fileSystemsManager;
    }

    /**
     * @param fileSystemsManager the fileSystemsManager to set
     */
    public void setFileSystemsManager(ISqlJetFileSystemsManager fileSystemsManager) {
        this.fileSystemsManager = fileSystemsManager;
    }
    
    /**
     * Set up external environment for testing. For example creates files.
     * 
     * @throws Exception
     */
    @Override
	protected void setUpEnvironment() throws Exception {
        super.setUpEnvironment();
    
        path = File.createTempFile(TEST_FILE, null);
        path.deleteOnExit();
        
        pathNew = File.createTempFile(TEST_FILE, null);
        pathNew.deleteOnExit();
        SqlJetFileUtil.deleteFile(pathNew);
        
        pathReadonly = File.createTempFile(TEST_FILE, null);
        pathReadonly.deleteOnExit();
        pathReadonly.setReadOnly();
    }

    /**
     * Set up instances of tested classes.
     * 
     * May be overriden for true implementations.
     * 
     * @throws Exception
     */
    @Override
    protected void setUpInstances() throws Exception {
        super.setUpInstances();

        if(null==fileSystemsManager) fileSystemsManager = 
            EasyMock.createNiceMock(ISqlJetFileSystemsManager.class);
        
        if(null!=fileSystem) return;
    
        fileSystem = EasyMock.createNiceMock(ISqlJetFileSystem.class);
        final ISqlJetFile file = EasyMock.createNiceMock(ISqlJetFile.class);
    
        /* Setup mocks rules */
        
        EasyMock.expect(fileSystem.getName()).andStubReturn(MOCK_FILE_SYSTEM);
        
        // open()
    
        final SqlJetException cantOpen = new SqlJetException(SqlJetErrorCode.CANTOPEN);
        
        EasyMock.expect(fileSystem.open(null, null, null)).andStubThrow(cantOpen);

        EasyMock.expect(fileSystem.open(EasyMock.notNull(), EasyMock.isNull(), EasyMock.isNull())).andStubThrow(cantOpen);
        
        EasyMock.expect(fileSystem.open(EasyMock.notNull(), EasyMock.notNull(), EasyMock.isNull())).andStubThrow(cantOpen);

        EasyMock.expect(fileSystem.open(EasyMock.notNull(), EasyMock.isNull(), EasyMock.notNull())).andStubThrow(cantOpen);
        
        
        EasyMock.expect(fileSystem.open(null, SqlJetFileType.TEMP_DB, PERM_TEMPORARY)).andStubReturn(file);
    
        EasyMock.expect(fileSystem.open(null, SqlJetFileType.TEMP_DB, PERM_READONLY)).andStubThrow(cantOpen);
    
        EasyMock.expect(fileSystem.open(path, SqlJetFileType.MAIN_DB, PERM_READONLY)).andStubReturn(file);
    
        EasyMock.expect(fileSystem.open(pathNew, SqlJetFileType.MAIN_DB, PERM_READONLY)).andStubThrow(cantOpen);
    
        EasyMock.expect(fileSystem.open(
                EasyMock.or((File)EasyMock.isNull(),EasyMock.isA(File.class)), 
                EasyMock.or((SqlJetFileType)EasyMock.isNull(),EasyMock.isA(SqlJetFileType.class)), 
                EasyMock.eq(PERM_READONLY_AND_WRITE))).andStubThrow(cantOpen);
    
        EasyMock.expect(fileSystem.open(
                EasyMock.or((File)EasyMock.isNull(),EasyMock.isA(File.class)), 
                EasyMock.or((SqlJetFileType)EasyMock.isNull(),EasyMock.isA(SqlJetFileType.class)), 
                EasyMock.eq(PERM_CREATE_ONLY))).andStubThrow(cantOpen);
        
        EasyMock.expect(fileSystem.open(
                EasyMock.or((File)EasyMock.isNull(),EasyMock.isA(File.class)), 
                EasyMock.or((SqlJetFileType)EasyMock.isNull(),EasyMock.isA(SqlJetFileType.class)), 
                EasyMock.eq(PERM_CREATE_READONLY))).andStubThrow(cantOpen);
    
        EasyMock.expect(fileSystem.open(EasyMock.isA(File.class),
                EasyMock.eq(SqlJetFileType.MAIN_DB),
                EasyMock.eq(PERM_CREATE))).andStubReturn(file);
    
        EasyMock.expect(fileSystem.open(
                EasyMock.or((File)EasyMock.isNull(),EasyMock.isA(File.class)), 
                EasyMock.or((SqlJetFileType)EasyMock.isNull(),EasyMock.isA(SqlJetFileType.class)), 
                EasyMock.eq(PERM_EXCLUSIVE_ONLY))).andStubThrow(cantOpen);
    
        EasyMock.expect(fileSystem.open(EasyMock.isA(File.class), 
                EasyMock.eq(SqlJetFileType.MAIN_DB),
                EasyMock.eq(PERM_EXCLUSIVE_CREATE))).andStubReturn(file);
    
        EasyMock.expect(fileSystem.open(EasyMock.isA(File.class), 
                EasyMock.eq(SqlJetFileType.MAIN_DB),
                EasyMock.eq(PERM_EXCLUSIVE_CREATE))).andStubReturn(file);
    
        // delete()
    
        final SqlJetException cantDelete = new SqlJetIOException(SqlJetIOErrorCode.IOERR_DELETE);
    
        EasyMock.expect(Boolean.valueOf(fileSystem.delete(EasyMock.<File>isNull(), EasyMock.anyBoolean()))).
        	andStubThrow(cantDelete);
        
        EasyMock.expect(Boolean.valueOf(fileSystem.delete( EasyMock.eq(path), EasyMock.anyBoolean()))).
        	andStubReturn(Boolean.TRUE);
    
        EasyMock.expect(Boolean.valueOf(fileSystem.delete( 
                EasyMock.or( EasyMock.eq(pathNew), EasyMock.eq(pathReadonly)), 
                EasyMock.anyBoolean() ))).andStubReturn(Boolean.FALSE);
        
        // access()
    
        final SqlJetException cantAccess = new SqlJetIOException(SqlJetIOErrorCode.IOERR_ACCESS);
    
        EasyMock.expect(Boolean.valueOf(fileSystem.access(null,null))).andStubThrow(cantAccess);
        
        EasyMock.expect(Boolean.valueOf(fileSystem.access(
        		EasyMock.<File>isNull(), EasyMock.<SqlJetFileAccesPermission>anyObject()))
            ).andStubThrow(cantAccess);
    
        EasyMock.expect(Boolean.valueOf(fileSystem.access(
        		EasyMock.<File>anyObject(), EasyMock.<SqlJetFileAccesPermission>isNull() ))
            ).andStubThrow(cantAccess);
    
        EasyMock.expect(Boolean.valueOf(fileSystem.access(
        		EasyMock.eq(path), EasyMock.<SqlJetFileAccesPermission>anyObject())) 
            ).andStubReturn(Boolean.TRUE);
    
        EasyMock.expect(Boolean.valueOf(fileSystem.access(
        		EasyMock.eq(pathNew), EasyMock.<SqlJetFileAccesPermission>anyObject() )) 
            ).andStubReturn(Boolean.FALSE);
    
        EasyMock.expect(Boolean.valueOf(fileSystem.access(
        		pathReadonly, SqlJetFileAccesPermission.EXISTS )) 
            ).andStubReturn(Boolean.TRUE);
    
        EasyMock.expect(Boolean.valueOf(fileSystem.access( 
                pathReadonly, SqlJetFileAccesPermission.READONLY )) 
            ).andStubReturn(Boolean.TRUE);
        
        EasyMock.expect(Boolean.valueOf(fileSystem.access( 
                pathReadonly, SqlJetFileAccesPermission.READWRITE )) 
            ).andStubReturn(Boolean.FALSE);
        
        // randomness()
    
        final SqlJetException misuse = new SqlJetException(SqlJetErrorCode.MISUSE);
    
        // sleep()
        
        EasyMock.expect(Long.valueOf(fileSystem.sleep(EasyMock.anyInt())) ).andStubAnswer(
                new IAnswer<Long>(){
                    @Override
					@SuppressWarnings("finally")
                    public Long answer() throws Throwable {
                        final Object[] args = EasyMock.getCurrentArguments();
                        if(null==args||0==args.length) throw misuse;
                        final Object arg = args[0];
                        if(null==arg)  throw misuse;
                        if( arg instanceof Integer ){
                            int microseconds = ((Integer) arg).intValue();
                            if( 0>=microseconds ) throw misuse;
                            final long t1 = System.currentTimeMillis();
                            try {
                                Thread.sleep(microseconds);
                            } catch (InterruptedException e) {
                            } finally {
                                final long t2 = System.currentTimeMillis();
                                return Long.valueOf(t2 - t1);
                            }
                        } else throw misuse;
                    }
                }
            );
    
        // currentTime()
        
        EasyMock.expect( Long.valueOf(fileSystem.currentTime()) ).andStubReturn(Long.valueOf(1L));
        
        // Run mocks
        
        EasyMock.replay(fileSystem);
    }

    /**
     * Clean up instances of tested classes.
     */
    @Override
	protected void cleanUpInstances() throws Exception {
        super.cleanUpInstances();
        
        if (fileSystemsManager != null && fileSystem != null) {
            fileSystemsManager.unregister(fileSystem);
        }
        fileSystemsManager = null;
        fileSystem = null;
    }

}
