/**
 * SqlJetFileSystem.java
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
package org.tmatesoft.sqljet.core.internal.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.EnumSet;
import java.util.Set;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetFile;
import org.tmatesoft.sqljet.core.internal.ISqlJetFileSystem;
import org.tmatesoft.sqljet.core.internal.SqlJetFileAccesPermission;
import org.tmatesoft.sqljet.core.internal.SqlJetFileOpenPermission;
import org.tmatesoft.sqljet.core.internal.SqlJetFileType;
import org.tmatesoft.sqljet.core.internal.fs.util.SqlJetFileUtil;

/**
 * Default implementation of ISqlJetFileSystem.
 * 
 * 
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetFileSystem implements ISqlJetFileSystem {

    private static final String FS_NAME = SqlJetFileSystem.class.getCanonicalName();

    /**
     * Temporary files are named starting with this prefix followed by random
     * alphanumeric characters, and no file extension. They are stored in the
     * OS's standard temporary file directory, and are deleted prior to exit.
     * 
     * If SqlJet is being embedded in another program, you may wish to change
     * the prefix to reflect your program's name, so that if your program exits
     * prematurely, old temporary files can be easily identified.
     * 
     * TODO - specify a way to change SQLJET_TEMP_FILE_PREFIX ( e.g. may be
     * system property name as SQLJET_TEMP_FILE_PREFIX ) .
     * 
     * ********************************************************************
     * 
     * Below some from SQLite comments:
     * 
     * 2006-10-31: The default prefix used to be "sqlite_". But then Mcafee
     * started using SQLite in their anti-virus product and it started putting
     * files with the "sqlite" name in the c:/temp folder. This annoyed many
     * windows users. Those users would then do a Google search for "sqlite",
     * find the telephone numbers of the developers and call to wake them up at
     * night and complain. For this reason, the default name prefix is changed
     * to be "sqlite" spelled backwards. So the temp files are still identified,
     * but anybody smart enough to figure out the code is also likely smart
     * enough to know that calling the developer will not help get rid of the
     * file.
     */

    private static final String SQLJET_TEMP_FILE_PREFIX = "tejlqs_";

    @Override
	public String getName() {
        return FS_NAME;
    }

    @Override
	public @Nonnull ISqlJetFile open(final File path, @Nonnull SqlJetFileType type, @Nonnull Set<SqlJetFileOpenPermission> permissions)
            throws SqlJetException {
        boolean isExclusive = permissions.contains(SqlJetFileOpenPermission.EXCLUSIVE);
        boolean isDelete = permissions.contains(SqlJetFileOpenPermission.DELETEONCLOSE);
        boolean isCreate = permissions.contains(SqlJetFileOpenPermission.CREATE);
        boolean isReadonly = permissions.contains(SqlJetFileOpenPermission.READONLY);
        boolean isReadWrite = !isReadonly;

        /*
         * Check the following statements are true:
         * 
         * (a) Exactly one of the READWRITE and READONLY flags must be set, and
         * (b) if CREATE is set, then READWRITE must also be set, and (c) if
         * EXCLUSIVE is set, then CREATE must also be set. (d) if DELETEONCLOSE
         * is set, then CREATE must also be set.
         */
        assert !isCreate || isReadWrite;
        assert !isExclusive || isCreate;
        assert !isDelete || isCreate;

        /*
         * The main DB, main journal, and master journal are never automatically
         * deleted
         */

        assert SqlJetFileType.MAIN_DB != type || !isDelete;
        assert SqlJetFileType.MAIN_JOURNAL != type || !isDelete;
        assert SqlJetFileType.MASTER_JOURNAL != type || !isDelete;

        final @Nonnull File filePath;

        if (null != path) {
            try {
                filePath = new File(path.getCanonicalPath());
            } catch (IOException e) {
                // TODO may through exception for missing file.
                throw new SqlJetException(SqlJetErrorCode.CANTOPEN, e);
            }
        } else {

            assert isDelete && !(isCreate && (SqlJetFileType.MASTER_JOURNAL == type || SqlJetFileType.MAIN_JOURNAL == type));
            try {
                filePath = getTempFile();
            } catch (IOException e) {
                throw new SqlJetException(SqlJetErrorCode.CANTOPEN, e);
            }

            assert null != filePath;
        }

        if (!isReadWrite && !(filePath.isFile() && filePath.canRead())) {
            throw new SqlJetException(SqlJetErrorCode.CANTOPEN);
        }

        String mode = "rw";
        if (isReadonly && !isReadWrite && !isCreate && !isExclusive) {
            mode = "r";
        } else if (isReadWrite && !isExclusive && filePath.isFile() && !filePath.canWrite() && filePath.canRead()) {
            // force opening as read only.
            Set<SqlJetFileOpenPermission> ro = EnumSet.copyOf(permissions);
            ro.remove(SqlJetFileOpenPermission.CREATE);
            ro.add(SqlJetFileOpenPermission.READONLY);

            return open(filePath, type, ro);
        }

        RandomAccessFile file = null;
        try {
            file = SqlJetFileUtil.openFile(filePath, mode);
        } catch (FileNotFoundException e) {

            if (isReadWrite && !isExclusive) {
                /* Failed to open the file for read/write access. Try read-only. */
                Set<SqlJetFileOpenPermission> ro = EnumSet.copyOf(permissions);
                ro.remove(SqlJetFileOpenPermission.CREATE);
                ro.add(SqlJetFileOpenPermission.READONLY);
                return open(filePath, type, ro);
            }

            throw new SqlJetException(SqlJetErrorCode.CANTOPEN);
        }

        return type.noLock() ? 
        		new SqlJetNoLockFile(this, file, filePath, permissions) : 
        		new SqlJetFile(this, file, filePath, permissions);

    }

    /**
     * @return
     * @throws IOException
     */
	@Override
	public @Nonnull File getTempFile() throws IOException {
        return File.createTempFile(SQLJET_TEMP_FILE_PREFIX, null);
    }

    @Override
	public boolean delete(File path, boolean sync) {
        assert null != path;
        return SqlJetFileUtil.deleteFile(path, sync);
    }

    @Override
	public boolean access(File path, SqlJetFileAccesPermission permission) throws SqlJetException {

        assert null != path;
        assert null != permission;

        switch (permission) {
        case EXISTS:
            return path.exists();

        case READONLY:
            return path.canRead() && !path.canWrite();

        case READWRITE:
            return path.canRead() && path.canWrite();

        default:
            throw new SqlJetException(SqlJetErrorCode.INTERNAL, "Unhandled SqlJetFileAccesPermission value :"
                    + permission.name());
        }
    }

    @Override
	public @Nonnull ISqlJetFile memJournalOpen() {
        return new SqlJetMemJournal();
    }
}
