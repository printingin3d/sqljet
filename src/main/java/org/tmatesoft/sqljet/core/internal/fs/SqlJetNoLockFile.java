/**
 * SqlJetNoLockFile.java
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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.logging.Logger;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetIOErrorCode;
import org.tmatesoft.sqljet.core.SqlJetIOException;
import org.tmatesoft.sqljet.core.internal.ISqlJetFile;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.SqlJetFileOpenPermission;
import org.tmatesoft.sqljet.core.internal.SqlJetLockType;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.internal.fs.util.SqlJetFileUtil;
import org.tmatesoft.sqljet.core.internal.fs.util.SqlJetTimer;

public class SqlJetNoLockFile implements ISqlJetFile {
    /**
     * Activates logging of files operations.
     */
    private static final @Nonnull String SQLJET_LOG_FILES_PROP = "SQLJET_LOG_FILES";
	
    private static final boolean SQLJET_LOG_FILES = SqlJetUtility.getBoolSysProp(SQLJET_LOG_FILES_PROP, false);
    private static Logger filesLogger = Logger.getLogger(SQLJET_LOG_FILES_PROP);
    
    private static final int SQLJET_DEFAULT_SECTOR_SIZE = 512;

    protected static void OSTRACE(String format, Object... args) {
        if (SQLJET_LOG_FILES) {
            SqlJetUtility.log(filesLogger, format, args);
        }
    }

	protected FileChannel channel;

    protected Set<SqlJetFileOpenPermission> permissions;
    protected RandomAccessFile file;
    private final @Nonnull File filePath;

    private SqlJetLockType lockType = SqlJetLockType.NONE;

    /**
     * @param fileSystem
     * @param file
     * @param filePath
     * @param permissions
     * @param type
     * @param noLock
     */

    public SqlJetNoLockFile(final SqlJetFileSystem fileSystem, final RandomAccessFile file, @Nonnull File filePath,
            final Set<SqlJetFileOpenPermission> permissions) {
        this.file = file;
        this.filePath = filePath;
        this.permissions = Collections.unmodifiableSet(EnumSet.copyOf(permissions));

        this.channel = file.getChannel();

        OSTRACE("OPEN    %s\n", this.filePath);
    }

    @Override
	public synchronized Set<SqlJetFileOpenPermission> getPermissions() {
        // the permissions field is unmodifiable, so we can return it freely
        return permissions;
    }

    @Override
	public synchronized void close() throws SqlJetException {
        if (null == file) {
			return;
		}

        unlock(SqlJetLockType.NONE);
        /*
         * if (!noLock && null != openCount && null != lockInfo &&
         * lockInfo.sharedLockCount > 0) { openCount.pending.add(file);
         * return; }
         */

        try {
            file.close();
            channel.close();
        } catch (IOException e) {
            throw new SqlJetException(SqlJetErrorCode.IOERR, e);
        } finally {
        	file = null;
            channel = null;
        }

        if (permissions.contains(SqlJetFileOpenPermission.DELETEONCLOSE)) {
            if (!SqlJetFileUtil.deleteFile(filePath)) {
                throw new SqlJetIOException(SqlJetIOErrorCode.IOERR_DELETE, String.format("Can't delete file '%s'",
                        filePath.getPath()));
            }
        }

        OSTRACE("CLOSE   %s\n", this.filePath);
    }

    @Override
	public synchronized int read(ISqlJetMemoryPointer buffer, int amount, long offset) throws SqlJetIOException {
        assert amount > 0;
        assert offset >= 0;
        assert buffer != null;
        assert buffer.remaining() >= amount;
        assert file != null;
        assert channel != null;
        try {
            SqlJetTimer timer = new SqlJetTimer();
            final int read = buffer.readFromFile(file, channel, offset, amount);
            timer.end();
            OSTRACE("READ %s %5d %7d %s\n", this.filePath, Integer.valueOf(read), Long.valueOf(offset), timer.format());
            return read < 0 ? 0 : read;
        } catch (IOException e) {
            throw new SqlJetIOException(SqlJetIOErrorCode.IOERR_READ, e);
        }
    }

    @Override
	public synchronized void write(ISqlJetMemoryPointer buffer, int amount, long offset) throws SqlJetIOException {
        assert amount > 0;
        assert offset >= 0;
        assert buffer != null;
        assert buffer.remaining() >= amount;
        assert file != null;
        assert channel != null;
        try {
            SqlJetTimer timer = new SqlJetTimer();
            final int write = buffer.writeToFile(file, channel, offset, amount);
            timer.end();
            OSTRACE("WRITE %s %5d %7d %s\n", this.filePath, Integer.valueOf(write), Long.valueOf(offset), timer.format());
        } catch (IOException e) {
            throw new SqlJetIOException(SqlJetIOErrorCode.IOERR_WRITE, e);
        }
    }

    @Override
	public synchronized void truncate(long size) throws SqlJetIOException {
        assert size >= 0;
        assert file != null;
        try {
            file.setLength(size);
        } catch (IOException e) {
            throw new SqlJetIOException(SqlJetIOErrorCode.IOERR_TRUNCATE, e);
        }
    }

    @Override
	public synchronized void sync() throws SqlJetIOException {
        assert file != null;
        try {
            OSTRACE("SYNC    %s\n", this.filePath);
            channel.force(true);
        } catch (IOException e) {
            throw new SqlJetIOException(SqlJetIOErrorCode.IOERR_FSYNC, e);
        }
    }

    @Override
	public synchronized long fileSize() throws SqlJetException {
        assert file != null;
        try {
            return channel.size();
        } catch (IOException e) {
            throw new SqlJetException(SqlJetErrorCode.IOERR, e);
        }
    }

    @Override
	public synchronized SqlJetLockType getLockType() {
        return lockType;
    }

    @Override
	public synchronized boolean lock(final SqlJetLockType lockType) throws SqlJetIOException {
    	return false;
    }

    @Override
	public synchronized boolean unlock(final SqlJetLockType lockType) throws SqlJetIOException {
    	return false;
    }

    @Override
	public synchronized boolean checkReservedLock() {
    	return false;
    }

    @Override
	public int sectorSize() {
        return SQLJET_DEFAULT_SECTOR_SIZE;
    }

    @Override
	public boolean isMemJournal() {
        return false;
    }

}
