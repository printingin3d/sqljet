/**
 * SqlJetDbHandle.java
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
package org.tmatesoft.sqljet.core.internal.db;

import org.tmatesoft.sqljet.core.SqlAbstractJetMutex;
import org.tmatesoft.sqljet.core.internal.ISqlJetDbHandle;
import org.tmatesoft.sqljet.core.internal.ISqlJetFileSystem;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.internal.fs.SqlJetFileSystemsManager;
import org.tmatesoft.sqljet.core.internal.mutex.SqlJetEmptyMutex;
import org.tmatesoft.sqljet.core.internal.mutex.SqlJetMutex;
import org.tmatesoft.sqljet.core.table.ISqlJetBusyHandler;
import org.tmatesoft.sqljet.core.table.ISqlJetOptions;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetDbHandle implements ISqlJetDbHandle {
    private static final boolean SYNCHRONIZED_THREADING = SqlJetUtility.getBoolSysProp("SQLJET_SYNCHRONIZED_THREADING",
            true);

    private final ISqlJetFileSystem fileSystem;
    private final SqlAbstractJetMutex mutex;
    private ISqlJetOptions options;
    private ISqlJetBusyHandler busyHandler;

    public SqlJetDbHandle() {
        this(SqlJetFileSystemsManager.getManager().find(null));
    }

    public SqlJetDbHandle(ISqlJetFileSystem fs) {
        if (SYNCHRONIZED_THREADING) {
            mutex = new SqlJetMutex();
        } else {
            mutex = new SqlJetEmptyMutex();
        }
        this.fileSystem = fs;
    }

    @Override
    public ISqlJetBusyHandler getBusyHandler() {
        return busyHandler;
    }

    /**
     * @param busyHandler
     *            the busyHandler to set
     */
    @Override
    public void setBusyHandler(ISqlJetBusyHandler busyHandler) {
        this.busyHandler = busyHandler;
    }

    @Override
    public ISqlJetFileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public SqlAbstractJetMutex getMutex() {
        return mutex;
    }

    @Override
    public ISqlJetOptions getOptions() {
        return options;
    }

    @Override
    public void setOptions(ISqlJetOptions options) {
        this.options = options;
    }
}
