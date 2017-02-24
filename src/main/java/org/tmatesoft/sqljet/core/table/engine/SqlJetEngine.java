/**
 * SqlJetEngine.java
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
package org.tmatesoft.sqljet.core.table.engine;

import static org.tmatesoft.sqljet.core.internal.SqlJetAssert.assertNotNull;

import java.io.File;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtree;
import org.tmatesoft.sqljet.core.internal.ISqlJetDbHandle;
import org.tmatesoft.sqljet.core.internal.ISqlJetFile;
import org.tmatesoft.sqljet.core.internal.ISqlJetFileSystem;
import org.tmatesoft.sqljet.core.internal.SqlJetAssert;
import org.tmatesoft.sqljet.core.internal.SqlJetBtreeFlags;
import org.tmatesoft.sqljet.core.internal.SqlJetFileOpenPermission;
import org.tmatesoft.sqljet.core.internal.SqlJetFileType;
import org.tmatesoft.sqljet.core.internal.SqlJetPagerJournalMode;
import org.tmatesoft.sqljet.core.internal.SqlJetSafetyLevel;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.internal.btree.SqlJetBtree;
import org.tmatesoft.sqljet.core.internal.db.SqlJetDbHandle;
import org.tmatesoft.sqljet.core.internal.fs.SqlJetFileSystemsManager;
import org.tmatesoft.sqljet.core.internal.schema.SqlJetSchema;
import org.tmatesoft.sqljet.core.internal.table.SqlJetOptions;
import org.tmatesoft.sqljet.core.table.ISqlJetBooleanTransaction;
import org.tmatesoft.sqljet.core.table.ISqlJetBusyHandler;
import org.tmatesoft.sqljet.core.table.ISqlJetOptions;
import org.tmatesoft.sqljet.core.table.ISqlJetTransaction;
import org.tmatesoft.sqljet.core.table.SqlJetDefaultBusyHandler;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public abstract class SqlJetEngine {

	private static final Set<SqlJetBtreeFlags> READ_FLAGS = Collections
			.unmodifiableSet(SqlJetUtility.of(SqlJetBtreeFlags.READONLY));
	private static final Set<SqlJetFileOpenPermission> READ_PERMISSIONS = Collections
			.unmodifiableSet(SqlJetUtility
					.of(SqlJetFileOpenPermission.READONLY));
	private static final Set<SqlJetBtreeFlags> WRITE_FLAGS = Collections
			.unmodifiableSet(SqlJetUtility.of(SqlJetBtreeFlags.READWRITE,
					SqlJetBtreeFlags.CREATE));
	private static final Set<SqlJetFileOpenPermission> WRITE_PREMISSIONS = Collections
			.unmodifiableSet(SqlJetUtility.of(
					SqlJetFileOpenPermission.READWRITE,
					SqlJetFileOpenPermission.CREATE));

	protected final ISqlJetFileSystem fileSystem;

	protected final boolean writable;
	protected ISqlJetDbHandle dbHandle;
	protected ISqlJetBtree btree;
	protected boolean open = true;
	private final File file;

	private SqlJetTransactionMode transactionMode;

	/**
	 * @param file
	 * @param writable
	 * @param fs
	 * @throws SqlJetException 
	 */
	public SqlJetEngine(final File file, final boolean writable, final ISqlJetFileSystem fs) throws SqlJetException {
		this.file = file;
		this.fileSystem = fs;
		
		ISqlJetDbHandle dbHandle = new SqlJetDbHandle(fileSystem);
		dbHandle.setBusyHandler(new SqlJetDefaultBusyHandler());
		final Set<SqlJetBtreeFlags> flags = EnumSet
				.copyOf(writable ? WRITE_FLAGS : READ_FLAGS);
		final Set<SqlJetFileOpenPermission> permissions = EnumSet
				.copyOf(writable ? WRITE_PREMISSIONS : READ_PERMISSIONS);
		final SqlJetFileType type = file != null ? SqlJetFileType.MAIN_DB
				: SqlJetFileType.TEMP_DB;
		btree = new SqlJetBtree(file, dbHandle, flags, type, permissions);

		this.dbHandle = dbHandle;
		
		// force readonly.
		ISqlJetFile file2 = btree.getPager().getFile();
		if (file2 != null) {
			this.writable = file2.isReadWrite();
		} else {
			this.writable = writable;
		}
	}

	/**
	 * @throws SqlJetException 
     *
     */
	public SqlJetEngine(final File file, final boolean writable) throws SqlJetException {
		this(file, writable, SqlJetFileSystemsManager.getManager().find(null));
	}

	/**
	 * @param file
	 * @param writable
	 * @param fsName
	 * @throws SqlJetException
	 */
	public SqlJetEngine(final File file, final boolean writable, final String fsName) throws SqlJetException {
		this(file, writable, 
				assertNotNull(SqlJetFileSystemsManager.getManager().find(fsName), 
						SqlJetErrorCode.MISUSE, String.format("File system '%s' not found", fsName)));
	}

	/**
	 * @param fs
	 * @param isDefault
	 * @throws SqlJetException
	 */
	public void registerFileSystem(final ISqlJetFileSystem fs, final boolean isDefault) throws SqlJetException {
		SqlJetFileSystemsManager.getManager().register(fs, isDefault);
	}

	/**
	 * @param fs
	 * @throws SqlJetException
	 */
	public void unregisterFileSystem(final ISqlJetFileSystem fs) throws SqlJetException {
		SqlJetFileSystemsManager.getManager().unregister(fs);
	}

	/**
	 * @return database file this engine is created for.
	 */
	public File getFile() {
		return this.file;
	}

	/**
	 * Check write access to data base.
	 * 
	 * @return true if modification is allowed
	 */
	public boolean isWritable() throws SqlJetException {
		return writable;
	}

	public ISqlJetFileSystem getFileSystem() {
		return fileSystem;
	}

	/**
	 * Checks is database open.
	 * 
	 * @return true if database is open.
	 */
	public boolean isOpen() {
		return open;
	}

	protected void checkOpen() throws SqlJetException {
		SqlJetAssert.assertTrue(isOpen(), SqlJetErrorCode.MISUSE, "Database closed");
	}

	public <T> T runSynchronized(ISqlJetTransaction<T, SqlJetEngine> op) throws SqlJetException {
		checkOpen();
		return dbHandle.getMutex().run(mutex -> op.run(this));
	}
	
	public boolean runSynchronizedBool(ISqlJetBooleanTransaction<SqlJetEngine> op)
			throws SqlJetException {
		checkOpen();
		return dbHandle.getMutex().runBool(mutex -> op.run(this));
	}

	/**
	 * Close connection to database. It is safe to call this method if database
	 * connections is closed already.
	 * 
	 * @throws SqlJetException
	 *             it is possible to get exception if there is actvie
	 *             transaction and rollback did not success.
	 */
	public void close() throws SqlJetException {
		if (open) {
			runSynchronized(engine -> {
					if (btree != null) {
						btree.close();
						btree = null;
						open = false;
					}
					closeResources();
					return null;
			});
			if (!open) {
				dbHandle = null;
			}
		}
	}

	protected void closeResources() throws SqlJetException {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#finalize()
	 */
	@Override
	protected void finalize() throws Throwable {
		try {
			if (open) {
				close();
			}
		} finally {
			super.finalize();
		}
	}

	/**
	 * Reads database schema and options.
	 * 
	 * @throws SqlJetException
	 */
	protected void readSchema() throws SqlJetException {
		runSynchronized(engline -> {
				dbHandle.setOptions(new SqlJetOptions(btree, dbHandle));
				btree.setSchema(new SqlJetSchema(dbHandle, btree));
				return null;
		});
	}

	/**
	 * Returns database options.
	 * 
	 * @return options of this database.
	 */
	public ISqlJetOptions getOptions() throws SqlJetException {
		checkOpen();
		if (null == btree.getSchema()) {
			readSchema();
		}
		return dbHandle.getOptions();
	}

	/**
	 * Refreshes database schema.
	 */
	public void refreshSchema() throws SqlJetException {
		if (null == btree.getSchema()
				|| !getOptions().verifySchemaVersion()) {
			readSchema();
		}
	}

	protected SqlJetSchema getSchemaInternal() throws SqlJetException {
		checkOpen();
		refreshSchema();
		return btree.getSchema();
	}

	/**
	 * Get busy handler.
	 * 
	 * @return the busy handler.
	 */
	public ISqlJetBusyHandler getBusyHandler() {
		return dbHandle.getBusyHandler();
	}

	/**
	 * Set busy handler. Busy handler treats situation when database is locked
	 * by other process or thread.
	 * 
	 * @param busyHandler
	 *            the busy handler.
	 */
	public void setBusyHandler(ISqlJetBusyHandler busyHandler) {
		dbHandle.setBusyHandler(busyHandler);
	}

	/**
	 * Set cache size (in count of pages).
	 * 
	 * @param cacheSize
	 *            the count of pages which can hold cache.
	 */
	public void setCacheSize(final int cacheSize) throws SqlJetException {
		checkOpen();
		runSynchronized(engline -> {
				btree.setCacheSize(cacheSize);
				return null;
		});
	}

	/**
	 * Get cache size (in count of pages).
	 * 
	 * @return the count of pages which can hold cache.
	 */
	public int getCacheSize() throws SqlJetException {
		checkOpen();
		refreshSchema();
		return btree.getCacheSize();
	}

	/**
     * Set safety level
     * 
     * @param safetyLevel
     *            
     */
    public void setSafetyLevel(final SqlJetSafetyLevel safetyLevel) throws SqlJetException {
        checkOpen();
        runSynchronized(engine -> {
                btree.setSafetyLevel(safetyLevel);
                return null;
        });
    }

    /**
     * Set journal mode
     * 
     * @param journalMode
     *            
     */
    public void setJournalMode(final SqlJetPagerJournalMode journalMode) throws SqlJetException {
        checkOpen();
        runSynchronized(engine -> {
                btree.setJournalMode(journalMode);
                return null;
        });
    }

    /**
     * Get safety level
     * 
     * @return the safety level set.
     */
    public SqlJetSafetyLevel getSafetyLevel() throws SqlJetException {
        checkOpen();
        refreshSchema();
        return btree.getSafetyLevel();
    }

    /**
     * Get jounrnal mode
     * 
     * @return the safety level set.
     */
    public SqlJetPagerJournalMode getJournalMode() throws SqlJetException {
        checkOpen();
        refreshSchema();
        return runSynchronized(engine -> btree.getJournalMode());
    }

	/**
	 * Returns true if a transaction is active.
	 * 
	 * @return true if there is an active running transaction.
	 */
	public boolean isInTransaction() {
		return transactionMode!=null;
	}

	public SqlJetTransactionMode getTransactionMode() {
		return transactionMode;
	}

	/**
	 * Begin transaction.
	 * 
	 * @param mode
	 *            transaction's mode.
	 */
	public void beginTransaction(@Nonnull SqlJetTransactionMode mode) throws SqlJetException {
		checkOpen();
		runSynchronized(engine -> {
				if (!isTransactionStarted(mode)) {
					doBeginTransaction(mode);
				}
				return null;
		});
	}

	/**
	 * Commits transaction.
	 * 
	 */
	public void commit() throws SqlJetException {
		checkOpen();
		runSynchronized(engine -> {
				if (isInTransaction()) {
					doCommitTransaction();
				}
				return null;
		});
	}

	/**
	 * Rolls back transaction.
	 * 
	 */
	public void rollback() throws SqlJetException {
		checkOpen();
		runSynchronized(engine -> {
				doRollbackTransaction();
				return null;
		});
	}

	/**
	 * Runs transaction.
	 * 
	 * @param op
	 *            transaction's body (closure).
	 * @param mode
	 *            transaction's mode.
	 * @return result of
	 *         {@link ISqlJetTransaction#run(org.tmatesoft.sqljet.core.table.SqlJetDb)}
	 *         call.
	 * @throws SqlJetException
	 */
	public <T> T runEngineTransaction(final ISqlJetTransaction<T, SqlJetEngine> op,
			@Nonnull SqlJetTransactionMode mode) throws SqlJetException {
		checkOpen();
		return runSynchronized(engine -> {
				if (isTransactionStarted(mode)) {
					return op.run(SqlJetEngine.this);
				} else {
					doBeginTransaction(mode);
					boolean success = false;
					try {
						final T result = op.run(SqlJetEngine.this);
						doCommitTransaction();
						success = true;
						return result;
					} finally {
						if (!success) {
							doRollbackTransaction();
						}
						transactionMode = null;
					}
				}
		});
	}
	
	/**
	 * Runs transaction.
	 * 
	 * @param op
	 *            transaction's body (closure).
	 * @param mode
	 *            transaction's mode.
	 * @return result of
	 *         {@link ISqlJetTransaction#run(org.tmatesoft.sqljet.core.table.SqlJetDb)}
	 *         call.
	 * @throws SqlJetException
	 */
	public boolean runEngineTransactionBool(final ISqlJetBooleanTransaction<SqlJetEngine> op,
			@Nonnull SqlJetTransactionMode mode) throws SqlJetException {
		checkOpen();
		return runSynchronizedBool(engine -> {
			if (isTransactionStarted(mode)) {
				return op.run(SqlJetEngine.this);
			} else {
				doBeginTransaction(mode);
				boolean success = false;
				try {
					final boolean result = op.run(SqlJetEngine.this);
					doCommitTransaction();
					success = true;
					return result;
				} finally {
					if (!success) {
						doRollbackTransaction();
					}
					transactionMode = null;
				}
			}
		});
	}

	private boolean isTransactionStarted(final SqlJetTransactionMode mode) {
		return transactionMode != null
				&& (transactionMode == mode || mode == SqlJetTransactionMode.READ_ONLY);
	}

	private void doBeginTransaction(@Nonnull SqlJetTransactionMode mode) throws SqlJetException {
		btree.beginTrans(mode);
		refreshSchema();
		transactionMode = mode;
	}

	private void doCommitTransaction() throws SqlJetException {
		btree.closeAllCursors();
		btree.commit();
		transactionMode = null;
	}

	private void doRollbackTransaction() throws SqlJetException {
		btree.closeAllCursors();
		btree.rollback();
		transactionMode = null;
	}

}