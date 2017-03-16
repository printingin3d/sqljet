/**
 * SqlJetBtree.java
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
package org.tmatesoft.sqljet.core.internal.btree;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtree;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtreeCursor;
import org.tmatesoft.sqljet.core.internal.ISqlJetDbHandle;
import org.tmatesoft.sqljet.core.internal.ISqlJetFileSystem;
import org.tmatesoft.sqljet.core.internal.ISqlJetKeyInfo;
import org.tmatesoft.sqljet.core.internal.ISqlJetLimits;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetPage;
import org.tmatesoft.sqljet.core.internal.SqlJetAbstractPager;
import org.tmatesoft.sqljet.core.internal.SqlJetAssert;
import org.tmatesoft.sqljet.core.internal.SqlJetAutoVacuumMode;
import org.tmatesoft.sqljet.core.internal.SqlJetBtreeFlags;
import org.tmatesoft.sqljet.core.internal.SqlJetBtreeTableCreateFlags;
import org.tmatesoft.sqljet.core.internal.SqlJetFileOpenPermission;
import org.tmatesoft.sqljet.core.internal.SqlJetFileType;
import org.tmatesoft.sqljet.core.internal.SqlJetPagerJournalMode;
import org.tmatesoft.sqljet.core.internal.SqlJetResultWithOffset;
import org.tmatesoft.sqljet.core.internal.SqlJetSafetyLevel;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.internal.pager.SqlJetMemPager;
import org.tmatesoft.sqljet.core.internal.pager.SqlJetPager;
import org.tmatesoft.sqljet.core.internal.pager.SqlJetTempPager;
import org.tmatesoft.sqljet.core.internal.schema.SqlJetSchema;
import org.tmatesoft.sqljet.core.table.ISqlJetBusyHandler;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class SqlJetBtree implements ISqlJetBtree {
    private static final String SQLITE_FILE_HEADER = "SQLite format 3";

    /*
     * The header string that appears at the beginning of every* SQLite
     * database. Should be 15 character + closing zero
     */
    private static final @Nonnull ISqlJetMemoryPointer MAGIC_HEADER = SqlJetUtility
            .wrapPtr(SqlJetUtility.addZeroByteEnd(SQLITE_FILE_HEADER.getBytes(StandardCharsets.UTF_8)));

    /**
     * Activates logging of b-tree operations.
     */
    private static final @Nonnull String SQLJET_LOG_BTREE_PROP = "SQLJET_LOG_BTREE";

    private static Logger btreeLogger = Logger.getLogger(SQLJET_LOG_BTREE_PROP);

    private static final boolean SQLJET_LOG_BTREE = SqlJetUtility.getBoolSysProp(SQLJET_LOG_BTREE_PROP, false);

    static void TRACE(String format, Object... args) {
        if (SQLJET_LOG_BTREE) {
            SqlJetUtility.log(btreeLogger, format, args);
        }
    }

    static void traceInt(String format, long... args) {
        if (SQLJET_LOG_BTREE) {
            SqlJetUtility.log(btreeLogger, format, args);
        }
    }

    private static final @Nonnull ISqlJetMemoryPointer PAGE1_21 = SqlJetUtility
            .wrapPtr(new byte[] { (byte) 0100, (byte) 040, (byte) 040 });

    /** The database connection holding this btree */
    @Nonnull
    protected final ISqlJetDbHandle db;

    /** Sharable content of this btree */
    @Nonnull
    protected final SqlJetBtreeShared pBt;

    /** True if the underlying file is readonly */
    private boolean readOnly;

    @Nonnull
    protected final SqlJetBtreeCursors cursors = new SqlJetBtreeCursors();

    /**
     * Btree.inTrans may take one of the following values.
     *
     * If the shared-data extension is enabled, there may be multiple users of
     * the Btree structure. At most one of these may open a write transaction,
     * but any number may have active read transactions.
     */
    protected static enum TransMode {
        NONE, READ, WRITE
    }

    /** TRANS_NONE, TRANS_READ or TRANS_WRITE */
    @Nonnull
    protected TransMode inTrans = TransMode.NONE;

    private SqlJetTransactionMode transMode = null;

    /** Pointer to space allocated by sqlite3BtreeSchema() */
    private SqlJetSchema pSchema;

    /**
     * Open a database file.
     *
     * zFilename is the name of the database file. If zFilename is NULL a new
     * database with a random name is created. This randomly named database file
     * will be deleted when sqlite3BtreeClose() is called. If zFilename is
     * ":memory:" then an in-memory database is created that is automatically
     * destroyed when it is closed.
     *
     *
     * @param filename
     *            Name of database file to open
     * @param db
     *            Associated database connection
     * @param flags
     *            Flags
     * @param fsFlags
     *            Flags passed through to VFS open
     * @return
     */
    public SqlJetBtree(File filename, @Nonnull ISqlJetDbHandle db, Set<SqlJetBtreeFlags> flags,
            @Nonnull SqlJetFileType type, @Nonnull Set<SqlJetFileOpenPermission> permissions) throws SqlJetException {
        /*
         * Set the variable isMemdb to true for an in-memory database, or false
         * for a file-based database. This symbol is only required if either of
         * the shared-data or autovacuum features are compiled into the library.
         */
        final boolean isMemdb = filename != null && SqlJetAbstractPager.MEMORY_DB.equals(filename.getPath());

        ISqlJetFileSystem pVfs = db
                .getFileSystem(); /* The VFS to use for this btree */
        this.db = db;

        /*
         * The following asserts make sure that structures used by the btree are
         * the right size. This is to guard against size changes that result
         * when compiling on a different architecture.
         */
        pBt = new SqlJetBtreeShared(); /* Shared part of btree structure */
        if (isMemdb) {
            pBt.pPager = new SqlJetMemPager(pVfs);
        } else if (filename == null) {
            pBt.pPager = new SqlJetTempPager(pVfs, type, permissions);
        } else {
            pBt.pPager = new SqlJetPager(pVfs, filename, SqlJetBtreeFlags.toPagerFlags(flags), type, permissions);
        }
        try {
            ISqlJetMemoryPointer zDbHeader = SqlJetUtility.memoryManager.allocatePtr(100);

            pBt.pPager.readFileHeader(zDbHeader.remaining(), zDbHeader);
            pBt.pPager.setBusyhandler(this::invokeBusyHandler);
            pBt.pPager.setReiniter(page -> pageReinit(page));

            readOnly = pBt.pPager.isReadOnly();
            int pageSize = zDbHeader.getShortUnsigned(16);

            int nReserve;

            if (pageSize < ISqlJetLimits.SQLJET_MIN_PAGE_SIZE || pageSize > ISqlJetLimits.SQLJET_MAX_PAGE_SIZE
                    || (pageSize - 1 & pageSize) != 0) {
                pageSize = pBt.pPager.setPageSize(ISqlJetLimits.SQLJET_DEFAULT_PAGE_SIZE);
                /*
                 * If the magic name ":memory:" will create an in-memory
                 * database, then leave the autoVacuum mode at 0 (do not
                 * auto-vacuum), even if SQLITE_DEFAULT_AUTOVACUUM is true. On
                 * the other hand, if SQLITE_OMIT_MEMORYDB has been defined,
                 * then ":memory:" is just a regular file-name. In this case the
                 * auto-vacuum applies as per normal.
                 */
                if (null != filename && !isMemdb) {
                    pBt.autoVacuumMode = SQLJET_DEFAULT_AUTOVACUUM;
                }
                nReserve = 0;
            } else {
                nReserve = zDbHeader.getByteUnsigned(20);
                pBt.autoVacuumMode = SqlJetAutoVacuumMode.selectVacuumMode(zDbHeader.getInt(36 + 4 * 4) != 0,
                        zDbHeader.getInt(36 + 7 * 4) != 0);
            }
            pBt.usableSize = pageSize - nReserve;
            assert (pageSize & 7) == 0; // 8-byte alignment of pageSize
            pBt.setPageSize(pBt.pPager.setPageSize(pageSize));
        } catch (SqlJetException e) {
            // btree_open_out:
            pBt.pPager.close();
            throw e;
        }
    }

    /**
     * @return the db
     */
    @Override
    public @Nonnull ISqlJetDbHandle getDb() {
        return db;
    }

    /**
     * @return the transMode
     */
    @Override
    public @Nullable SqlJetTransactionMode getTransMode() {
        return transMode;
    }

    /**
     * Invoke the busy handler for a btree.
     */
    public boolean invokeBusyHandler(int number) {
        assert db.getMutex().held();
        final ISqlJetBusyHandler busyHandler = db.getBusyHandler();
        if (busyHandler == null) {
            return false;
        }
        return busyHandler.call(number);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtree#close()
     */
    @Override
    public void close() throws SqlJetException {
        /* Close all cursors opened via this handle. */

        assert this.db.getMutex().held();
        cursors.close();

        /*
         * Rollback any active transaction and free the handle structure. The
         * call to sqlite3BtreeRollback() drops any table-locks held by this
         * handle.
         */
        this.rollback();

        /*
         * The pBt is no longer on the sharing list, so we can access it without
         * having to hold the mutex.
         *
         * Clean out and delete the BtShared object.
         */
        assert cursors.isEmpty();
        pBt.pPager.close();
        pSchema = null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtree#setCacheSize(int)
     */
    @Override
    public void setCacheSize(int mxPage) {
        assert db.getMutex().held();
        pBt.pPager.setCacheSize(mxPage);
    }

    @Override
    public int getCacheSize() {
        return pBt.pPager.getCacheSize();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtree#setSafetyLevel(org.tmatesoft.
     * sqljet .core.SqlJetSafetyLevel)
     */
    @Override
    public void setSafetyLevel(SqlJetSafetyLevel level) {
        assert db.getMutex().held();
        pBt.pPager.setSafetyLevel(level);
    }

    @Override
    public SqlJetSafetyLevel getSafetyLevel() {
        return pBt.pPager.getSafetyLevel();
    }

    @Override
    public SqlJetPagerJournalMode getJournalMode() {
        return pBt.pPager.getJournalMode();
    }

    @Override
    public @Nonnull SqlJetAutoVacuumMode getAutoVacuum() {
        return pBt.autoVacuumMode;
    }

    /**
     * Get a reference to pPage1 of the database file. This will also acquire a
     * readlock on that file.
     *
     * SQLITE_OK is returned on success. If the file is not a well-formed
     * database file, then SQLITE_CORRUPT is returned. SQLITE_BUSY is returned
     * if the database is locked. SQLITE_NOMEM is returned if we run out of
     * memory.
     */
    private void lockBtree() throws SqlJetException {
        if (pBt.pPage1 != null) {
            return;
        }

        SqlJetMemPage pPage1 = pBt.getPage(1, false);

        try {
            /*
             * Do some checking to help insure the file we opened really is a
             * valid database file.
             */
            int nPage = pBt.pPager.getPageCount();
            if (nPage > 0) {
                ISqlJetMemoryPointer page1 = pPage1.getData();
                SqlJetAssert.assertTrue(SqlJetUtility.memcmp(page1, MAGIC_HEADER, 16) == 0, SqlJetErrorCode.NOTADB);
                if (page1.getByteUnsigned(18) > 1) {
                    readOnly = true;
                }
                SqlJetAssert.assertFalse(page1.getByteUnsigned(19) > 1, SqlJetErrorCode.NOTADB);

                /*
                 * The maximum embedded fraction must be exactly 25%. And the
                 * minimum embedded fraction must be 12.5% for both leaf-data
                 * and non-leaf-data. The original design allowed these amounts
                 * to vary, but as of version 3.6.0, we require them to be
                 * fixed.
                 */
                SqlJetAssert.assertTrue(SqlJetUtility.memcmp(page1, 21, PAGE1_21, 0, 3) == 0, SqlJetErrorCode.NOTADB);

                int pageSize = page1.getShortUnsigned(16);
                if ((pageSize - 1 & pageSize) != 0 || pageSize < ISqlJetLimits.SQLJET_MIN_PAGE_SIZE) {
                    throw new SqlJetException(SqlJetErrorCode.NOTADB);
                }
                assert (pageSize & 7) == 0;
                int usableSize = pageSize - page1.getByteUnsigned(20);
                if (pageSize != pBt.getPageSize()) {
                    /*
                     * After reading the first page of the database assuming a
                     * page size of BtShared.pageSize, we have discovered that
                     * the page-size is actually pageSize. Unlock the database,
                     * leave pBt->pPage1 at zero and return SQLITE_OK. The
                     * caller will call this function again with the correct
                     * page-size.
                     */
                    pPage1.releasePage();
                    pBt.usableSize = usableSize;
                    pBt.setPageSize(pBt.pPager.setPageSize(pageSize));
                    return;
                }
                SqlJetAssert.assertTrue(usableSize >= 500, SqlJetErrorCode.NOTADB);
                pBt.usableSize = usableSize;
                pBt.autoVacuumMode = SqlJetAutoVacuumMode.selectVacuumMode(page1.getInt(36 + 4 * 4) > 0,
                        page1.getInt(36 + 7 * 4) > 0);
            }
            assert pBt.getMaxLeaf() + 23 <= pBt.mxCellSize();
            pBt.pPage1 = pPage1;
        } catch (SqlJetException e) {
            // page1_init_failed:
            pPage1.releasePage();
            throw e;
        }
    }

    /**
     * Create a new database by initializing the first page of the file.
     */
    private void newDatabase() throws SqlJetException {
        int nPage = pBt.pPager.getPageCount();
        if (nPage > 0) {
            return;
        }

        ISqlJetMemoryPointer data = pBt.pPage1.getData();
        pBt.pPage1.pDbPage.write();
        data.copyFrom(MAGIC_HEADER, MAGIC_HEADER.remaining());
        data.putShortUnsigned(16, pBt.getPageSize());
        data.putByteUnsigned(18, (byte) 1);
        data.putByteUnsigned(19, (byte) 1);
        assert pBt.usableSize <= pBt.getPageSize() && pBt.usableSize + 255 >= pBt.getPageSize();
        data.putByteUnsigned(20, (byte) (pBt.getPageSize() - pBt.usableSize));
        data.putByteUnsigned(21, (byte) 64);
        data.putByteUnsigned(22, (byte) 32);
        data.putByteUnsigned(23, (byte) 32);
        data.fill(24, 100 - 24, (byte) 0);
        pBt.pPage1.zeroPage(SqlJetMemPage.PTF_INTKEY | SqlJetMemPage.PTF_LEAF | SqlJetMemPage.PTF_LEAFDATA);
        data.putIntUnsigned(36 + 4 * 4, pBt.autoVacuumMode.isAutoVacuum() ? 1 : 0);
        data.putIntUnsigned(36 + 7 * 4, pBt.autoVacuumMode.isIncrVacuum() ? 1 : 0);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.ISqlJetBtree#beginTrans(org.tmatesoft.sqljet
     * .core.SqlJetTransactionMode)
     */
    @Override
    public void beginTrans(@Nonnull SqlJetTransactionMode mode) throws SqlJetException {
        SqlJetException rc = null;

        /*
         * If the btree is already in a write-transaction, or it is already in a
         * read-transaction and a read-transaction is requested, this is a
         * no-op.
         */
        if (inTrans == TransMode.WRITE || inTrans == TransMode.READ && mode == SqlJetTransactionMode.READ_ONLY) {
            return;
        }

        /* Write transactions are not possible on a read-only database */
        SqlJetAssert.assertFalse(readOnly && mode != SqlJetTransactionMode.READ_ONLY, SqlJetErrorCode.READONLY);

        transMode = mode;

        int nBusy = 0;
        do {
            rc = null;

            try {
                while (pBt.pPage1 == null) {
                    lockBtree();
                }

                if (mode != SqlJetTransactionMode.READ_ONLY) {
                    SqlJetAssert.assertFalse(readOnly, SqlJetErrorCode.READONLY);
                    pBt.pPager.begin(mode == SqlJetTransactionMode.EXCLUSIVE);
                    newDatabase();
                }
            } catch (SqlJetException e) {
                btreeLogger.log(Level.WARNING, "Busy?", e);
                rc = e;
                unlockBtreeIfUnused();
            }

        } while (rc != null && rc.getErrorCode() == SqlJetErrorCode.BUSY && inTrans == TransMode.NONE
                && invokeBusyHandler(nBusy) && nBusy++ > -1);

        if (rc == null) {
            inTrans = mode != SqlJetTransactionMode.READ_ONLY ? TransMode.WRITE : TransMode.READ;
        } else {
            throw rc;
        }
    }

    /**
     * This routine does the first phase of a two-phase commit. This routine
     * causes a rollback journal to be created (if it does not already exist)
     * and populated with enough information so that if a power loss occurs the
     * database can be restored to its original state by playing back the
     * journal. Then the contents of the journal are flushed out to the disk.
     * After the journal is safely on oxide, the changes to the database are
     * written into the database file and flushed to oxide. At the end of this
     * call, the rollback journal still exists on the disk and we are still
     * holding all locks, so the transaction has not committed. See
     * sqlite3BtreeCommit() for the second phase of the commit process.
     *
     * This call is a no-op if no write-transaction is currently active on pBt.
     *
     * Otherwise, sync the database file for the btree pBt. zMaster points to
     * the name of a master journal file that should be written into the
     * individual journal file, or is NULL, indicating no master journal file
     * (single database transaction).
     *
     * When this is called, the master journal should already have been created,
     * populated with this journal pointer and synced to disk.
     *
     * Once this is routine has returned, the only thing required to commit the
     * write-transaction for this database file is to delete the journal.
     *
     * @param master
     * @throws SqlJetException
     */
    private void commitPhaseOne() throws SqlJetException {
        if (this.inTrans == TransMode.WRITE) {
            if (pBt.autoVacuumMode.isAutoVacuum()) {
                pBt.autoVacuumCommit();
            }
            pBt.pPager.commitPhaseOne(false);
        }
    }

    /**
     * Commit the transaction currently in progress.
     *
     * This routine implements the second phase of a 2-phase commit. The
     * sqlite3BtreeSync() routine does the first phase and should be invoked
     * prior to calling this routine. The sqlite3BtreeSync() routine did all the
     * work of writing information out to disk and flushing the contents so that
     * they are written onto the disk platter. All this routine has to do is
     * delete or truncate the rollback journal (which causes the transaction to
     * commit) and drop locks.
     *
     * This will release the write lock on the database file. If there are no
     * active cursors, it also releases the read lock.
     *
     * @throws SqlJetException
     */
    private void commitPhaseTwo() throws SqlJetException {
        /*
         * If the handle has a write-transaction open, commit the shared-btrees
         * transaction and set the shared state to TRANS_READ.
         */
        if (this.inTrans == TransMode.WRITE) {
            pBt.pPager.commitPhaseTwo();
        }

        /*
         * Set the handles current transaction state to TRANS_NONE and unlock
         * the pager if this call closed the only read or write transaction.
         */
        this.inTrans = TransMode.NONE;
        unlockBtreeIfUnused();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtree#commit()
     */
    @Override
    public void commit() throws SqlJetException {
        commitPhaseOne();
        commitPhaseTwo();
        transMode = null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtree#rollback()
     */
    @Override
    public void rollback() throws SqlJetException {
        try {
            cursors.saveAllCursors(0, null);
        } catch (SqlJetException e) {
            /*
             * This is a horrible situation. An IO or malloc() error occured
             * whilst trying to save cursor positions. If this is an automatic
             * rollback (as the result of a constraint, malloc() failure or IO
             * error) then the cache may be internally inconsistent (not contain
             * valid trees) so we cannot simply return the error to the caller.
             * Instead, abort all queries that may be using any of the cursors
             * that failed to save.
             */
            tripAllCursors(e.getErrorCode());
        }

        try {
            if (this.inTrans == TransMode.WRITE) {
                try {
                    pBt.pPager.rollback();
                } finally {
                    /*
                     * The rollback may have destroyed the pPage1->aData value.
                     * So call sqlite3BtreeGetPage() on page 1 again to make
                     * sure pPage1->aData is set correctly.
                     */
                    pBt.getPage(1, false).releasePage();
                }
            }
        } finally {
            this.inTrans = TransMode.NONE;
            unlockBtreeIfUnused();
        }

        transMode = null;
    }

    /**
     * During a rollback, when the pager reloads information into the cache so
     * that the cache is restored to its original state at the start of the
     * transaction, for each page restored this routine is called.
     *
     * This routine needs to reset the extra data section at the end of the page
     * to agree with the restored data.
     *
     * @param page
     * @throws SqlJetException
     */
    protected void pageReinit(ISqlJetPage page) throws SqlJetException {
        final SqlJetMemPage pPage = page.getExtra();
        if (pPage != null && pPage.isInit) {
            pPage.isInit = false;
            if (page.getRefCount() > 0) {
                pPage.initPage();
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtree#createTable(java.util.Set)
     */
    @Override
    public int createTable(Set<SqlJetBtreeTableCreateFlags> flags) throws SqlJetException {
        return doCreateTable(flags);
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * @param flags
     * @return
     * @throws SqlJetException
     */
    private int doCreateTable(Set<SqlJetBtreeTableCreateFlags> flags) throws SqlJetException {
        SqlJetMemPage pRoot;
        int pgnoRoot;

        assertWriteTransaction();
        assert !readOnly;

        if (pBt.autoVacuumMode.isAutoVacuum()) {
            /* Move a page here to make room for the root-page */
            int[] pgnoMove = new int[1];
            SqlJetMemPage pPageMove; /* The page to move to. */

            /*
             * Read the value of meta[3] from the database to determine where
             * the* root page of the new table should go. meta[3] is the largest
             * root-page* created so far, so the new root-page is (meta[3]+1).
             */
            pgnoRoot = getMeta(4);
            pgnoRoot++;

            /*
             * The new root-page may not be allocated on a pointer-map page, or
             * the* PENDING_BYTE page.
             */
            while (pgnoRoot == pBt.ptrmapPageNo(pgnoRoot) || pgnoRoot == pBt.pendingBytePage()) {
                pgnoRoot++;
            }
            assert pgnoRoot >= 3;

            /*
             * Allocate a page. The page that currently resides at pgnoRoot will
             * * be moved to the allocated page (unless the allocated page
             * happens* to reside at pgnoRoot).
             */
            pPageMove = pBt.allocatePage(pgnoMove, pgnoRoot, true);

            if (pgnoMove[0] != pgnoRoot) {
                /*
                 * pgnoRoot is the page that will be used for the root-page of*
                 * the new table (assuming an error did not occur). But we were*
                 * allocated pgnoMove. If required (i.e. if it was not allocated
                 * * by extending the file), the current page at position
                 * pgnoMove* is already journaled.
                 */
                SqlJetMemPage.releasePage(pPageMove);

                /* Move the page currently at pgnoRoot to pgnoMove. */
                pRoot = pBt.getPage(pgnoRoot, false);
                SqlJetResultWithOffset<SqlJetPtrMapType> eType = pBt.ptrmapGet(pgnoRoot);
                if (eType.getValue() == SqlJetPtrMapType.PTRMAP_ROOTPAGE
                        || eType.getValue() == SqlJetPtrMapType.PTRMAP_FREEPAGE) {
                    pRoot.releasePage();
                    throw new SqlJetException(SqlJetErrorCode.CORRUPT);
                }
                assert eType.getValue() != SqlJetPtrMapType.PTRMAP_ROOTPAGE;
                assert eType.getValue() != SqlJetPtrMapType.PTRMAP_FREEPAGE;
                try {
                    pRoot.pDbPage.write();
                    pBt.relocatePage(pRoot, eType.getValue(), eType.getOffset(), pgnoMove[0], false);
                } finally {
                    pRoot.releasePage();
                }

                /* Obtain the page at pgnoRoot */
                pRoot = pBt.getPage(pgnoRoot, false);
                try {
                    pRoot.pDbPage.write();
                } catch (SqlJetException e) {
                    pRoot.releasePage();
                    throw e;
                }
            } else {
                pRoot = pPageMove;
            }

            /*
             * Update the pointer-map and meta-data with the new root-page
             * number.
             */
            try {
                pBt.ptrmapPut(pgnoRoot, SqlJetPtrMapType.PTRMAP_ROOTPAGE, 0);
                updateMeta(4, pgnoRoot);
            } catch (SqlJetException e) {
                SqlJetMemPage.releasePage(pRoot);
                throw e;
            }

        } else {
            int[] a = new int[1];
            pRoot = pBt.allocatePage(a, 1, false);
            pgnoRoot = a[0];
        }

        // assert( sqlite3PagerIswriteable(pRoot->pDbPage) );
        try {
            pRoot.zeroPage(SqlJetBtreeTableCreateFlags.toByte(flags) | SqlJetMemPage.PTF_LEAF);
        } finally {
            pRoot.pDbPage.unref();
        }

        return pgnoRoot;

    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtree#isInTrans()
     */
    @Override
    public boolean isInTrans() {
        assert db.getMutex().held();
        return inTrans == TransMode.WRITE;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtree#getSchema()
     */
    @Override
    public SqlJetSchema getSchema() {
        return pSchema;
    }

    @Override
    public void setSchema(SqlJetSchema schema) {
        pSchema = schema;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtree#dropTable(int)
     */
    @Override
    public int dropTable(int table) throws SqlJetException {
        return doDropTable(table);
    }

    /**
     * Erase all information in a table and add the root of the table to the
     * freelist. Except, the root of the principle table (the one on page 1) is
     * never added to the freelist.
     *
     * This routine will fail with SQLITE_LOCKED if there are any open cursors
     * on the table.
     *
     * If AUTOVACUUM is enabled and the page at iTable is not the last root page
     * in the database file, then the last root page in the database file is
     * moved into the slot formerly occupied by iTable and that last slot
     * formerly occupied by the last root page is added to the freelist instead
     * of iTable. In this say, all root pages are kept at the beginning of the
     * database file, which is necessary for AUTOVACUUM to work right. *piMoved
     * is set to the page number that used to be the last root page in the file
     * before the move. If no page gets moved, *piMoved is set to 0. The last
     * root page is recorded in meta[3] and the value of meta[3] is updated by
     * this procedure.
     */
    private int doDropTable(int iTable) throws SqlJetException {
        assertWriteTransaction();

        /*
         * It is illegal to drop a table if any cursors are open on the*
         * database. This is because in auto-vacuum mode the backend may* need
         * to move another root-page to fill a gap left by the deleted* root
         * page. If an open cursor was using this page a problem would* occur.
         */
        SqlJetAssert.assertTrue(cursors.isEmpty(), SqlJetErrorCode.LOCKED);

        int piMoved = 0;
        SqlJetMemPage pPage = pBt.getPage(iTable, false);
        try {
            clearTable(iTable, null);

            if (iTable > 1) {
                if (pBt.autoVacuumMode.isAutoVacuum()) {
                    int maxRootPgno = getMeta(4);

                    if (iTable == maxRootPgno) {
                        /*
                         * If the table being dropped is the table with the
                         * largest root-page* number in the database, put the
                         * root page on the free list.
                         */
                        pPage.freePage();
                    } else {
                        /*
                         * The table being dropped does not have the largest
                         * root-page* number in the database. So move the page
                         * that does into the* gap left by the deleted
                         * root-page.
                         */
                        SqlJetMemPage pMove = pBt.getPage(maxRootPgno, false);
                        try {
                            pBt.relocatePage(pMove, SqlJetPtrMapType.PTRMAP_ROOTPAGE, 0, iTable, false);
                        } finally {
                            pMove.releasePage();
                        }
                        pMove = pBt.getPage(maxRootPgno, false);
                        try {
                            pMove.freePage();
                        } finally {
                            pMove.releasePage();
                        }
                        piMoved = maxRootPgno;
                    }

                    /*
                     * Set the new 'max-root-page' value in the database header.
                     * This* is the old value less one, less one more if that
                     * happens to* be a root-page number, less one again if that
                     * is the* PENDING_BYTE_PAGE.
                     */
                    maxRootPgno--;
                    if (maxRootPgno == pBt.pendingBytePage()) {
                        maxRootPgno--;
                    }
                    if (maxRootPgno == pBt.ptrmapPageNo(maxRootPgno)) {
                        maxRootPgno--;
                    }
                    assert maxRootPgno != pBt.pendingBytePage();

                    updateMeta(4, maxRootPgno);
                } else {
                    pPage.freePage();
                }
            } else {
                /* If sqlite3BtreeDropTable was called on page 1. */
                pPage.zeroPage(SqlJetMemPage.PTF_INTKEY | SqlJetMemPage.PTF_LEAF);
            }
        } finally {
            pPage.releasePage();
        }

        return piMoved;

    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtree#clearTable(int, int[])
     */
    @Override
    public void clearTable(int table, int[] change) throws SqlJetException {
        assertWriteTransaction();
        cursors.saveAllCursors(table, null);
        pBt.clearDatabasePage(table, false, change);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtree#getMeta(int)
     */
    @Override
    public int getMeta(int idx) throws SqlJetException {
        ISqlJetPage pDbPage = null;
        ISqlJetMemoryPointer pP1;

        assert idx >= 0 && idx <= 15;
        if (pBt.pPage1 != null) {
            /*
             * The b-tree is already holding a reference to page 1 of the
             * database file. In this case the required meta-data value can be
             * read directly from the page data of this reference. This is
             * slightly faster than* requesting a new reference from the pager
             * layer.
             */
            pP1 = pBt.pPage1.getData();
        } else {
            /*
             * The b-tree does not have a reference to page 1 of the database
             * file. Obtain one from the pager layer.
             */
            pDbPage = pBt.pPager.acquirePage(1, true);
            pP1 = pDbPage.getData();
        }

        int pMeta = pP1.getInt(36 + idx * 4);

        /*
         * If the b-tree is not holding a reference to page 1, then one was
         * requested from the pager layer in the above block. Release it now.
         */
        if (pDbPage != null) {
            pDbPage.unref();
        }

        return pMeta;
    }

    @Override
    public void updateMeta(int idx, int value) throws SqlJetException {
        assert idx >= 1 && idx <= 15;
        assert pBt.pPage1 != null;

        assertWriteTransaction();
        ISqlJetMemoryPointer pP1 = pBt.pPage1.getData();
        pBt.pPage1.pDbPage.write();
        pP1.putIntUnsigned(36 + idx * 4, value);
        if (idx == 7) {
            assert pBt.autoVacuumMode.isAutoVacuum() || value == 0;
            assert value == 0 || value == 1;
            pBt.autoVacuumMode = pBt.autoVacuumMode.changeIncrMode(value != 0);
        }
    }

    @Override
    public void tripAllCursors(@Nonnull SqlJetErrorCode errCode) throws SqlJetException {
        cursors.tripAllCursors(errCode);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtree#getPager()
     */
    @Override
    public SqlJetAbstractPager getPager() {
        return pBt.pPager;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtree#getCursor(int, boolean,
     * org.tmatesoft.sqljet.core.ISqlJetKeyInfo)
     */
    @Override
    public @Nonnull ISqlJetBtreeCursor getCursor(int table, boolean wrFlag, ISqlJetKeyInfo keyInfo)
            throws SqlJetException {
        return new SqlJetBtreeCursor(this, table, wrFlag, keyInfo);
    }

    /**
     * This routine works like lockBtree() except that it also invokes the busy
     * callback if there is lock contention.
     *
     * @throws SqlJetException
     */
    void lockWithRetry() throws SqlJetException {
        if (inTrans == TransMode.NONE) {
            try {
                beginTrans(SqlJetTransactionMode.READ_ONLY);
            } finally {
                inTrans = TransMode.NONE;
            }
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.internal.ISqlJetBtree#closeAllCursors()
     */
    @Override
    public void closeAllCursors() throws SqlJetException {
        cursors.close();
    }

    /**
     * If there are no outstanding cursors and we are not in the middle of a
     * transaction but there is a read lock on the database, then this routine
     * unrefs the first page of the database file which has the effect of
     * releasing the read lock.
     *
     * If there are any outstanding cursors, this routine is a no-op.
     *
     * If there is a transaction in progress, this routine is a no-op.
     *
     * @throws SqlJetException
     */
    public void unlockBtreeIfUnused() throws SqlJetException {
        if (inTrans == TransMode.NONE && cursors.isEmpty() && pBt.pPage1 != null) {
            if (pBt.pPager.getRefCount() >= 1) {
                assert pBt.pPage1.getData() != null;
                pBt.pPage1.releasePage();
            }
            pBt.pPage1 = null;
        }
    }

    private void assertWriteTransaction() throws SqlJetException {
        SqlJetAssert.assertTrue(inTrans == TransMode.WRITE, SqlJetErrorCode.MISUSE,
                "The operation can only be done in a write transaction");
    }
}
