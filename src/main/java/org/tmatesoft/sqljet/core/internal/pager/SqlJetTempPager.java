/**
 * Pager.java
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
package org.tmatesoft.sqljet.core.internal.pager;

import static org.tmatesoft.sqljet.core.internal.SqlJetAssert.assertNoError;

import java.io.File;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetIOErrorCode;
import org.tmatesoft.sqljet.core.SqlJetIOException;
import org.tmatesoft.sqljet.core.internal.ISqlJetFile;
import org.tmatesoft.sqljet.core.internal.ISqlJetFileSystem;
import org.tmatesoft.sqljet.core.internal.ISqlJetLimits;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetPage;
import org.tmatesoft.sqljet.core.internal.ISqlJetPageCache;
import org.tmatesoft.sqljet.core.internal.ISqlJetPageCallback;
import org.tmatesoft.sqljet.core.internal.SqlJetAbstractPager;
import org.tmatesoft.sqljet.core.internal.SqlJetAssert;
import org.tmatesoft.sqljet.core.internal.SqlJetFileAccesPermission;
import org.tmatesoft.sqljet.core.internal.SqlJetFileOpenPermission;
import org.tmatesoft.sqljet.core.internal.SqlJetFileType;
import org.tmatesoft.sqljet.core.internal.SqlJetPageFlags;
import org.tmatesoft.sqljet.core.internal.SqlJetPagerJournalMode;
import org.tmatesoft.sqljet.core.internal.SqlJetSafetyLevel;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.internal.fs.SqlJetFile;
import org.tmatesoft.sqljet.core.table.ISqlJetBusyHandler;

/**
 * A open page cache is an instance of the following structure.
 *
 * Pager.errCode may be set to SQLITE_IOERR, SQLITE_CORRUPT, or or SQLITE_FULL.
 * Once one of the first three errors occurs, it persists and is returned as the
 * result of every major pager API call. The SQLITE_FULL return code is slightly
 * different. It persists only until the next successful rollback is performed
 * on the pager cache. Also, SQLITE_FULL does not affect the sqlite3PagerGet()
 * and sqlite3PagerLookup() APIs, they may still be used successfully.
 *
 * Managing the size of the database file in pages is a little complicated. The
 * variable Pager.dbSize contains the number of pages that the database image
 * currently contains. As the database image grows or shrinks this variable is
 * updated. The variable Pager.dbFileSize contains the number of pages in the
 * database file. This may be different from Pager.dbSize if some pages have
 * been appended to the database image but not yet written out from the cache to
 * the actual file on disk. Or if the image has been truncated by an
 * incremental-vacuum operation. The Pager.dbOrigSize variable contains the
 * number of pages in the database image when the current transaction was
 * opened. The contents of all three of these variables is only guaranteed to be
 * correct if the boolean Pager.dbSizeValid is true.
 *
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class SqlJetTempPager extends SqlJetAbstractPager implements ISqlJetLimits, ISqlJetPageCallback {
    /**
     * Activates logging of pager operations.
     */
    private static final @Nonnull String SQLJET_LOG_PAGER_PROP = "SQLJET_LOG_PAGER";

    private static Logger pagerLogger = Logger.getLogger(SQLJET_LOG_PAGER_PROP);

    private static final boolean SQLJET_LOG_PAGER = SqlJetUtility.getBoolSysProp(SQLJET_LOG_PAGER_PROP, false);

    static void PAGERTRACE(String format, Object... args) {
        if (SQLJET_LOG_PAGER) {
            SqlJetUtility.log(pagerLogger, format, args);
        }
    }

    @Override
    public String pagerId() {
        return null;
    }

    /**
     * The maximum allowed sector size. 16MB. If the xSectorsize() method
     * returns a value larger than this, then MAX_SECTOR_SIZE is used instead.
     * This could conceivably cause corruption following a power failure on such
     * a system. This is currently an undocumented limit.
     */
    private static final int MAX_SECTOR_SIZE = 0x0100000;

    private final ISqlJetFileSystem fileSystem;
    private final @Nonnull Set<SqlJetFileOpenPermission> permissions;

    /** True if header of journal is synced */
    private boolean journalStarted;

    /** True if cached pages have changed */
    private boolean dirtyCache;

    /** True if there are any changes to the Db */
    private boolean dbModified;

    /** Set after incrementing the change-counter */
    private boolean changeCountDone;

    /** Set when dbSize is correct */
    private boolean dbSizeValid;

    /** Number of pages in the database file */
    private int dbFileSize;

    /** Number of pages written to the journal */
    private int nRec;

    /** Quasi-random value added to every checksum */
    private long cksumInit;

    /** Maximum allowed size of the database */
    private int mxPgno;

    /** File descriptors for database and journal */
    private final ISqlJetFile fd;
    private ISqlJetFile jfd;

    /** Current byte offset in the journal file */
    private long journalOff;

    /** Byte offset to previous journal header */
    private long journalHdr;

    /** Changes whenever database file changes */
    private final @Nonnull ISqlJetMemoryPointer dbFileVers = SqlJetUtility.memoryManager.allocatePtr(16);

    /** Size limit for persistent journal files */
    private final long journalSizeLimit;

    /** Pointer to page cache object */
    private final ISqlJetPageCache pageCache;

    private boolean synced = false;;

    /** One of several kinds of errors */
    private SqlJetErrorCode errCode;

    @Override
    public boolean isLockedState() {
        return true;
    }

    @Override
    public boolean isReservedState() {
        return true;
    }

    /**
     * Open a new page cache.
     * 
     * Creates a randomly-named temporary file is created and used as the file
     * to be cached. The file will be deleted automatically when it is closed.
     * 
     * @param fs
     *            The file system to use
     * @param fileName
     *            Name of the database file to open
     * @param flags
     *            flags controlling this file
     * @param type
     *            file type passed through to
     *            {@link ISqlJetFileSystem#open(java.io.File, SqlJetFileType, Set)}
     * @param permissions
     *            permissions passed through to
     *            {@link ISqlJetFileSystem#open(java.io.File, SqlJetFileType, Set)}
     * @throws SqlJetException
     */
    public SqlJetTempPager(final ISqlJetFileSystem fileSystem, @Nonnull SqlJetFileType type,
            @Nonnull Set<SqlJetFileOpenPermission> permissions) throws SqlJetException {
        this.fileSystem = fileSystem;
        this.permissions = EnumSet.copyOf(permissions);

        int szPageDflt = SQLJET_DEFAULT_PAGE_SIZE;
        /*
         * If a temporary file is requested, it is not opened immediately. In
         * this case we accept the default page size and delay actually opening
         * the file until the first call to OsWrite().
         */
        this.fd = openTemp(type);

        pageCache = new SqlJetPageCache(szPageDflt, true, this);

        PAGERTRACE("OPEN %s %s\n", pagerId(), null);

        this.dbSizeValid = false;
        this.pageSize = szPageDflt;
        this.mxPgno = SQLJET_MAX_PAGE_COUNT;

        this.journalSizeLimit = SQLJET_DEFAULT_JOURNAL_SIZE_LIMIT;

        setSectorSize();
        setSafetyLevel(SqlJetSafetyLevel.NORMAL);
    }

    /**
     * Set the sectorSize for the pager.
     *
     * The sector size is at least as big as the sector size reported by
     * {@link SqlJetFile#sectorSize()}.
     */
    private void setSectorSize() {
        if (this.sectorSize < SQLJET_MIN_SECTOR_SIZE) {
            this.sectorSize = SQLJET_MIN_SECTOR_SIZE;
        }
        if (this.sectorSize > MAX_SECTOR_SIZE) {
            this.sectorSize = MAX_SECTOR_SIZE;
        }
    }

    @Override
    public ISqlJetFile getFile() {
        return fd;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public SqlJetPagerJournalMode getJournalMode() {
        return SqlJetPagerJournalMode.DELETE;
    }

    @Override
    public void setBusyhandler(final ISqlJetBusyHandler busyHandler) {
    }

    @Override
    public int setPageSize(final int pageSize) throws SqlJetException {
        checkErrorCode();
        SqlJetAssert.assertTrue(pageSize >= SQLJET_MIN_PAGE_SIZE && pageSize <= SQLJET_MAX_PAGE_SIZE,
                SqlJetErrorCode.CORRUPT);
        if (pageSize != this.pageSize && pageCache.getRefCount() == 0) {
            reset();
            this.pageSize = pageSize;
            setSectorSize();
            pageCache.setPageSize(pageSize);
        }
        return this.pageSize;
    }

    /**
     * @throws SqlJetExceptionRemove
     */
    private void checkErrorCode() throws SqlJetException {
        assertNoError(errCode);
    }

    /**
     ** Find a page in the hash table given its page number. Return a pointer to
     * the page or NULL if not found.
     *
     * @throws SqlJetException
     */
    @Override
    public ISqlJetPage lookup(int pageNumber) throws SqlJetException {
        return pageCache.fetch(pageNumber, false);
    }

    /**
     * Clear the in-memory cache. This routine sets the state of the pager back
     * to what it was when it was first opened. Any outstanding pages are
     * invalidated and subsequent attempts to access those pages will likely
     * result in a coredump.
     *
     */
    private void reset() {
        if (null != errCode) {
            return;
        }
        if (pageCache != null) {
            pageCache.clear();
        }
    }

    @Override
    public void setCacheSize(int cacheSize) {
        pageCache.setCacheSize(cacheSize);
    }

    @Override
    public int getCacheSize() {
        return pageCache.getCachesize();
    }

    @Override
    public void readFileHeader(final int count, @Nonnull ISqlJetMemoryPointer buffer) throws SqlJetException {
        if (null != fd) {
            try {
                fd.read(buffer, count, 0);
            } catch (final SqlJetIOException e) {
                if (SqlJetIOErrorCode.IOERR_SHORT_READ != e.getIoErrorCode()) {
                    throw e;
                }
            }
        }
    }

    @Override
    public int getPageCount() throws SqlJetException {
        checkErrorCode();

        int n = 0;

        if (dbSizeValid) {
            n = dbSize;
        } else {
            long l = 0;
            if (null != fd) {
                try {
                    l = fd.fileSize();
                } catch (SqlJetException e) {
                    error(e);
                    throw e;
                }
            }
            if (l > 0 && l < pageSize) {
                n = 1;
            } else {
                n = (int) (l / pageSize);
            }
            dbSize = n;
            dbFileSize = n;
            dbSizeValid = true;
        }
        if (n == ISqlJetFile.PENDING_BYTE / pageSize) {
            n++;
        }
        if (n > mxPgno) {
            mxPgno = n;
        }
        return n;
    }

    /**
     * This function should be called when an error occurs within the pager
     * code. The first argument is a pointer to the pager structure, the second
     * the error-code about to be returned by a pager API function. The value
     * returned is a copy of the second argument to this function.
     *
     * If the second argument is SQLITE_IOERR, SQLITE_CORRUPT, or SQLITE_FULL
     * the error becomes persistent. Until the persisten error is cleared,
     * subsequent API calls on this Pager will immediately return the same error
     * code.
     *
     * A persistent error indicates that the contents of the pager-cache cannot
     * be trusted. This state can be cleared by completely discarding the
     * contents of the pager-cache. If a transaction was active when the
     * persistent error occured, then the rollback journal may need to be
     * replayed.
     *
     * @param e
     */
    private void error(final SqlJetException e) {
        final SqlJetErrorCode c = e.getErrorCode();
        if (SqlJetErrorCode.FULL == c || SqlJetErrorCode.IOERR == c || SqlJetErrorCode.CORRUPT == c) {
            errCode = c;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetPager#close()
     */
    @Override
    public void close() throws SqlJetException {
        errCode = null;
        reset();
        /*
         * Set Pager.journalHdr to -1 for the benefit of the pager_playback()
         * call which may be made from within pagerUnlockAndRollback(). If it is
         * not -1, then the unsynced portion of an open journal file may be
         * played back into the database. If a power failure occurs while this
         * is happening, the database may become corrupt.
         */
        journalHdr = -1;
        unlockAndRollback();
        PAGERTRACE("CLOSE %s\n", pagerId());
        if (journalOpen) {
            if (null != jfd) {
                jfd.close();
            }
        }
        pagesInJournal = null;
        pagesAlwaysRollback.clear();
        if (null != fd) {
            fd.close();
        }

        /*
         * Temp files are automatically deleted by the OS if( pPager->tempFile
         * ){ sqlite3OsDelete(pPager->zFilename); }
         */

        if (pageCache != null) {
            pageCache.close();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetPager#acquire(int, boolean)
     */
    @Override
    public ISqlJetPage acquirePage(final int pageNumber, final boolean read) throws SqlJetException {
        assert pageCache.getRefCount() > 0 || pageNumber == 1;

        if (pageNumber > PAGER_MAX_PGNO || pageNumber == 0 || pageNumber == ISqlJetFile.PENDING_BYTE / pageSize + 1) {
            throw new SqlJetException(SqlJetErrorCode.CORRUPT);
        }

        /*
         * If this is the first page accessed, then get a SHARED lock on the
         * database file. pagerSharedLock() is a no-op if a database lock is
         * already held.
         */
        sharedLock();

        final ISqlJetPage page = pageCache.fetch(pageNumber, true);
        SqlJetAssert.assertNotNull(page, SqlJetErrorCode.INTERNAL, "Page cache is overflow");

        if (null == page.getPager()) {
            /*
             * The pager cache has created a new page. Its content needs to be
             * initialized.
             */
            page.setPager(this);

            int nMax;
            try {
                nMax = getPageCount();
            } catch (SqlJetException e) {
                page.unref();
                throw e;
            }

            if (nMax < pageNumber || !read) {
                if (pageNumber > mxPgno) {
                    page.unref();
                    throw new SqlJetException(SqlJetErrorCode.FULL);
                }

                page.getData().fill(pageSize, (byte) 0);
                if (!read) {
                    page.getFlags().add(SqlJetPageFlags.NEED_READ);
                }
                PAGERTRACE("ZERO %s %d\n", pagerId(), Integer.valueOf(pageNumber));

            } else {
                try {
                    readDbPage(page, pageNumber);
                } catch (SqlJetIOException e) {
                    if (SqlJetIOErrorCode.IOERR_SHORT_READ != e.getIoErrorCode()) {
                        dropPage(page);
                        throw e;
                    }
                }
            }
        } else {
            /* The requested page is in the page cache. */
            assert pageCache.getRefCount() > 0 || 1 == pageNumber;
            if (read) {
                try {
                    getContent(page);
                } catch (SqlJetException e) {
                    page.unref();
                    throw e;
                }
            }
        }

        return page;
    }

    @Override
    public void getContent(final ISqlJetPage page) throws SqlJetException {
        final Set<SqlJetPageFlags> flags = page.getFlags();
        if (flags.contains(SqlJetPageFlags.NEED_READ)) {
            readDbPage(page, page.getPageNumber());
            flags.remove(SqlJetPageFlags.NEED_READ);
        }
    }

    /**
     * @param page
     * @throws SqlJetException
     */
    private void dropPage(final ISqlJetPage page) throws SqlJetException {
        pageCache.drop(page);
        unlockIfUnused();
    }

    @Override
    public void unlockIfUnused() throws SqlJetException {
        if (pageCache.getRefCount() == 0 && journalOff > 0) {
            unlockAndRollback();
        }
    }

    /**
     * Execute a rollback if a transaction is active and unlock the database
     * file. If the pager has already entered the error state, do not attempt
     * the rollback.
     *
     * @throws SqlJetException
     */
    private void unlockAndRollback() throws SqlJetException {
        if (errCode == null) {
            rollback();
        }
    }

    /**
     * Read the content of page pPg out of the database file.
     *
     * @param page
     * @param pageNumber
     * @throws SqlJetIOException
     */
    private void readDbPage(final ISqlJetPage page, int pageNumber) throws SqlJetException {
        if (null == fd) {
            throw new SqlJetIOException(SqlJetIOErrorCode.IOERR_SHORT_READ);
        }
        final long offset = (long) (pageNumber - 1) * pageSize;
        final ISqlJetMemoryPointer data = page.getData();
        fd.read(data, pageSize, offset);
        if (1 == pageNumber) {
            dbFileVers.copyFrom(0, data, 24, dbFileVers.remaining());
        }
        PAGERTRACE("FETCH %s page %d\n", pagerId(), Integer.valueOf(page.getPageNumber()));
    }

    /**
     * This function is called to obtain the shared lock required before data
     * may be read from the pager cache. If the shared lock has already been
     * obtained, this function is a no-op.
     *
     * Immediately after obtaining the shared lock (if required), this function
     * checks for a hot-journal file. If one is found, an emergency rollback is
     * performed immediately.
     *
     * @throws SqlJetException
     */
    private void sharedLock() throws SqlJetException {
        /*
         * If the pager is still in an error state, do not proceed. The error
         * state will be cleared at some point in the future when all page
         * references are dropped and the cache can be discarded.
         */
        if (null != errCode && errCode != SqlJetErrorCode.FULL) {
            throw new SqlJetException(errCode);
        }
    }

    /**
     *
     * Playback the journal and thus restore the database file to the state it
     * was in before we started making changes.
     *
     * The journal file format is as follows:
     *
     * (1) 8 byte prefix. A copy of aJournalMagic[]. (2) 4 byte big-endian
     * integer which is the number of valid page records in the journal. If this
     * value is 0xffffffff, then compute the number of page records from the
     * journal size. (3) 4 byte big-endian integer which is the initial value
     * for the sanity checksum. (4) 4 byte integer which is the number of pages
     * to truncate the database to during a rollback. (5) 4 byte big-endian
     * integer which is the sector size. The header is this many bytes in size.
     * (6) 4 byte big-endian integer which is the page case. (7) 4 byte integer
     * which is the number of bytes in the master journal name. The value may be
     * zero (indicate that there is no master journal.) (8) N bytes of the
     * master journal name. The name will be nul-terminated and might be shorter
     * than the value read from (5). If the first byte of the name is \000 then
     * there is no master journal. The master journal name is stored in UTF-8.
     * (9) Zero or more pages instances, each as follows: + 4 byte page number.
     * + pPager->pageSize bytes of data. + 4 byte checksum
     *
     * When we speak of the journal header, we mean the first 8 items above.
     * Each entry in the journal is an instance of the 9th item.
     *
     * Call the value from the second bullet "nRec". nRec is the number of valid
     * page entries in the journal. In most cases, you can compute the value of
     * nRec from the size of the journal file. But if a power failure occurred
     * while the journal was being written, it could be the case that the size
     * of the journal file had already been increased but the extra entries had
     * not yet made it safely to disk. In such a case, the value of nRec
     * computed from the file size would be too large. For that reason, we
     * always use the nRec value in the header.
     *
     * If the nRec value is 0xffffffff it means that nRec should be computed
     * from the file size. This value is used when the user selects the no-sync
     * option for the journal. A power failure could lead to corruption in this
     * case. But for things like temporary table (which will be deleted when the
     * power is restored) we don't care.
     *
     * If the file opened as the journal file is not a well-formed journal file
     * then all pages up to the first corrupted page are rolled back (or no
     * pages if the journal header is corrupted). The journal file is then
     * deleted and SQLITE_OK returned, just as if no corruption had been
     * encountered.
     *
     * If an I/O or malloc() error occurs, the journal-file is not deleted and
     * an error code is returned.
     *
     * @param isHot
     * @throws SqlJetException
     */
    private void playback(boolean isHot) throws SqlJetException {
        try {
            int nRec = -1; /* Number of Records in the journal */
            int mxPg = 0; /* Size of the original file in pages */
            boolean res = true; /* Value returned by sqlite3OsAccess() */

            /*
             * Figure out how many records are in the journal. Abort early if
             * the journal is empty.
             */
            assert journalOpen;

            try {
                long szJ = jfd
                        .fileSize(); /* Size of the journal file in bytes */
                if (szJ == 0) {
                    return;
                }

                /*
                 * Read the master journal name from the journal, if it is
                 * present. If a master journal file name is specified, but the
                 * file is not present on disk, then the journal is not hot and
                 * does not need to be played back.
                 */
                String zMaster = readMasterJournal(jfd);
                if (null != zMaster) {
                    res = fileSystem.access(new File(zMaster), SqlJetFileAccesPermission.EXISTS);
                }
                if (!res) {
                    return;
                }
                journalOff = 0;

                /*
                 * This loop terminates either when the readJournalHdr() call
                 * returns SQLITE_DONE or an IO error occurs.
                 */
                while (true) {

                    /*
                     * Read the next journal header from the journal file. If
                     * there are not enough bytes left in the journal file for a
                     * complete header, or it is corrupted, then a process must
                     * of failed while writing it. This indicates nothing more
                     * needs to be rolled back.
                     */
                    try {
                        final int[] readJournalHdr = readJournalHdr(szJ);
                        nRec = readJournalHdr[0];
                        mxPg = readJournalHdr[1];
                    } catch (SqlJetException e) {
                        if (SqlJetErrorCode.DONE == e.getErrorCode()) {
                            return;
                        }
                    }

                    /*
                     * If nRec is 0xffffffff, then this journal was created by a
                     * process working in no-sync mode. This means that the rest
                     * of the journal file consists of pages, there are no more
                     * journal headers. Compute the value of nRec based on this
                     * assumption.
                     */
                    if (nRec == 0xffffffff) {
                        assert journalOff == sectorSize;
                        nRec = (int) ((szJ - sectorSize) / JOURNAL_PG_SZ());
                    }

                    /*
                     * If nRec is 0 and this rollback is of a transaction
                     * created by this process and if this is the final header
                     * in the journal, then it means that this part of the
                     * journal was being filled but has not yet been synced to
                     * disk. Compute the number of pages based on the remaining
                     * size of the file.
                     *
                     * The third term of the test was added to fix ticket #2565.
                     * When rolling back a hot journal, nRec==0 always means
                     * that the next chunk of the journal contains zero pages to
                     * be rolled back. But when doing a ROLLBACK and the nRec==0
                     * chunk is the last chunk in the journal, it means that the
                     * journal might contain additional pages that need to be
                     * rolled back and that the number of pages should be
                     * computed based on the journal file size.
                     */
                    if (nRec == 0 && !isHot && journalHdr + sectorSize == journalOff) {
                        nRec = (int) ((szJ - journalOff) / JOURNAL_PG_SZ());
                    }

                    /*
                     * If this is the first header read from the journal,
                     * truncate the database file back to its original size.
                     */
                    if (journalOff == sectorSize) {
                        doTruncate(mxPg);
                        dbSize = mxPg;
                    }

                    if (isHot) {
                        reset();
                    }

                    /*
                     * Copy original pages out of the journal and back into the
                     * database file.
                     */
                    for (int u = 0; u < nRec; u++) {
                        try {
                            journalOff = playbackOnePage(journalOff);
                        } catch (SqlJetException e) {
                            if (e.getErrorCode() == SqlJetErrorCode.DONE) {
                                journalOff = szJ;
                                break;
                            } else {
                                /*
                                 * If we are unable to rollback, then the
                                 * database is probably going to end up being
                                 * corrupt. It is corrupt to us, anyhow. Perhaps
                                 * the next process to come along can fix it....
                                 */
                                throw new SqlJetException(SqlJetErrorCode.CORRUPT);
                            }
                        }

                    }
                }
            } finally {
                // end_playback:
                String zMaster = readMasterJournal(jfd);
                endTransaction(zMaster != null);

                if (zMaster != null && res) {
                    /*
                     * If there was a master journal and this routine will
                     * return success, see if it is possible to delete the
                     * master journal.
                     */
                    deleteMaster(zMaster);
                }
            }
        } finally {
            /*
             * The Pager.sectorSize variable may have been updated while rolling
             * back a journal created by a process with a different sector size
             * value. Reset it to the correct value for this process.
             */
            setSectorSize();
        }
    }

    /**
     * Parameter zMaster is the name of a master journal file. A single journal
     * file that referred to the master journal file has just been rolled back.
     * This routine checks if it is possible to delete the master journal file,
     * and does so if it is.
     *
     * Argument zMaster may point to Pager.pTmpSpace. So that buffer is not
     * available for use within this function.
     *
     *
     * The master journal file contains the names of all child journals. To tell
     * if a master journal can be deleted, check to each of the children. If all
     * children are either missing or do not refer to a different master
     * journal, then this master journal can be deleted.
     *
     * @param master
     * @throws SqlJetException
     */
    private void deleteMaster(@Nonnull String master) throws SqlJetException {
        /*
         * Open the master journal file exclusively in case some other process
         * is running this routine also. Not that it makes too much difference.
         */
        File masterFile = new File(master);
        try (ISqlJetFile pMaster = fileSystem.open(masterFile, SqlJetFileType.MASTER_JOURNAL,
                EnumSet.of(SqlJetFileOpenPermission.READONLY))) {
            /* Size of master journal file */
            int nMasterJournal = (int) pMaster.fileSize();

            if (nMasterJournal > 0) {

                /*
                 * Load the entire master journal file into space obtained from
                 * sqlite3_malloc() and pointed to by zMasterJournal.
                 */
                /* Contents of master journal file */
                ISqlJetMemoryPointer zMasterJournal = SqlJetUtility.memoryManager.allocatePtr(nMasterJournal);
                pMaster.read(zMasterJournal, nMasterJournal, 0);

                int nMasterPtr = 0;
                while (nMasterPtr < nMasterJournal) {

                    int zMasterPtr = SqlJetUtility.strlen(zMasterJournal, nMasterPtr);
                    String zJournal = SqlJetUtility.toString(zMasterJournal.pointer(nMasterPtr));
                    final File journalPath = new File(zJournal);
                    boolean exists = fileSystem.access(journalPath, SqlJetFileAccesPermission.EXISTS);

                    if (exists) {
                        /*
                         * One of the journals pointed to by the master journal
                         * exists. Open it and check if it points at the master
                         * journal. If so, return without deleting the master
                         * journal file.
                         */
                        try (ISqlJetFile pJournal = fileSystem.open(journalPath, SqlJetFileType.MAIN_JOURNAL,
                                SqlJetUtility.of(SqlJetFileOpenPermission.READONLY))) {
                            final String readJournal = readMasterJournal(pJournal);
                            if (readJournal != null && readJournal.equals(master)) {
                                /*
                                 * We have a match. Do not delete the master
                                 * journal file.
                                 */
                                return;
                            }
                        }
                    }
                    nMasterPtr += zMasterPtr + 1;
                }
            }

            fileSystem.delete(masterFile, false);
        }
    }

    /**
     * This routine ends a transaction. A transaction is ended by either a
     * COMMIT or a ROLLBACK.
     *
     * When this routine is called, the pager has the journal file open and a
     * RESERVED or EXCLUSIVE lock on the database. This routine will release the
     * database lock and acquires a SHARED lock in its place if that is the
     * appropriate thing to do. Release locks usually is appropriate, unless we
     * are in exclusive access mode or unless this is a COMMIT AND BEGIN or
     * ROLLBACK AND BEGIN operation.
     *
     * The journal file is either deleted or truncated.
     *
     * TODO: Consider keeping the journal file open for temporary databases.
     * This might give a performance improvement on windows where opening a file
     * is an expensive operation.
     *
     * @param hasMaster
     * @throws SqlJetException
     */
    private void endTransaction(boolean hasMaster) throws SqlJetException {
        SqlJetException rc = null;

        if (journalOpen) {
            pagesInJournal = null;
            pagesAlwaysRollback.clear();
            // pCache.iterate(setPageHash)
            pageCache.cleanAll();
            dirtyCache = false;
            nRec = 0;

            try {
                zeroJournalHdr(hasMaster);
            } catch (SqlJetException e) {
                rc = e;
                error(e);
            }
            journalOff = 0;
            journalStarted = false;
        } else {
            assert null == pagesInJournal;
        }

        dbOrigSize = 0;
        pageCache.truncate(dbSize);
        dbSizeValid = false;
        dbModified = false;

        synced = false;

        if (rc != null) {
            throw rc;
        }
    }

    /**
     * Write zeros over the header of the journal file. This has the effect of
     * invalidating the journal file and committing the transaction.
     *
     * @param doTruncate
     * @throws SqlJetException
     */
    private void zeroJournalHdr(boolean doTruncate) throws SqlJetException {
        if (journalOff > 0) {
            long iLimit = journalSizeLimit;

            if (doTruncate || iLimit == 0) {
                jfd.truncate(0);
            } else {
                ISqlJetMemoryPointer zeroHdr = SqlJetUtility.memoryManager.allocatePtr(28);
                jfd.write(zeroHdr, zeroHdr.remaining(), 0);
            }

            /*
             * At this point the transaction is committed but the write lock is
             * still held on the file. If there is a size limit configured for
             * the persistent journal and the journal file currently consumes
             * more space than that limit allows for, truncate it now. There is
             * no need to sync the file following this operation.
             */
            if (iLimit > 0) {
                long sz = jfd.fileSize();

                if (sz > iLimit) {
                    jfd.truncate(iLimit);
                }
            }
        }
    }

    /**
     * Read a single page from either the journal file (if isMainJrnl==1) or
     * from the sub-journal (if isMainJrnl==0) and playback that page. The page
     * begins at offset *pOffset into the file. The *pOffset value is increased
     * to the start of the next page in the journal.
     *
     * The isMainJrnl flag is true if this is the main rollback journal and
     * false for the statement journal. The main rollback journal uses checksums
     * - the statement journal does not.
     *
     * If pDone is not NULL, then it is a record of pages that have already been
     * played back. If the page at *pOffset has already been played back (if the
     * corresponding pDone bit is set) then skip the playback. Make sure the
     * pDone bit corresponding to the *pOffset page is set prior to returning.
     *
     * @param isMainJrnl
     *            true -> main journal. false -> sub-journal.
     * @param pOffset
     *            Offset of record to playback
     * @throws SqlJetException
     *
     */
    private long playbackOnePage(long pOffset) throws SqlJetException {
        ISqlJetMemoryPointer aData = getTempSpace(); /*
                                                      * Temporary storage for
                                                      * the page
                                                      */

        ISqlJetFile jfd = this.jfd; /*
                                     * The file descriptor for the journal file
                                     */
        int pgno = read32bits(jfd,
                pOffset); /* The page number of a page in journal */
        jfd.read(aData, pageSize, pOffset + 4);
        pOffset += pageSize + 4 + 4;

        /*
         * Sanity checking on the page. This is more important that I originally
         * thought. If a power failure occurs while the journal is being
         * written, it could cause invalid data to be written into the journal.
         * We need to detect this invalid data (with high probability) and
         * ignore it.
         */
        SqlJetAssert.assertFalse(pgno == 0 || pgno == PAGER_MJ_PGNO(), SqlJetErrorCode.DONE);

        if (pgno > dbSize) {
            return pOffset;
        }
        long cksum = read32bitsUnsigned(jfd,
                pOffset - 4); /* Checksum used for sanity checking */
        SqlJetAssert.assertFalse(cksum(aData) != cksum, SqlJetErrorCode.DONE);

        /*
         * If the pager is in RESERVED state, then there must be a copy of this
         * page in the pager cache. In this case just update the pager cache,
         * not the database file. The page is left marked dirty in this case.
         *
         * An exception to the above rule: If the database is in no-sync mode
         * and a page is moved during an incremental vacuum then the page may
         * not be in the pager cache. Later: if a malloc() or IO error occurs
         * during a Movepage() call, then the page may not be in the cache
         * either. So the condition described in the above paragraph is not
         * assert()able.
         *
         * If in EXCLUSIVE state, then we update the pager cache if it exists
         * and the main file. The page is then marked not dirty.
         *
         * Ticket #1171: The statement journal might contain page content that
         * is different from the page content at the start of the transaction.
         * This occurs when a page is changed prior to the start of a statement
         * then changed again within the statement. When rolling back such a
         * statement we must not write to the original database unless we know
         * for certain that original page contents are synced into the main
         * rollback journal. Otherwise, a power loss might leave modified data
         * in the database file without an entry in the rollback journal that
         * can restore the database to its original form. Two conditions must be
         * met before writing to the database files. (1) the database must be
         * locked. (2) we know that the original page content is fully synced in
         * the main journal either because the page is not in cache or else the
         * page is marked as needSync==0.
         *
         * 2008-04-14: When attempting to vacuum a corrupt database file, it is
         * possible to fail a statement on a database that does not yet exist.
         * Do not attempt to write if database file has never been opened.
         */
        ISqlJetPage pPg = lookup(pgno); /* An existing page in the cache */
        PAGERTRACE("PLAYBACK %s page %d %s\n", pagerId(), Integer.valueOf(pgno), "main-journal");
        if ((pPg == null || !pPg.getFlags().contains(SqlJetPageFlags.NEED_SYNC)) && null != fd) {
            final long ofst = (pgno - 1) * (long) pageSize;
            fd.write(aData, pageSize, ofst);
            if (pgno > dbFileSize) {
                dbFileSize = pgno;
            }
        }
        if (null != pPg) {
            /*
             * No page should ever be explicitly rolled back that is in use,
             * except for page 1 which is held in use in order to keep the lock
             * on the database active. However such a page may be rolled back as
             * a result of an internal error resulting in an automatic call to
             * sqlite3PagerRollback().
             */
            final ISqlJetMemoryPointer pData = pPg.getData();
            pData.copyFrom(aData, pageSize);

            if (null != reiniter) {
                reiniter.pageCallback(pPg);
            }

            /*
             * If the contents of this page were just restored from the main
             * journal file, then its content must be as they were when the
             * transaction was first opened. In this case we can mark the page
             * as clean, since there will be no need to write it out to the.
             *
             * There is one exception to this rule. If the page is being rolled
             * back as part of a savepoint (or statement) rollback from an
             * unsynced portion of the main journal file, then it is not safe to
             * mark the page as clean. This is because marking the page as clean
             * will clear the PGHDR_NEED_SYNC flag. Since the page is already in
             * the journal file (recorded in Pager.pInJournal) and the
             * PGHDR_NEED_SYNC flag is cleared, if the page is written to again
             * within this transaction, it will be marked as dirty but the
             * PGHDR_NEED_SYNC flag will not be set. It could then potentially
             * be written out into the database file before its journal file
             * segment is synced. If a crash occurs during or following this,
             * database corruption may ensue.
             */
            pPg.makeClean();

            /*
             * If this was page 1, then restore the value of Pager.dbFileVers.
             * Do this before any decoding.
             */
            if (pgno == 1) {
                dbFileVers.copyFrom(0, pData, 24, dbFileVers.remaining());
            }

            pPg.release();
        }
        return pOffset;
    }

    /**
     * Compute and return a checksum for the page of data.
     *
     * This is not a real checksum. It is really just the sum of the random
     * initial value and the page number. We experimented with a checksum of the
     * entire data, but that was found to be too slow.
     *
     * Note that the page number is stored at the beginning of data and the
     * checksum is stored at the end. This is important. If journal corruption
     * occurs due to a power failure, the most likely scenario is that one end
     * or the other of the record will be changed. It is much less likely that
     * the two ends of the journal record will be correct and the middle be
     * corrupt. Thus, this "checksum" scheme, though fast and simple, catches
     * the mostly likely kind of corruption.
     *
     * FIX ME: Consider adding every 200th (or so) byte of the data to the
     * checksum. That way if a single page spans 3 or more disk sectors and only
     * the middle sector is corrupt, we will still have a reasonable chance of
     * failing the checksum and thus detecting the problem.
     *
     * @param data
     * @return
     */
    long cksum(ISqlJetMemoryPointer data) {
        long cksum = cksumInit;
        int i = pageSize - 200;
        while (i > 0) {
            cksum += data.getByteUnsigned(i);
            i -= 200;
        }
        return cksum;
    }

    /**
     * When this is called the journal file for pager pPager must be open. The
     * master journal file name is read from the end of the file and written
     * into memory supplied by the caller.
     *
     * zMaster must point to a buffer of at least nMaster bytes allocated by the
     * caller. This should be sqlite3_vfs.mxPathname+1 (to ensure there is
     * enough space to write the master journal name). If the master journal
     * name in the journal is longer than nMaster bytes (including a
     * nul-terminator), then this is handled as if no master journal name were
     * present in the journal.
     *
     * If no master journal file name is present zMaster[0] is set to 0 and
     * SQLITE_OK returned.
     *
     * @throws SqlJetException
     *
     */
    private String readMasterJournal(final ISqlJetFile journal) throws SqlJetException {
        long szJ = journal.fileSize();
        if (szJ < 16) {
            return null;
        }

        /* A buffer to hold the magic header */
        ISqlJetMemoryPointer aMagic = SqlJetUtility.memoryManager.allocatePtr(8);

        int len = read32bits(journal, szJ - 16);
        long cksum = read32bitsUnsigned(journal, szJ - 12);

        journal.read(aMagic, aMagic.remaining(), szJ - 8);
        if (0 != SqlJetUtility.memcmp(aMagic, aJournalMagic, aMagic.remaining())) {
            return null;
        }

        ISqlJetMemoryPointer zMaster = SqlJetUtility.memoryManager.allocatePtr(len);
        journal.read(zMaster, len, szJ - 16 - len);

        /* See if the checksum matches the master journal name */
        for (int u = 0; u < len; u++) {
            cksum -= zMaster.getByteUnsigned(u);
        }
        if (cksum > 0) {
            /*
             * If the checksum doesn't add up, then one or more of the disk
             * sectors containing the master journal filename is corrupted. This
             * means definitely roll back, so just return SQLITE_OK and report a
             * (nul) master-journal filename.
             */
            return null;
        }

        return SqlJetUtility.toString(zMaster);
    }

    private static int read32bits(final ISqlJetFile fd, final long offset) throws SqlJetException {
        ISqlJetMemoryPointer ac = SqlJetUtility.memoryManager.allocatePtr(4);
        fd.read(ac, ac.remaining(), offset);
        return ac.getInt();
    }

    private static long read32bitsUnsigned(final ISqlJetFile fd, final long offset) throws SqlJetException {
        ISqlJetMemoryPointer ac = SqlJetUtility.memoryManager.allocatePtr(4);
        fd.read(ac, ac.remaining(), offset);
        return ac.getIntUnsigned();
    }

    /**
     * The journal file must be open when this is called. A journal header file
     * (JOURNAL_HDR_SZ bytes) is read from the current location in the journal
     * file. The current location in the journal file is given by
     * pPager->journalOff. See comments above function writeJournalHdr() for a
     * description of the journal header format.
     *
     * If the header is read successfully,nRec is set to the number of page
     * records following this header and dbSize is set to the size of the
     * database before the transaction began, in pages. Also, pPager->cksumInit
     * is set to the value read from the journal header. SQLITE_OK is returned
     * in this case.
     *
     * If the journal header file appears to be corrupted, SQLITE_DONE is
     * returned and nRec and dbSize are undefined. If JOURNAL_HDR_SZ bytes
     * cannot be read from the journal file an error code is returned.
     */
    private int[] readJournalHdr(long journalSize) throws SqlJetException {

        int[] result = new int[2];

        /* A buffer to hold the magic header */
        ISqlJetMemoryPointer aMagic = SqlJetUtility.memoryManager.allocatePtr(8);
        long jrnlOff;
        int iPageSize;
        int iSectorSize;

        seekJournalHdr();
        if (journalOff + sectorSize > journalSize) {
            throw new SqlJetException(SqlJetErrorCode.DONE);
        }
        jrnlOff = journalOff;

        jfd.read(aMagic, aMagic.remaining(), jrnlOff);
        jrnlOff += aMagic.remaining();

        if (0 != SqlJetUtility.memcmp(aMagic, aJournalMagic, aMagic.remaining())) {
            throw new SqlJetException(SqlJetErrorCode.DONE);
        }

        int pNRec = read32bits(jfd, jrnlOff);
        cksumInit = read32bitsUnsigned(jfd, jrnlOff + 4);
        int pDbSize = read32bits(jfd, jrnlOff + 8);

        result[0] = pNRec;
        result[1] = pDbSize;

        if (journalOff == 0) {
            iPageSize = read32bits(jfd, jrnlOff + 16);

            if (iPageSize < SQLJET_MIN_PAGE_SIZE || iPageSize > SQLJET_MAX_PAGE_SIZE
                    || (iPageSize - 1 & iPageSize) != 0) {
                /*
                 * If the page-size in the journal-header is invalid, then the
                 * process that wrote the journal-header must have crashed
                 * before the header was synced. In this case stop reading the
                 * journal file here.
                 */
                throw new SqlJetException(SqlJetErrorCode.DONE);
            } else {
                setPageSize(iPageSize);
                assert pageSize == iPageSize;
            }

            /*
             * Update the assumed sector-size to match the value used by the
             * process that created this journal. If this journal was created by
             * a process other than this one, then this routine is being called
             * from within pager_playback(). The local value of Pager.sectorSize
             * is restored at the end of that routine.
             */

            iSectorSize = read32bits(jfd, jrnlOff + 12);

            if ((iSectorSize & iSectorSize - 1) != 0 || iSectorSize < SQLJET_MIN_PAGE_SIZE
                    || iSectorSize > SQLJET_MAX_PAGE_SIZE) {
                throw new SqlJetException(SqlJetErrorCode.DONE);
            }
            sectorSize = iSectorSize;
        }

        journalOff += sectorSize;

        return result;
    }

    /**
     * Seek the journal file descriptor to the next sector boundary where a
     * journal header may be read or written. Pager.journalOff is updated with
     * the new seek offset.
     *
     * i.e for a sector size of 512:
     *
     * Input Offset Output Offset --------------------------------------- 0 0
     * 512 512 100 512 2000 2048
     *
     *
     */
    private long journalHdrOffset() {
        long offset = 0;
        long c = journalOff;
        if (c > 0) {
            offset = ((c - 1) / sectorSize + 1) * sectorSize;
        }
        assert offset % sectorSize == 0;
        assert offset >= c;
        assert offset - c < sectorSize;
        return offset;
    }

    private void seekJournalHdr() {
        journalOff = journalHdrOffset();
    }

    @Override
    public ISqlJetPage getPage(int pageNumber) throws SqlJetException {
        return acquirePage(pageNumber, true);
    }

    @Override
    public ISqlJetPage lookupPage(int pageNumber) throws SqlJetException {
        assert pageNumber != 0;
        if (errCode == null || errCode == SqlJetErrorCode.FULL) {
            return pageCache.fetch(pageNumber, false);
        }
        return null;
    }

    @Override
    public void truncateImage(int pagesNumber) {
        assert dbSizeValid;
        assert dbSize >= pagesNumber;
        dbSize = pagesNumber;
    }

    /**
     * Truncate the main file of the given pager to the number of pages
     * indicated. Also truncate the cached representation of the file.
     *
     * It might might be the case that the file on disk is smaller than nPage.
     * This can happen, for example, if we are in the middle of a transaction
     * which has extended the file size and the new pages are still all held in
     * cache, then an INSERT or UPDATE does a statement rollback. Some operating
     * system implementations can get confused if you try to truncate a file to
     * some size that is larger than it currently is, so detect this case and
     * write a single zero byte to the end of the new file instead.
     *
     * @param page
     * @throws SqlJetException
     */
    private void doTruncate(int pageNumber) throws SqlJetException {
        if (null != fd) {
            final long currentSize, newSize;
            currentSize = fd.fileSize();
            newSize = (long) pageSize * pageNumber;
            if (currentSize != newSize) {
                if (currentSize > newSize) {
                    fd.truncate(newSize);
                } else {
                    final ISqlJetMemoryPointer b = SqlJetUtility.memoryManager.allocatePtr(1);
                    fd.write(b, 1, newSize - 1);
                }
                dbFileSize = 0;
            }
        }
    }

    /**
     * Write a 32-bit integer into the given file descriptor. Return SQLITE_OK
     * on success or an error code is something goes wrong.
     *
     * @throws SqlJetIOException
     */
    private void write32bits(long offset, int val) throws SqlJetException {
        final ISqlJetMemoryPointer b = SqlJetUtility.put4byte(val);
        jfd.write(b, b.remaining(), offset);
    }

    private void write32bitsUnsigned(long offset, long val) throws SqlJetException {
        final ISqlJetMemoryPointer b = SqlJetUtility.put4byteUnsigned(val);
        jfd.write(b, b.remaining(), offset);
    }

    @Override
    public boolean writeData(@Nonnull ISqlJetMemoryPointer pData, int pgno) throws SqlJetException {
        /*
         * We should never write to the journal file the page that contains the
         * database locks. The following assert verifies that we do not.
         */
        assert pgno != PAGER_MJ_PGNO();

        try {
            long cksum = cksum(pData);
            write32bits(journalOff, pgno);
            try {
                jfd.write(pData, pageSize, journalOff + 4);
            } finally {
                journalOff += pageSize + 4;
            }
            try {
                write32bitsUnsigned(journalOff, cksum);
            } finally {
                journalOff += 4;
            }
        } finally {
            /*
             * Even if an IO or diskfull error occurred while journalling the
             * page in the block above, set the need-sync flag for the page.
             * Otherwise, when the transaction is rolled back, the logic in
             * playback_one_page() will think that the page needs to be restored
             * in the database file. And if an IO error occurs while doing so,
             * then corruption may follow.
             */

            /*
             * An error has occured writing to the journal file. The transaction
             * will be rolled back by the layer above.
             */
        }

        nRec++;
        addPageToJournal(pgno);

        return false;
    }

    @Override
    public void begin(boolean exclusive) throws SqlJetException {
        if (journalOpen && journalOff == 0) {
            /*
             * This happens when the pager was in exclusive-access mode the last
             * time a (read or write) transaction was successfully concluded by
             * this connection. Instead of deleting the journal file it was kept
             * open and either was truncated to 0 bytes or its header was
             * overwritten with zeros.
             */
            assert pagesInJournal == null;
            assert nRec == 0;
            assert dbOrigSize == 0;
            getPageCount();
            pagesInJournal = new BitSet(dbSize);
            dbOrigSize = dbSize;
            writeJournalHdr();
        }
        assert !journalOpen || journalOff > 0;
    }

    /**
     * The journal file must be open when this routine is called. A journal
     * header (JOURNAL_HDR_SZ bytes) is written into the journal file at the
     * current location.
     *
     * The format for the journal header is as follows: - 8 bytes: Magic
     * identifying journal format. - 4 bytes: Number of records in journal, or
     * -1 no-sync mode is on. - 4 bytes: Random number used for page hash. - 4
     * bytes: Initial database page count. - 4 bytes: Sector size used by the
     * process that wrote this journal. - 4 bytes: Database page size.
     *
     * Followed by (JOURNAL_HDR_SZ - 28) bytes of unused space.
     *
     * @throws SqlJetException
     *
     */
    private void writeJournalHdr() throws SqlJetException {
        ISqlJetMemoryPointer zHeader = getTempSpace();
        int nHeader = Integer.min(pageSize, sectorSize);

        seekJournalHdr();
        journalHdr = journalOff;

        zHeader.copyFrom(aJournalMagic, aJournalMagic.remaining());

        /*
         * Write the nRec Field - the number of page records that follow this
         * journal header. Normally, zero is written to this value at this time.
         * After the records are added to the journal (and the journal synced,
         * if in full-sync mode), the zero is overwritten with the true number
         * of records (see syncJournal()).
         *
         * A faster alternative is to write 0xFFFFFFFF to the nRec field. When
         * reading the journal this value tells SQLite to assume that the rest
         * of the journal file contains valid page records. This assumption is
         * dangerous, as if a failure occured whilst writing to the journal file
         * it may contain some garbage data. There are two scenarios where this
         * risk can be ignored:
         *
         * When the pager is in no-sync mode. Corruption can follow a power
         * failure in this case anyway.
         *
         * When the SQLITE_IOCAP_SAFE_APPEND flag is set. This guarantees that
         * garbage data is never appended to the journal file.
         */
        zHeader.putIntUnsigned(aJournalMagic.remaining(), 0xffffffff);

        /* The random check-hash initialiser */
        cksumInit = randomnessInt();
        zHeader.putIntUnsigned(aJournalMagic.remaining() + 4, cksumInit);

        /* The initial database size */
        zHeader.putIntUnsigned(aJournalMagic.remaining() + 8, dbOrigSize);

        /* The assumed sector size for this process */
        zHeader.putIntUnsigned(aJournalMagic.remaining() + 12, sectorSize);

        /*
         * Initializing the tail of the buffer is not necessary. Everything
         * works find if the following memset() is omitted. But initializing the
         * memory prevents valgrind from complaining, so we are willing to take
         * the performance hit.
         */
        zHeader.fill(aJournalMagic.remaining() + 16, nHeader - (aJournalMagic.remaining() + 16), (byte) 0);

        if (journalHdr == 0) {
            /* The page size */
            zHeader.putIntUnsigned(aJournalMagic.remaining() + 16, pageSize);
        }

        for (int nWrite = 0; nWrite < sectorSize; nWrite += nHeader) {
            jfd.write(zHeader, nHeader, journalOff);
            journalOff += nHeader;
        }
    }

    @Override
    public void openJournal() throws SqlJetException {
        boolean success = false;

        assert pagesInJournal == null;

        getPageCount();
        pagesInJournal = new BitSet(dbSize);

        try {
            if (!journalOpen) {
                Set<SqlJetFileOpenPermission> flags = EnumSet.of(SqlJetFileOpenPermission.EXCLUSIVE,
                        SqlJetFileOpenPermission.CREATE, SqlJetFileOpenPermission.DELETEONCLOSE);
                jfd = fileSystem.open(null, SqlJetFileType.TEMP_JOURNAL, flags);
                journalOff = 0;
                journalHdr = 0;
            }
            journalOpen = true;
            journalStarted = false;
            nRec = 0;
            assertNoError(errCode);
            dbOrigSize = dbSize;

            writeJournalHdr();

            success = true;
        } finally {
            // failed_to_open_journal:
            if (!success) {
                endTransaction(false);
                pagesInJournal = null;
            }
        }
    }

    @Override
    public void commitPhaseOne(boolean noSync) throws SqlJetException {
        assertNoError(errCode);

        /*
         * If no changes have been made, we can leave the transaction early.
         */
        if (!dbModified) {
            assert !dirtyCache || !journalOpen;
            return;
        }

        PAGERTRACE("DATABASE SYNC: File=%s nSize=%d\n", null, Integer.valueOf(dbSize));

        /*
         * If no pages have been written to, or this function has already been
         * called, it is a no-op.
         */
        if (!synced && dirtyCache) {
            /*
             * If a master journal file name has already been written to the
             * journal file, then no sync is required. This happens when it is
             * written, then the process fails to upgrade from a RESERVED to an
             * EXCLUSIVE lock. The next time the process tries to commit the
             * transaction the m-j name will have already been written.
             */
            incrChangeCounter();

            if (dbSize < dbOrigSize) {
                /*
                 * If this transaction has made the database smaller, then all
                 * pages being discarded by the truncation must be written to
                 * the journal file.
                 */
                int i;
                long iSkip = PAGER_MJ_PGNO();
                int dbSize = this.dbSize;
                this.dbSize = this.dbOrigSize;
                for (i = dbSize + 1; i <= this.dbOrigSize; i++) {
                    if (!SqlJetUtility.bitSetTest(pagesInJournal, i) && i != iSkip) {
                        final ISqlJetPage pg = getPage(i);
                        pg.write();
                        pg.unref();
                    }
                }
                this.dbSize = dbSize;
            }

            /* Write all dirty pages to the database file */
            writePageList(pageCache.getDirtyList());
            /*
             * The error might have left the dirty list all fouled up here, but
             * that does not matter because if the if the dirty list did get
             * corrupted, then the transaction will roll back and discard the
             * dirty list. There is an assert in pager_get_all_dirty_pages()
             * that verifies that no attempt is made to use an invalid dirty
             * list.
             */
            pageCache.cleanAll();

            if (dbSize != dbFileSize) {
                doTruncate(dbSize - (dbSize == PAGER_MJ_PGNO() ? 1 : 0));
            }

            synced = true;
        }
    }

    /**
     * Given a list of pages (connected by the PgHdr.pDirty pointer) write every
     * one of those pages out to the database file. No calls are made to the
     * page-cache to mark the pages as clean. It is the responsibility of the
     * caller to use PcacheCleanAll() or PcacheMakeClean() to mark the pages as
     * clean.
     *
     * @param pList
     * @throws SqlJetException
     */
    private void writePageList(List<ISqlJetPage> pList) throws SqlJetException {
        if (pList.isEmpty()) {
            return;
        }

        for (ISqlJetPage page : pList) {
            /*
             * If there are dirty pages in the page cache with page numbers
             * greater than Pager.dbSize, this means sqlite3PagerTruncate() was
             * called to make the file smaller (presumably by auto-vacuum code).
             * Do not write any such pages to the file.
             */
            if (page.getPageNumber() <= dbSize && !page.getFlags().contains(SqlJetPageFlags.DONT_WRITE)) {

                final long offset = (long) (page.getPageNumber() - 1) * pageSize;
                PAGERTRACE("STORE %s page %d\n", pagerId(), Integer.valueOf(page.getPageNumber()));

                ISqlJetMemoryPointer pData = page.getData();

                fd.write(pData, pageSize, offset);
                if (page.getPageNumber() == 1) {
                    dbFileVers.copyFrom(0, pData, 24, dbFileVers.remaining());
                }
                if (page.getPageNumber() > dbFileSize) {
                    dbFileSize = page.getPageNumber();
                }
            } else {
                PAGERTRACE("NOSTORE %s page %d\n", pagerId(), Integer.valueOf(page.getPageNumber()));
            }
        }

    }

    /**
     * Open a temporary file.
     *
     * Write the file descriptor into *fd. Return SQLITE_OK on success or some
     * other error code if we fail. The OS will automatically delete the
     * temporary file when it is closed.
     *
     * @param fd2
     * @param type2
     * @param permissions2
     * @throws SqlJetException
     */
    private ISqlJetFile openTemp(@Nonnull SqlJetFileType type) throws SqlJetException {
        Set<SqlJetFileOpenPermission> flags = EnumSet.of(SqlJetFileOpenPermission.CREATE,
                SqlJetFileOpenPermission.EXCLUSIVE, SqlJetFileOpenPermission.DELETEONCLOSE);
        flags.addAll(permissions);
        return fileSystem.open(null, type, flags);
    }

    /**
     * This routine is called to increment the database file change-counter,
     * stored at byte 24 of the pager file.
     *
     * @param b
     * @throws SqlJetException
     */
    private void incrChangeCounter() throws SqlJetException {
        if (!changeCountDone && dbSize > 0) {
            /* Open page 1 of the file for writing. */
            ISqlJetPage page = getPage(1);

            try {
                page.write();

                /*
                 * Increment the value just read and write it back to byte 24.
                 */
                int changeCounter = dbFileVers.getInt() + 1;
                page.getData().putIntUnsigned(24, changeCounter);

                /* Release the page reference. */
            } finally {
                page.unref();
            }
            changeCountDone = true;
        }
    }

    @Override
    public void commitPhaseTwo() throws SqlJetException {
        assertNoError(errCode);
        if (!dbModified) {
            assert !dirtyCache || !journalOpen;
            return;
        }

        PAGERTRACE("COMMIT %s\n", pagerId());

        assert synced || !dirtyCache;
        try {
            endTransaction(false);
        } catch (SqlJetException e) {
            error(e);
        }
    }

    @Override
    public void rollback() throws SqlJetException {
        PAGERTRACE("ROLLBACK %s\n", pagerId());
        if (!dirtyCache || !journalOpen) {
            endTransaction(false);
        } else if (null != errCode && errCode != SqlJetErrorCode.FULL) {
            playback(false);
            SqlJetAssert.assertNoError(errCode);
        } else {
            try {
                playback(false);
            } catch (SqlJetException e) {
                dbSizeValid = false;
                /*
                 * If an error occurs during a ROLLBACK, we can no longer trust
                 * the pager cache. So call pager_error() on the way out to make
                 * any error persistent.
                 */
                error(e);
            }
        }
    }

    @Override
    public int getRefCount() {
        return pageCache.getRefCount();
    }

    @Override
    public void pageCallback(final ISqlJetPage pPg) {
        /*
         * This function is called by the pcache layer when it has reached some
         * soft memory limit. The argument is a pointer to a purgeable Pager
         * object. This function attempts to make a single dirty page that has
         * no outstanding references (if one exists) clean so that it can be
         * recycled by the pcache layer.
         */

        if (doNotSync) {
            return;
        }

        assert pPg.getFlags().contains(SqlJetPageFlags.DIRTY);
        if (errCode == null) {
            try {
                writePageList(Collections.singletonList(pPg));
            } catch (SqlJetException e) {
                error(e);
            }
        }
        PAGERTRACE("STRESS %s page %d\n", pagerId(), Integer.valueOf(pPg.getPageNumber()));
        pPg.makeClean();
    }

    @Override
    public boolean isJournalStarted() {
        return journalStarted;
    }

    @Override
    public boolean isNoSync() {
        return true;
    }

    @Override
    public void requireSync() {
    }

    @Override
    public void pageModified() {
        dirtyCache = true;
        dbModified = true;
    }

    @Override
    public boolean isMemDb() {
        return false;
    }

    @Override
    public void removeFromCache(ISqlJetPage pPgOld) {
        pageCache.drop(pPgOld);
    }

    @Override
    public void assertCanWrite() throws SqlJetException {
        /*
         * Check for errors
         */
        checkErrorCode();
    }

    @Override
    public boolean doWrite(int pg) throws SqlJetException {
        boolean result = false;
        if (pg != PAGER_MJ_PGNO()) {
            ISqlJetPage pPage = getPage(pg);
            pPage.doWrite();
            if (pPage.getFlags().contains(SqlJetPageFlags.NEED_SYNC)) {
                result = true;
            }
            pPage.unref();
        }
        return result;

    }
}
