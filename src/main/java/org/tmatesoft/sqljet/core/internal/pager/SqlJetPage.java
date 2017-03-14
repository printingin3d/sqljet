/**
 * Page.java
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

import java.util.EnumSet;
import java.util.Set;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetPage;
import org.tmatesoft.sqljet.core.internal.ISqlJetPager;
import org.tmatesoft.sqljet.core.internal.SqlJetMemoryBufferType;
import org.tmatesoft.sqljet.core.internal.SqlJetPageFlags;
import org.tmatesoft.sqljet.core.internal.SqlJetPagerJournalMode;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.internal.btree.SqlJetMemPage;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetPage implements ISqlJetPage {
    public static final SqlJetMemoryBufferType BUFFER_TYPE = SqlJetUtility.getEnumSysProp(
            "SqlJetPage.BUFFER_TYPE", SqlJetMemoryBufferType.ARRAY);

    /** Content of this page */
    protected final @Nonnull ISqlJetMemoryPointer pData;

    /** Extra content */
    private SqlJetMemPage pExtra;

    /** Page number for this page */
    private int pgno;

    /** The pager this page is part of */
    private ISqlJetPager pPager;

    private final @Nonnull Set<SqlJetPageFlags> flags = EnumSet.noneOf(SqlJetPageFlags.class);

    /*
     * Elements above are public. All that follows is private to pcache.c and
     * should not be accessed by other modules.
     */

    /** Number of users of this page */
    private int nRef;

    /** Cache that owns this page */
    private final SqlJetPageCache pCache;

    /**
     * 
     */
    protected SqlJetPage(int szPage, int pgno, SqlJetPageCache pCache) {
        this.pData = SqlJetUtility.memoryManager.allocatePtr(szPage, BUFFER_TYPE);
        this.pgno = pgno;
        this.pCache = pCache;
    }

    @Override
	public void dontRollback() {

        assert pPager.isReservedState();

        /*
         * If the journal file is not open, or DontWrite() has been called on
         * this page (DontWrite() sets the Pager.pAlwaysRollback bit), then this
         * function is a no-op.
         */
        if (!pPager.isJournalOpen() || pPager.isAlwaysRollBack(pgno) || pPager.isNewPage(pgno)) {
            return;
        }

        if (pageInJournal() || pPager.isNewPage(pgno)) {
            return;
        }

        /*
         * If SECURE_DELETE is disabled, then there is no way that this routine
         * can be called on a page for which sqlite3PagerDontWrite() has not
         * been previously called during the same transaction. And if
         * DontWrite() has previously been called, the following conditions must
         * be met.
         * 
         * (Later:) Not true. If the database is corrupted by having duplicate
         * pages on the freelist (ex: corrupt9.test) then the following is not
         * necessarily true:
         */

        flags.remove(SqlJetPageFlags.NEED_READ);

        pPager.addPageToJournal(pgno);

        SqlJetPager.PAGERTRACE("DONT_ROLLBACK page %d of %s\n", Integer.valueOf(pgno), pPager.pagerId());
        // IOTRACE(("GARBAGE %p %d\n", pPager, pPg->pgno))

    }

    @Override
	public void dontWrite() {
        if (pPager.isNewPage(pgno)) {
            return;
        }

        pPager.setAlwaysRollBack(pgno);
        if (flags.contains(SqlJetPageFlags.DIRTY)) {
            assert pPager.isLockedState();
            if (pPager.isLastPage(pgno) && pPager.dbHasGrown()) {
                /*
                 * If this pages is the last page in the file and the file has
                 * grown during the current transaction, then do NOT mark the
                 * page as clean. When the database file grows, we must make
                 * sure that the last page gets written at least once so that
                 * the disk file will be the correct size. If you do not write
                 * this page and the size of the file on the disk ends up being
                 * too small, that can lead to database corruption during the
                 * next transaction.
                 */
            } else {
                SqlJetPager.PAGERTRACE("DONT_WRITE page %d of %s\n", Integer.valueOf(pgno), pPager.pagerId());
                // IOTRACE(("CLEAN %p %d\n", pPager, pPg->pgno))
                flags.add(SqlJetPageFlags.DONT_WRITE);
            }
        }
    }

    @Override
	public @Nonnull ISqlJetMemoryPointer getData() {
        return pData;
    }

    @Override
	public SqlJetMemPage getExtra() {
        return pPager != null ? pExtra : null;
    }

    @Override
	public void setExtra(SqlJetMemPage extra) {
        this.pExtra = extra;
    }

    @Override
	public void move(int pageNumber, boolean isCommit) throws SqlJetException {

        int needSyncPgno = 0;

        assert nRef > 0;
        assert pageNumber > 0;

        SqlJetPager.PAGERTRACE("MOVE %s page %d (needSync=%b) moves to %d\n", pPager.pagerId(), Integer.valueOf(pgno), 
        		Boolean.valueOf(flags.contains(SqlJetPageFlags.NEED_SYNC)), Integer.valueOf(pageNumber));

        pPager.getContent(this);

        /*
         * If the journal needs to be sync()ed before page pPg->pgno can be
         * written to, store pPg->pgno in local variable needSyncPgno.
         * 
         * If the isCommit flag is set, there is no need to remember that the
         * journal needs to be sync()ed before database page pPg->pgno can be
         * written to. The caller has already promised not to write to it.
         */
        if (flags.contains(SqlJetPageFlags.NEED_SYNC) && !isCommit) {
            needSyncPgno = pgno;
            assert pageInJournal() || pPager.isNewPage(pgno);
            assert flags.contains(SqlJetPageFlags.DIRTY);
        }

        /*
         * If the cache contains a page with page-number pgno, remove it from
         * its hash chain. Also, if the PgHdr.needSync was set for page pgno
         * before the 'move' operation, it needs to be retained for the page
         * moved there.
         */
        flags.remove(SqlJetPageFlags.NEED_SYNC);
        ISqlJetPage pPgOld = pPager.lookup(pageNumber); /* The page being overwritten. */
        if (pPgOld != null) {
        	assert pPgOld.getRefCount() >= 1;
        	
            if (pPgOld.getFlags().contains(SqlJetPageFlags.NEED_SYNC)) {
				flags.add(SqlJetPageFlags.NEED_SYNC);
			}
            pPager.removeFromCache(pPgOld);
        }

        pCache.pCache.rekey(this, pageNumber);
        if (this.flags.contains(SqlJetPageFlags.DIRTY) && this.flags.contains(SqlJetPageFlags.NEED_SYNC)) {
        	removeFromDirtyList();
        	addToDirtyList();
        }
        
        makeDirty();
        pPager.pageModified();

        if (needSyncPgno != 0) {
            /*
             * If needSyncPgno is non-zero, then the journal file needs to be
             * sync()ed before any data is written to database file page
             * needSyncPgno. Currently, no such page exists in the page-cache
             * and the "is journaled" bitvec flag has been set. This needs to be
             * remedied by loading the page into the pager-cache and setting the
             * PgHdr.needSync flag.
             * 
             * If the attempt to load the page into the page-cache fails, (due
             * to a malloc() or IO failure), clear the bit in the pInJournal[]
             * array. Otherwise, if the page is loaded and written again in this
             * transaction, it may be written to the database file before it is
             * synced into the journal file. This way, it may end up in the
             * journal file twice, but that is not a problem.
             * 
             * The sqlite3PagerGet() call may cause the journal to sync. So make
             * sure the Pager.needSync flag is set too.
             */
            SqlJetPage pPgHdr;
            try {
                pPgHdr = (SqlJetPage) pPager.getPage(needSyncPgno);
            } catch (SqlJetException e) {
                if (!pPager.isNewPage(needSyncPgno)) {
                    pPager.removePageToJournal(needSyncPgno);
                }
                throw e;
            }

            pPager.requireSync();
            assert !pPager.isNoSync() && !pPager.isMemDb();
            pPgHdr.getFlags().add(SqlJetPageFlags.NEED_SYNC);
            pPgHdr.makeDirty();
            pPgHdr.unref();
        }
    }

    @Override
	public void ref() {
        nRef++;
    }

    @Override
	public void unref() throws SqlJetException {
        try {
            release();
        } finally {
            pPager.unlockIfUnused();
        }
    }

    @Override
	public void write() throws SqlJetException {

        int nPagePerSector = pPager.getSectorSizePerPage();

        if (nPagePerSector > 1) {

            int nPage; /* Number of pages starting at pg1 to journal */
            boolean needSync = false;

            /*
             * Set the doNotSync flag to 1. This is because we cannot allow a
             * journal header to be written between the pages journaled by this
             * function.
             */
            assert !pPager.isMemDb();
            pPager.startWrite();

            /*
             * This trick assumes that both the page-size and sector-size are an
             * integer power of 2. It sets variable pg1 to the identifier of the
             * first page of the sector pPg is located on.
             */
            int pg1 = (pgno - 1 & ~(nPagePerSector - 1)) + 1;         /* First page of the sector pPg is located on. */

            int nPageCount = pPager.getPageCount();                        /* Total number of pages in database file */
            if (pgno > nPageCount) {
                nPage = pgno - pg1 + 1;
            } else if (pg1 + nPagePerSector - 1 > nPageCount) {
                nPage = nPageCount + 1 - pg1;
            } else {
                nPage = nPagePerSector;
            }
            assert nPage > 0;
            assert pg1 <= pgno;
            assert pg1 + nPage > pgno;

            for (int ii = 0; ii < nPage; ii++) {
                int pg = pg1 + ii;
                ISqlJetPage pPage;
                if (pg == pgno || !pPager.pageInJournal(pg)) {
                	needSync |= pPager.doWrite(pg);
                } else if ((pPage = pPager.lookup(pg)) != null) {
                    if (pPage.getFlags().contains(SqlJetPageFlags.NEED_SYNC)) {
                        needSync = true;
                    }
                    pPage.unref();
                }
            }

            /*
             * If the PGHDR_NEED_SYNC flag is set for any of the nPage pages
             * starting at pg1, then it needs to be set for all of them. Because
             * writing to any of these nPage pages may damage the others, the
             * journal file must contain sync()ed copies of all of them before
             * any of them can be written out to the database file.
             */
            if (needSync) {
                assert !pPager.isMemDb() && !pPager.isNoSync();
                for (int ii = 0; ii < nPage; ii++) {
                    ISqlJetPage pPage = (SqlJetPage) pPager.lookup(pg1 + ii);
                    if (pPage != null) {
                        pPage.getFlags().add(SqlJetPageFlags.NEED_SYNC);
                        pPage.unref();
                    }
                }
            }

            pPager.endWrite();
        } else {
            doWrite();
        }
    }

    @Override
	public void doWrite() throws SqlJetException {
        /*
         * Check for errors
         */
    	pPager.assertCanWrite();

        /*
         * If this page was previously acquired with noContent==1, that means we
         * didn't really read in the content of the page. This can happen (for
         * example) when the page is being moved to the freelist. But now we are
         * (perhaps) moving the page off of the freelist for reuse and we need
         * to know its original content so that content can be stored in the
         * rollback journal. So do the read at this time.
         */
        pPager.getContent(this);

        /*
         * Mark the page as dirty. If the page has already been written to the
         * journal then we can return right away.
         */
        makeDirty();
        if (pageInJournal()) {
            pPager.pageModified();
        } else {
            /*
             * If we get this far, it means that the page needs to be written to
             * the transaction journal or the ckeckpoint journal or both.
             * 
             * First check to see that the transaction journal exists and create
             * it if it does not.
             */
            assert pPager.isLockedState();
            pPager.begin(false);
            assert pPager.isReservedState();
            if (!pPager.isJournalOpen() && pPager.getJournalMode() != SqlJetPagerJournalMode.OFF) {
                pPager.openJournal();
            }
            pPager.pageModified();

            /*
             * The transaction journal now exists and we have a RESERVED or an
             * EXCLUSIVE lock on the main database file. Write the current page
             * to the transaction journal if it is not there already.
             */
            if (!pageInJournal() && pPager.isJournalOpen()) {
                if (!pPager.isNewPage(pgno)) {
                	pPager.writeData(pData, pgno);
                } else {
                    if (!pPager.isJournalStarted() && !pPager.isNoSync()) {
                        flags.add(SqlJetPageFlags.NEED_SYNC);
                        pPager.requireSync();
                    }
                    SqlJetPager.PAGERTRACE("APPEND %s page %d needSync=%b\n", pPager.pagerId(), Integer.valueOf(pgno), 
                    		Boolean.valueOf(flags.contains(SqlJetPageFlags.NEED_SYNC)));
                }
            }
        }

        /*
         * Update the database size and return.
         */
        pPager.updateDbSize(pgno);
    }

    /**
     ** Return true if the page is already in the journal file.
     */
    private boolean pageInJournal() {
        return pPager.pageInJournal(pgno);
    }

    @Override
	public Set<SqlJetPageFlags> getFlags() {
        return flags;
    }

    @Override
	public ISqlJetPager getPager() {
        return pPager;
    }

	@Override
	public void setPager(SqlJetPager sqlJetPager) {
		this.pPager = sqlJetPager;
	}

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPage#getPageNumber()
     */
    @Override
	public int getPageNumber() {
        return pgno;
    }

	public void setPageNumber(int pgno) {
		this.pgno = pgno;
	}

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPage#getRefCount()
     */
    @Override
	public int getRefCount() {
        return nRef;
    }
    
    @Override
	public boolean isWriteable() {
        return flags.contains( SqlJetPageFlags.DIRTY );
    }

    /*
     * Remove this page from the list of dirty pages.
     */
    @Override
    public void removeFromDirtyList() {
    	pCache.dirtyList.remove(this);
    }

    @Override
    public void unpin() {
        if (pCache.bPurgeable) {
            pCache.pCache.unpin(this, false);
        }
    }

    @Override
	public void makeClean() {
        if (flags.contains(SqlJetPageFlags.DIRTY)) {
            removeFromDirtyList();
            flags.remove(SqlJetPageFlags.DIRTY);
            flags.remove(SqlJetPageFlags.NEED_SYNC);
            if (nRef == 0) {
                unpin();
            }
        }
    }

    /**
     * Add this page to the head of the dirty list.
     */
    private void addToDirtyList() {
        pCache.dirtyList.add(0, this);
    }

    /**
     * Make sure the page is marked as dirty. If it isn't dirty already, make it
     * so.
     * 
     * @param page
     * @throws SqlJetExceptionRemove
     */
	private void makeDirty() {
        this.flags.remove(SqlJetPageFlags.DONT_WRITE);
        assert this.nRef > 0;
        if (!this.flags.contains(SqlJetPageFlags.DIRTY)) {
            this.flags.add(SqlJetPageFlags.DIRTY);
            this.addToDirtyList();
        }
    }
	
    @Override
	public void release() {
        assert this.nRef > 0;
        this.nRef--;
        if (this.nRef == 0) {
            pCache.nRef--;
            if (!this.flags.contains(SqlJetPageFlags.DIRTY)) {
                this.unpin();
            } else {
                /* Move the page to the head of the dirty list. */
                this.removeFromDirtyList();
                this.addToDirtyList();
            }
        }
    }
}
