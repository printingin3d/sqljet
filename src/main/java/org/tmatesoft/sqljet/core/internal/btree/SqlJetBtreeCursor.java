/**
 * SqlJetBtreeCursor.java
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

import static org.tmatesoft.sqljet.core.internal.SqlJetUtility.memcpy;
import static org.tmatesoft.sqljet.core.internal.SqlJetUtility.mutexHeld;
import static org.tmatesoft.sqljet.core.internal.btree.SqlJetBtree.TRACE;
import static org.tmatesoft.sqljet.core.internal.btree.SqlJetBtree.traceInt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtreeCursor;
import org.tmatesoft.sqljet.core.internal.ISqlJetConfig;
import org.tmatesoft.sqljet.core.internal.ISqlJetDbHandle;
import org.tmatesoft.sqljet.core.internal.ISqlJetKeyInfo;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetPage;
import org.tmatesoft.sqljet.core.internal.ISqlJetUnpackedRecord;
import org.tmatesoft.sqljet.core.internal.SqlJetAssert;
import org.tmatesoft.sqljet.core.internal.SqlJetCloneable;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.internal.btree.SqlJetBtree.TransMode;
import org.tmatesoft.sqljet.core.internal.memory.SqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.vdbe.SqlJetUnpackedRecord;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class SqlJetBtreeCursor extends SqlJetCloneable implements ISqlJetBtreeCursor {

    /*
     * The following parameters determine how many adjacent pages get involved
     * in a balancing operation. NN is the number of neighbors on either side*
     * of the page that participate in the balancing operation. NB is the* total
     * number of pages that participate, including the target page and* NN
     * neighbors on either side.** The minimum value of NN is 1 (of course).
     * Increasing NN above 1* (to 2 or 3) gives a modest improvement in SELECT
     * and DELETE performance* in exchange for a larger degradation in INSERT
     * and UPDATE performance.* The value of NN appears to give the best results
     * overall.
     */
    /** Number of neighbors on either side of pPage */
    private static final int NN = 1;
    /** Total pages involved in the balance */
    private static final int NB = (NN * 2 + 1);

    /** The Btree to which this cursor belongs */
    protected SqlJetBtree pBtree;

    /** The BtShared this cursor points to */
    protected final SqlJetBtreeShared pBt;

    /** Argument passed to comparison function */
    private final ISqlJetKeyInfo pKeyInfo;

    /** The root page of this tree */
    protected final int pgnoRoot;

    /** A parse of the cell we are pointing at */
    protected SqlJetBtreeCellInfo info = new SqlJetBtreeCellInfo(null, 0, 0, 0, 0, 0, 0);

    /** True if writable */
    protected final boolean wrFlag;

    /** Cursor pointing to the last entry */
    private boolean atLast;

    /** True if info.nKey is valid */
    private boolean validNKey;

    /** One of the CURSOR_XXX constants (see below) */
    protected SqlJetCursorState eState;

    /** Saved key that was cursor's last known position */
    private ISqlJetMemoryPointer pKey;

    /** Size of pKey, or last integer key */
    private long nKey;

    protected SqlJetErrorCode error;

    /**
     * (skip<0) -> Prev() is a no-op. (skip>0) -> Next() is
     */
    protected int skip;

    /** True if this cursor is an incr. io handle */
    protected boolean isIncrblobHandle;

    /** Cache of overflow page locations */
    protected int[] aOverflow;

    /** Index of current page in apPage */
    private int iPage = -1;
    
    private static class IndexedMemPage {
    	private final SqlJetMemPage page;
    	private int index;
		public IndexedMemPage(SqlJetMemPage page) {
			this.page = page;
			this.index = 0;
		}
		public int getIndex() {
			return index;
		}
		public int incrIndex() {
			return ++index;
		}
		public int decrIndex() {
			return --index;
		}
		public void setIndex(int index) {
			this.index = index;
		}
		public SqlJetMemPage getPage() {
			return page;
		}
    }

    /**
     * Pages from root to current page
     */
    private final IndexedMemPage[] apPage = new IndexedMemPage[BTCURSOR_MAX_DEPTH];

    /** Current index in apPage[i] */
//    private final int[] aiIdx = new int[BTCURSOR_MAX_DEPTH];

    /**
     * Create a new cursor for the BTree whose root is on the page iTable. The
     * act of acquiring a cursor gets a read lock on the database file.
     *
     * If wrFlag==0, then the cursor can only be used for reading. If wrFlag==1,
     * then the cursor can be used for reading or for writing if other
     * conditions for writing are also met. These are the conditions that must
     * be met in order for writing to be allowed:
     *
     * 1: The cursor must have been opened with wrFlag==1
     *
     * 2: Other database connections that share the same pager cache but which
     * are not in the READ_UNCOMMITTED state may not have cursors open with
     * wrFlag==0 on the same table. Otherwise the changes made by this write
     * cursor would be visible to the read cursors in the other database
     * connection.
     *
     * 3: The database must be writable (not on read-only media)
     *
     * 4: There must be an active transaction.
     *
     * No checking is done to make sure that page iTable really is the root page
     * of a b-tree. If it is not, then the cursor acquired will not work
     * correctly.
     *
     * It is assumed that the sqlite3BtreeCursorSize() bytes of memory pointed
     * to by pCur have been zeroed by the caller.
     *
     *
     * @param sqlJetBtree
     * @param table
     * @param wrFlag2
     * @param keyInfo
     *
     * @throws SqlJetException
     */
    public SqlJetBtreeCursor(SqlJetBtree btree, int table, boolean wrFlag, ISqlJetKeyInfo keyInfo) throws SqlJetException {
        SqlJetBtreeShared pBt = btree.pBt;

        assert (btree.holdsMutex());

        if (wrFlag) {
            SqlJetAssert.assertFalse(pBt.readOnly, SqlJetErrorCode.READONLY);
            SqlJetAssert.assertFalse(btree.checkReadLocks(table, null, 0), SqlJetErrorCode.LOCKED);
        }

        if (pBt.pPage1 == null) {
            btree.lockWithRetry();
        }
        this.pgnoRoot = table;
        int nPage = pBt.pPager.getPageCount();
        try {
        	SqlJetAssert.assertFalse(table == 1 && nPage == 0, SqlJetErrorCode.EMPTY);
        	addNewPage(pBt.getAndInitPage(pgnoRoot));
        } catch (SqlJetException e) {
        	// create_cursor_exception:
        	pBt.unlockBtreeIfUnused();
        	throw e;
        }

        /*
         * Now that no other errors can occur, finish filling in the
         * BtCursor* variables, link the cursor into the BtShared list and
         * set *ppCur (the* output argument to this function).
         */
        this.pKeyInfo = keyInfo;
        this.pBtree = btree;
        this.pBt = pBt;
        this.wrFlag = wrFlag;
        pBt.pCursor.add(0, this);
        this.eState = SqlJetCursorState.INVALID;
    }

    /**
     * @return
     */
    private boolean holdsMutex() {
        return pBt.mutex.held();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#clearCursor()
     */
    @Override
	public void clearCursor() {
        assert (holdsMutex());
        pKey = null;
        eState = SqlJetCursorState.INVALID;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#closeCursor()
     */
    @Override
	public void closeCursor() throws SqlJetException {
        if (pBtree != null) {
            pBtree.enter();
            try {
                pBt.db = pBtree.db;
                clearCursor();
                pBt.pCursor.remove(this);
                for (SqlJetMemPage mp : getAllPages()) {
					SqlJetMemPage.releasePage(mp);
                }
                pBt.unlockBtreeIfUnused();
                invalidateOverflowCache();
            } finally {
                SqlJetBtree p = pBtree;
                pBtree = null;
                p.leave();
            }
        }
    }

    /**
     * Invalidate the overflow page-list cache for cursor pCur, if any.
     */
    private void invalidateOverflowCache() {
        assert (holdsMutex());
        aOverflow = null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#moveTo(byte[], long,
     * boolean)
     */
    @Override
	public int moveTo(ISqlJetMemoryPointer pKey, long nKey, boolean bias) throws SqlJetException {
        /* Unpacked index key */
        SqlJetUnpackedRecord pIdxKey;

        if (pKey != null) {
            assert (nKey == (int) nKey);
            pIdxKey = pKeyInfo.recordUnpack((int) nKey, pKey);
        } else {
            pIdxKey = null;
        }

        return moveToUnpacked(pIdxKey, nKey, bias);
    }

    /**
     * Move the cursor to the root page
     */
    private void moveToRoot() throws SqlJetException {
        assert (this.holdsMutex());
        SqlJetAssert.assertFalse(eState == SqlJetCursorState.FAULT, error);
        if (eState == SqlJetCursorState.REQUIRESEEK) {
            this.clearCursor();
        }

        if (this.iPage >= 0) {
            for (int i = 1; i <= this.iPage; i++) {
                SqlJetMemPage.releasePage(this.apPage[i].getPage());
            }
            this.iPage = 0;
        } else {
            try {
            	addNewPage(pBt.getAndInitPage(this.pgnoRoot));
            } catch (SqlJetException e) {
                this.eState = SqlJetCursorState.INVALID;
                throw e;
            }
        }

        SqlJetMemPage pRoot = getCurrentPage();
        assert (pRoot.pgno == this.pgnoRoot);
        setIndexOnCurrentPage(0);
        this.info.nSize = 0;
        this.atLast = false;
        this.validNKey = false;

        if (pRoot.nCell == 0 && !pRoot.leaf) {
            assert (pRoot.pgno == 1);
            int subpage = pRoot.aData.getInt(pRoot.getHdrOffset() + 8);
            assert (subpage > 0);
            this.eState = SqlJetCursorState.VALID;
            moveToChild(subpage);
        } else {
            this.eState = ((pRoot.nCell > 0) ? SqlJetCursorState.VALID : SqlJetCursorState.INVALID);
        }

    }

    /**
     * Move the cursor down to a new child page. The newPgno argument is the
     * page number of the child page to move to.
     */
    private void moveToChild(int newPgno) throws SqlJetException {
        assert (this.holdsMutex());
        assert (this.eState == SqlJetCursorState.VALID);
        SqlJetAssert.assertTrue(this.iPage < (BTCURSOR_MAX_DEPTH - 1), SqlJetErrorCode.CORRUPT);
        SqlJetMemPage pNewPage = pBt.getAndInitPage(newPgno);
        addNewPage(pNewPage);

        this.info.nSize = 0;
        this.validNKey = false;
        SqlJetAssert.assertTrue(pNewPage.nCell>0, SqlJetErrorCode.CORRUPT);
    }

    /**
     * Make sure the BtCursor has a valid BtCursor.info structure. If it is not
     * already valid, call sqlite3BtreeParseCell() to fill it in.
     *
     * BtCursor.info is a cache of the information in the current cell. Using
     * this cache reduces the number of calls to sqlite3BtreeParseCell().
     *
     */
    private void getCellInfo() {
        if (this.info.nSize == 0) {
            this.info = this.getCurrentPage().parseCell(getIndexOnCurrentPage());
            this.validNKey = true;
        }
    }

    /**
     * Return a pointer to payload information from the entry that the pCur
     * cursor is pointing to. The pointer is to the beginning of the key if
     * skipKey==0 and it points to the beginning of data if skipKey==1. The
     * number of bytes of available key/data is written into *pAmt. If *pAmt==0,
     * then the value returned will not be a valid pointer.
     *
     * This routine is an optimization. It is common for the entire key and data
     * to fit on the local page and for there to be no overflow pages. When that
     * is so, this routine can be used to access the key and data without making
     * a copy. If the key and/or data spills onto overflow pages, then
     * accessPayload() must be used to reassembly the key/data and copy it into
     * a preallocated buffer.
     *
     * The pointer returned by this routine looks directly into the cached page
     * of the database. The data might change or move the next time any btree
     * routine is called.
     *
     * @param pAmt
     *            Write the number of available bytes here
     * @param skipKey
     *            read beginning at data if this is true
     * @return
     */
    private ISqlJetMemoryPointer fetchPayload(int[] pAmt, boolean skipKey) {
        int nLocal;

        assert (this.iPage >= 0 && getCurrentPage() != null);
        assert (this.eState == SqlJetCursorState.VALID);
        assert (this.holdsMutex());
        SqlJetMemPage pPage = getCurrentPage();
        assert (getIndexOnCurrentPage() < pPage.nCell);
        this.getCellInfo();
        ISqlJetMemoryPointer aPayload = this.info.pCell.pointer(this.info.nHeader);
        int nKey = pPage.intKey ? 0 : (int) this.info.getnKey();
        if (skipKey) {
        	aPayload.movePointer(nKey);
            nLocal = this.info.nLocal - nKey;
        } else {
            nLocal = Integer.min(nKey, this.info.nLocal);
        }
        pAmt[0] = nLocal;
        return aPayload;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#moveToUnpacked(org.tmatesoft
     * .sqljet.core.ISqlJetUnpackedRecord, long, boolean)
     */
    @Override
	public int moveToUnpacked(ISqlJetUnpackedRecord pIdxKey, long intKey, boolean biasRight) throws SqlJetException {
        assert (holdsMutex());
        assert (pBtree.db.getMutex().held());

        /*
         * If the cursor is already positioned at the point we are trying to
         * move to, then just return without doing any work
         */
        if (this.eState == SqlJetCursorState.VALID && this.validNKey && this.apPage[0].getPage().intKey) {
            if (this.info.getnKey() == intKey) {
                return 0;
            }
            if (this.atLast && this.info.getnKey() < intKey) {
                return -1;
            }
        }

        moveToRoot();

        assert (getCurrentPage() != null);
        assert (getCurrentPage().isInit);
        if (this.eState == SqlJetCursorState.INVALID) {
            assert (getCurrentPage().nCell == 0);
            return -1;
        }
        assert (this.apPage[0].getPage().intKey || pIdxKey != null);
        while (true) {
            SqlJetMemPage pPage = getCurrentPage();
            int c = -1; /* pRes return if table is empty must be -1 */
            int lwr = 0;
            int upr = pPage.nCell - 1;
            SqlJetAssert.assertFalse((!pPage.intKey && pIdxKey == null) || upr < 0, SqlJetErrorCode.CORRUPT);
            if (biasRight) {
                setIndexOnCurrentPage(upr);
            } else {
                setIndexOnCurrentPage((upr + lwr) / 2);
            }
            while (true) {
                long key = 0;
                int idx = getIndexOnCurrentPage();
                this.info.nSize = 0;
                this.validNKey = true;
                if (pPage.intKey) {
                    ISqlJetMemoryPointer pCell;
                    pCell = pPage.findCell(idx).pointer(pPage.getChildPtrSize());
                    if (pPage.hasData) {
                        pCell.movePointer(pCell.getVarint32().getOffset());
                    }
                    key = pCell.getVarint().getValue();
                    c = Long.compare(key, intKey);
                } else {
                    int[] available = new int[1];
                    ISqlJetMemoryPointer pCellKey = this.fetchPayload(available, false);
                    key = this.info.getnKey();
                    if (available[0] >= key) {
                        c = pIdxKey.recordCompare((int) key, pCellKey);
                    } else {
                        pCellKey = SqlJetUtility.memoryManager.allocatePtr((int) key);
                        try {
                            this.key(0, (int) key, pCellKey);
                        } finally {
                            c = pIdxKey.recordCompare((int) key, pCellKey);
                            // sqlite3_free(pCellKey);
                        }
                    }
                }
                if (c == 0) {
                    this.info.setnKey(key);
                    if (pPage.intKey && !pPage.leaf) {
                        lwr = idx;
                        upr = lwr - 1;
                        break;
                    } else {
                        return 0;
                    }
                }
                if (c < 0) {
                    lwr = idx + 1;
                } else {
                    upr = idx - 1;
                }
                if (lwr > upr) {
                    this.info.setnKey(key);
                    break;
                }
                setIndexOnCurrentPage((lwr + upr) / 2);
            }
            assert (lwr == upr + 1);
            assert (pPage.isInit);
            int chldPg;
            if (pPage.leaf) {
                chldPg = 0;
            } else if (lwr >= pPage.nCell) {
                chldPg = pPage.aData.getInt(pPage.getHdrOffset() + 8);
            } else {
                chldPg = pPage.findCell(lwr).getInt();
            }
            if (chldPg == 0) {
                assert (getIndexOnCurrentPage() < getCurrentPage().nCell);
                return c;
            }
            setIndexOnCurrentPage(lwr);
            this.info.nSize = 0;
            this.validNKey = false;
            moveToChild(chldPg);
        }
    }

    /**
     * Restore the cursor to the position it was in (or as close to as possible)
     * when saveCursorPosition() was called. Note that this call deletes the
     * saved position info stored by saveCursorPosition(), so there can be at
     * most one effective restoreCursorPosition() call after each
     * saveCursorPosition().
     */
    @Override
	public void restoreCursorPosition() throws SqlJetException {
        if (this.eState.compareTo(SqlJetCursorState.REQUIRESEEK) < 0) {
			return;
		}
        assert (this.holdsMutex());
        if (this.eState == SqlJetCursorState.FAULT) {
            throw new SqlJetException(this.error);
        }
        this.eState = SqlJetCursorState.INVALID;
        this.skip = this.moveTo(this.pKey, this.nKey, false);
        this.pKey = null;
        assert (this.eState == SqlJetCursorState.VALID || this.eState == SqlJetCursorState.INVALID);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#cursorHasMoved()
     */
    @Override
	public boolean cursorHasMoved() {
        try {
            restoreCursorPosition();
        } catch (SqlJetException e) {
            return true;
        }
        return eState != SqlJetCursorState.VALID || skip != 0;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#delete()
     */
	@Override
	public void delete() throws SqlJetException {
		SqlJetBtreeShared pBt = pBtree.pBt;

		assert (this.holdsMutex());
		assert (pBt.inTransaction == TransMode.WRITE);
		assert (!pBt.readOnly);
		assert (this.wrFlag);

		// assert( hasSharedCacheTableLock(p, pCur->pgnoRoot, pCur->pKeyInfo!=0,
		// 2) );
		// assert( !hasReadConflicts(p, pCur->pgnoRoot) );

		if (getIndexOnCurrentPage() >= getCurrentPage().nCell || this.eState != SqlJetCursorState.VALID) {
			/* Something has gone awry. */
			throw new SqlJetException(SqlJetErrorCode.ERROR);
		}

		/*
		 * If this is a delete operation to remove a row from a table b-tree,
		 * invalidate any incrblob cursors open on the row being deleted.
		 */
		// if( pCur.pKeyInfo==null ){
		// p.invalidateIncrblobCursors(pCur.info.nKey, 0);
		// }

		/* Depth of node containing pCell */
		final int iCellDepth = this.iPage;
		/* Index of cell to delete */
		int iCellIdx = getIndexOnCurrentPage();
		/* Page to delete cell from */
		SqlJetMemPage pPage = getCurrentPage();
		/* Pointer to cell to delete */
		ISqlJetMemoryPointer pCell = pPage.findCell(iCellIdx);

		/*
		 * If the page containing the entry to delete is not a leaf page, move
		 * the cursor to the largest entry in the tree that is smaller than the
		 * entry being deleted. This cell will replace the cell being deleted
		 * from the internal node. The 'previous' entry is used for this instead
		 * of the 'next' entry, as the previous entry is always a part of the
		 * sub-tree headed by the child page of the cell being deleted. This
		 * makes balancing the tree following the delete operation easier.
		 */
		if (!pPage.leaf) {
			this.previous();
		}

		/*
		 * Save the positions of any other cursors open on this table before
		 ** making any modifications. Make the page containing the entry to be
		 ** deleted writable. Then free any overflow pages associated with the
		 ** entry and finally remove the cell itself from within the page.
		 */
		pBt.saveAllCursors(this.pgnoRoot, this);
		pPage.pDbPage.write();
		pPage.clearCell(pCell);
		pPage.dropCell(iCellIdx, pPage.cellSizePtr(pCell));

		/*
		 * If the cell deleted was not located on a leaf page, then the cursor
		 * is currently pointing to the largest entry in the sub-tree headed by
		 * the child-page of the cell that was just deleted from an internal
		 * node. The cell from the leaf node needs to be moved to the internal
		 * node to replace the deleted cell.
		 */
		if (!pPage.leaf) {
			SqlJetMemPage pLeaf = getCurrentPage();
			int n = this.apPage[iCellDepth + 1].getPage().pgno;

			pCell = pLeaf.findCell(pLeaf.nCell - 1);
			int nCell = pLeaf.cellSizePtr(pCell);
			assert (pBt.mxCellSize() >= nCell);

			ISqlJetMemoryPointer pTmp = pBt.allocateTempSpace();

			pLeaf.pDbPage.write();
			pPage.insertCell(iCellIdx, pCell.getMoved(-4), nCell + 4, pTmp, n);
			pLeaf.dropCell(pLeaf.nCell - 1, nCell);
		}

		/*
		 * Balance the tree. If the entry deleted was located on a leaf page,
		 * then the cursor still points to that page. In this case the first
		 * call to balance() repairs the tree, and the if(...) condition is
		 * never true.
		 * 
		 * Otherwise, if the entry deleted was on an internal node page, then
		 * pCur is pointing to the leaf page from which a cell was removed to
		 * replace the cell deleted from the internal node. This is slightly
		 * tricky as the leaf node may be underfull, and the internal node may
		 * be either under or overfull. In this case run the balancing algorithm
		 * on the leaf node first. If the balance proceeds far enough up the
		 * tree that we can be sure that any problem in the internal node has
		 * been corrected, so be it. Otherwise, after balancing the leaf node,
		 * walk the cursor up the tree to the internal node and balance it as
		 * well.
		 */
		this.balance(false);
		if (this.iPage > iCellDepth) {
			while (this.iPage > iCellDepth) {
				SqlJetMemPage.releasePage(popCurrentPage());
			}
			this.balance(false);
		}
		this.moveToRoot();
	}

    /**
     * The page that pCur currently points to has just been modified in some
     * way. This function figures out if this modification means the tree needs
     * to be balanced, and if so calls the appropriate balancing routine.
     *
     * Parameter isInsert is true if a new cell was just inserted into the page,
     * or false otherwise.
     *
     * @param i
     * @throws SqlJetException
     */
	private void balance(boolean isInsert) throws SqlJetException {
		final int nMin = this.pBt.usableSize * 2 / 3;
		ISqlJetMemoryPointer aBalanceQuickSpace = SqlJetUtility.memoryManager.allocatePtr(13);

		int balance_quick_called = 0; // TESTONLY
		int balance_deeper_called = 0; // TESTONLY

		do {
			SqlJetMemPage pPage = getCurrentPage();

			if (iPage == 0) {
				if (pPage.aOvfl.size() > 0) {
					/*
					 * The root page of the b-tree is overfull. In this case
					 * call the balance_deeper() function to create a new child
					 * for the root-page and copy the current contents of the
					 * root-page to it. The next iteration of the do-loop will
					 * balance the child page.
					 */
					assert ((balance_deeper_called++) == 0);
					addNewPage(balanceDeeper(pPage));
					this.apPage[0].setIndex(0);
					assert (!this.apPage[1].getPage().aOvfl.isEmpty());
				} else {
					break;
				}
			} else if (pPage.aOvfl.isEmpty() && pPage.nFree <= nMin) {
				break;
			} else {
				final SqlJetMemPage pParent = this.apPage[iPage - 1].getPage();
				final int iIdx = this.apPage[iPage - 1].getIndex();

				pParent.pDbPage.write();

				if (pPage.hasData && pPage.aOvfl.size() == 1 && pPage.aOvfl.get(0).getIdx() == pPage.nCell
						&& pParent.pgno != 1 && pParent.nCell == iIdx) {
					/*
					 * Call balance_quick() to create a new sibling of pPage on
					 * which to store the overflow cell. balance_quick() inserts
					 * a new cell into pParent, which may cause pParent
					 * overflow. If this happens, the next interation of the
					 * do-loop will balance pParent use either balance_nonroot()
					 * or balance_deeper(). Until this happens, the overflow
					 * cell is stored in the aBalanceQuickSpace[] buffer.
					 **
					 ** The purpose of the following assert() is to check that
					 * only a single call to balance_quick() is made for each
					 * call to this function. If this were not verified, a
					 * subtle bug involving reuse of the aBalanceQuickSpace[]
					 * might sneak in.
					 */
					assert ((balance_quick_called++) == 0);
					balanceQuick(pParent, pPage, aBalanceQuickSpace);
				} else {
					/*
					 * In this case, call balance_nonroot() to redistribute
					 * cells between pPage and up to 2 of its sibling pages.
					 * This involves modifying the contents of pParent, which
					 * may cause pParent to become overfull or underfull. The
					 * next iteration of the do-loop will balance the parent
					 * page to correct this.
					 **
					 ** If the parent page becomes overfull, the overflow cell or
					 * cells are stored in the pSpace buffer allocated
					 * immediately below. A subsequent iteration of the do-loop
					 * will deal with this by calling balance_nonroot()
					 * (balance_deeper() may be called first, but it doesn't
					 * deal with overflow cells - just moves them to a different
					 * page). Once this subsequent call to balance_nonroot() has
					 * completed, it is safe to release the pSpace buffer used
					 * by the previous call, as the overflow cell data will have
					 * been copied either into the body of a database page or
					 * into the new pSpace buffer passed to the latter call to
					 * balance_nonroot().
					 */
					ISqlJetMemoryPointer pSpace = SqlJetUtility.memoryManager.allocatePtr(this.pBt.pageSize);
					balanceNonroot(pParent, iIdx, pSpace, iPage == 1);
				}

				pPage.aOvfl.clear();

				/*
				 * The next iteration of the do-loop balances the parent page.
				 */
				SqlJetMemPage.releasePage(pPage);
				this.iPage--;
			}
		} while (true);
	}

	/**
	 * This routine redistributes cells on the iParentIdx'th child of pParent
	 * (hereafter "the page") and up to 2 siblings so that all pages have about
	 * the same amount of free space. Usually a single sibling on either side of
	 * the page are used in the balancing, though both siblings might come from
	 * one side if the page is the first or last child of its parent. If the
	 * page has fewer than 2 siblings (something which can only happen if the
	 * page is a root page or a child of a root page) then all available
	 * siblings participate in the balancing.
	 *
	 * The number of siblings of the page might be increased or decreased by one
	 * or two in an effort to keep pages nearly full but not over full.
	 *
	 * Note that when this routine is called, some of the cells on the page
	 * might not actually be stored in MemPage.aData[]. This can happen if the
	 * page is overfull. This routine ensures that all cells allocated to the
	 * page and its siblings fit into MemPage.aData[] before returning.
	 *
	 * In the course of balancing the page and its siblings, cells may be
	 * inserted into or removed from the parent page (pParent). Doing so may
	 * cause the parent page to become overfull or underfull. If this happens,
	 * it is the responsibility of the caller to invoke the correct balancing
	 * routine to fix this problem (see the balance() routine).
	 *
	 * If this routine fails for any reason, it might leave the database in a
	 * corrupted state. So if this routine fails, the database should be rolled
	 * back.
	 *
	 * The third argument to this function, aOvflSpace, is a pointer to a buffer
	 * big enough to hold one page. If while inserting cells into the parent
	 * page (pParent) the parent page becomes overfull, this buffer is used to
	 * store the parent's overflow cells. Because this function inserts a
	 * maximum of four divider cells into the parent page, and the maximum size
	 * of a cell stored within an internal node is always less than 1/4 of the
	 * page-size, the aOvflSpace[] buffer is guaranteed to be large enough for
	 * all overflow cells.
	 *
	 * If aOvflSpace is set to a null pointer, this function returns
	 * SQLITE_NOMEM.
	 *
	 * @param pParent
	 *            Parent page of siblings being balanced
	 * @param iParentIdx
	 *            Index of "the page" in pParent
	 * @param aOvflSpace
	 *            page-size bytes of space for parent ovfl
	 * @param isRoot
	 *            True if pParent is a root-page
	 *
	 * @throws SqlJetException
	 */
	private void balanceNonroot(SqlJetMemPage pParent, int iParentIdx,
			ISqlJetMemoryPointer aOvflSpace, boolean isRoot) throws SqlJetException {

		  int nCell = 0;               /* Number of cells in apCell[] */
		  int nMaxCells = 0;           /* Allocated size of apCell, szCell, aFrom. */
		  int nNew = 0;                /* Number of pages in apNew[] */
		  int nOld = 0;                    /* Number of pages in apOld[] */
		  int i, j, k;                 /* Loop counters */
		  int nxDiv;                   /* Next divider slot in pParent->aCell[] */
		  int leafCorrection;          /* 4 if pPage is a leaf.  0 if not */
		  boolean leafData;                /* True if pPage is a leaf of a LEAFDATA tree */
		  int pageFlags;               /* Value of pPage->aData[0] */
		  int subtotal;                /* Subtotal of bytes in cells on one page */
		  int iSpace1 = 0;             /* First unused byte of aSpace1[] */
		  int iOvflSpace = 0;          /* First unused byte of aOvflSpace[] */
		  //int szScratch;               /* Size of scratch memory requested */
		  SqlJetMemPage[] apOld = new SqlJetMemPage[NB];          /* pPage and up to two siblings */
		  SqlJetMemPage[] apCopy = new SqlJetMemPage[NB];         /* Private copies of apOld[] pages */
		  SqlJetMemPage[] apNew = new SqlJetMemPage[NB+2];        /* pPage and up to NB siblings after balancing */
		  /*u8*/ISqlJetMemoryPointer pRight;                  /* Location in parent of right-sibling pointer */
		  /*u8*/ISqlJetMemoryPointer[] apDiv = new ISqlJetMemoryPointer[NB-1];             /* Divider cells in pParent */
		  int[] szNew=new int[NB+2];             /* Combined size of cells place on i-th page */
		  /*u8*/ ISqlJetMemoryPointer[] apCell;             /* All cells begin balanced */
		  /*u16*/ int[] szCell;                 /* Local size of all cells in apCell[] */
		  /*u8*/ //SqlJetMemPage[] aSpace1;                 /* Space for copies of dividers cells */
		  int pgno;                   /* Temp var to store a page number in */

		  SqlJetBtreeShared pBt = pParent.pBt;               /* The whole database */
		  assert(mutexHeld(pBt.mutex) );
		  assert(pParent.pDbPage.isWriteable());

		  /* At this point pParent may have at most one overflow cell. And if
		  ** this overflow cell is present, it must be the cell with
		  ** index iParentIdx. This scenario comes about when this function
		  ** is called (indirectly) from sqlite3BtreeDelete().
		  */
		  assert( pParent.aOvfl.size()<=1 );
		  assert( pParent.aOvfl.isEmpty() || pParent.aOvfl.get(0).getIdx()==iParentIdx );

		  try{
		  /* Find the sibling pages to balance. Also locate the cells in pParent
		  ** that divide the siblings. An attempt is made to find NN siblings on
		  ** either side of pPage. More siblings are taken from one side, however,
		  ** if there are fewer than NN siblings on the other side. If pParent
		  ** has NB or fewer children then all children of pParent are taken.
		  **
		  ** This loop also drops the divider cells from the parent page. This
		  ** way, the remainder of the function does not have to deal with any
		  ** overflow cells in the parent page, since if any existed they will
		  ** have already been removed.
		  */
		  i = pParent.aOvfl.size() + pParent.nCell;
		  if( i<2 ){
		    nxDiv = 0;
		    nOld = i+1;
		  }else{
		    nOld = 3;
		    if( iParentIdx==0 ){
		      nxDiv = 0;
		    }else if( iParentIdx==i ){
		      nxDiv = i-2;
		    }else{
		      nxDiv = iParentIdx-1;
		    }
		    i = 2;
		  }
		  if( (i+nxDiv-pParent.aOvfl.size())==pParent.nCell ){
		    pRight = pParent.aData.getMoved(pParent.getHdrOffset()+8);
		  }else{
		    pRight = pParent.findCell(i+nxDiv-pParent.aOvfl.size());
		  }
		  pgno = pRight.getInt();
		  while( true ){
			apOld[i] = pBt.getAndInitPage(pgno);
		    nMaxCells += 1+apOld[i].nCell+apOld[i].aOvfl.size();
		    if( (i--)==0 ) {
				break;
			}

		    if( !pParent.aOvfl.isEmpty() && i+nxDiv==pParent.aOvfl.get(0).getIdx() ){
		      apDiv[i] = pParent.aOvfl.get(0).getpCell();
		      pgno = apDiv[i].getInt();
		      szNew[i] = pParent.cellSizePtr(apDiv[i]);
		      pParent.aOvfl.clear();
		    }else{
		      apDiv[i] = pParent.findCell(i+nxDiv-pParent.aOvfl.size());
		      pgno = apDiv[i].getInt();
		      szNew[i] = pParent.cellSizePtr(apDiv[i]);

		      /* Drop the cell from the parent page. apDiv[i] still points to
		      ** the cell within the parent, even though it has been dropped.
		      ** This is safe because dropping a cell only overwrites the first
		      ** four bytes of it, and this function does not need the first
		      ** four bytes of the divider cell. So the pointer is safe to use
		      ** later on.
		      **
		      ** But not if we are in secure-delete mode. In secure-delete mode,
		      ** the dropCell() routine will overwrite the entire cell with zeroes.
		      ** In this case, temporarily copy the cell into the aOvflSpace[]
		      ** buffer. It will be copied out again as soon as the aSpace[] buffer
		      ** is allocated.  */
		      if( ISqlJetConfig.SECURE_DELETE ){
		        int iOff = apDiv[i].getPointer() - pParent.aData.getPointer();
		        if( (iOff+szNew[i])>pBt.usableSize ){
				  Arrays.fill(apOld, 0, i, null);
		          //rc = SqlJetErrorCode.CORRUPT;
				  //goto balance_cleanup;
				  throw new SqlJetException(SqlJetErrorCode.CORRUPT);
		        }else{
		        	aOvflSpace.getMoved(iOff).copyFrom(apDiv[i], szNew[i]);
		          apDiv[i] = aOvflSpace.getMoved(apDiv[i].getPointer() - pParent.aData.getPointer());
		        }
		      }
		      try {
		    	  pParent.dropCell( i+nxDiv-pParent.aOvfl.size(), szNew[i]);
		      } catch(SqlJetException e) {
		    	  //rc = e.getErrorCode();
		          TRACE("exception in dropCell call: %s", e.getMessage());
		    	  // e.printStackTrace();
		      }
		    }
		  }

		  /* Make nMaxCells a multiple of 4 in order to preserve 8-byte
		   ** alignment */
		  nMaxCells = (nMaxCells + 3)&~3;

		  /*
		  ** Allocate space for memory structures
		  */
		  apCell = new SqlJetMemoryPointer[nMaxCells];
		  szCell = new int[nMaxCells];
		  //aSpace1 = new SqlJetMemPage[nMaxCells];

		  /*
		  ** Load pointers to all cells on sibling pages and the divider cells
		  ** into the local apCell[] array.  Make copies of the divider cells
		  ** into space obtained from aSpace1[] and remove the the divider Cells
		  ** from pParent.
		  **
		  ** If the siblings are on leaf pages, then the child pointers of the
		  ** divider cells are stripped from the cells before they are copied
		  ** into aSpace1[].  In this way, all cells in apCell[] are without
		  ** child pointers.  If siblings are not leaves, then all cell in
		  ** apCell[] include child pointers.  Either way, all cells in apCell[]
		  ** are alike.
		  **
		  ** leafCorrection:  4 if pPage is a leaf.  0 if pPage is not a leaf.
		  **       leafData:  1 if pPage holds key+data and pParent holds only keys.
		  */
		  leafCorrection = apOld[0].leaf?4:0;
		  leafData = apOld[0].hasData;
		  for(i=0; i<nOld; i++){
		    int limit;

		    /* Before doing anything else, take a copy of the i'th original sibling
		    ** The rest of this function will use data from the copies rather
		    ** that the original pages since the original pages will be in the
		    ** process of being overwritten.  */
		    SqlJetMemPage pOld = apCopy[i] = memcpy(apOld[i]);
		    pOld.aData.copyFrom(apOld[i].aData,pBt.pageSize);

		    limit = pOld.nCell+pOld.aOvfl.size();
		    if( !pOld.aOvfl.isEmpty() ){
		      for(j=0; j<limit; j++){
		        assert( nCell<nMaxCells );
		        apCell[nCell] = pOld.findOverflowCell(j);
		        szCell[nCell] = pOld.cellSizePtr( apCell[nCell]);
		        nCell++;
		      }
		    }else{
		      ISqlJetMemoryPointer aData = pOld.aData;
		      int maskPage = pOld.maskPage;
		      int cellOffset = pOld.cellOffset;
		      for(j=0; j<limit; j++){
		        assert( nCell<nMaxCells );
		        apCell[nCell] = findCellv2(aData, maskPage, cellOffset, j);
		        szCell[nCell] = pOld.cellSizePtr(apCell[nCell]);
		        nCell++;
		      }
		    }
		    if( i<nOld-1 && !leafData){
		      int sz =szNew[i];
		      ISqlJetMemoryPointer pTemp;
		      assert( nCell<nMaxCells );
		      szCell[nCell] = sz;
		      pTemp = SqlJetUtility.memoryManager.allocatePtr(sz);
		      //pTemp = &aSpace1[iSpace1];
		      iSpace1 += sz;
		      assert( sz<=pBt.getMaxLocal()+23 );
		      assert( iSpace1 <= pBt.pageSize );
		      pTemp.copyFrom(apDiv[i], sz);
		      apCell[nCell] = pTemp.getMoved(leafCorrection);
		      assert( leafCorrection==0 || leafCorrection==4 );
		      szCell[nCell] = szCell[nCell] - leafCorrection;
		      if( !pOld.leaf ){
		        assert( leafCorrection==0 );
		        assert( pOld.getHdrOffset()==0 );
		        /* The right pointer of the child page pOld becomes the left
		        ** pointer of the divider cell */
		        apCell[nCell].copyFrom(pOld.aData.getMoved(8), 4);
		      }else{
		        assert( leafCorrection==4 );
		        if( szCell[nCell]<4 ){
		          /* Do not allow any cells smaller than 4 bytes. */
		          szCell[nCell] = 4;
		        }
		      }
		      nCell++;
		    }
		  }

		  /*
		  ** Figure out the number of pages needed to hold all nCell cells.
		  ** Store this number in "k".  Also compute szNew[] which is the total
		  ** size of all cells on the i-th page and cntNew[] which is the index
		  ** in apCell[] of the cell that divides page i from page i+1.
		  ** cntNew[k] should equal nCell.
		  **
		  ** Values computed by this block:
		  **
		  **           k: The total number of sibling pages
		  **    szNew[i]: Spaced used on the i-th sibling page.
		  **   cntNew[i]: Index in apCell[] and szCell[] for the first cell to
		  **              the right of the i-th sibling page.
		  ** usableSpace: Number of bytes of space available on each sibling.
		  **
		  */
		  int[] cntNew=new int[NB+2];            /* Index in aCell[] of cell after i-th page */
		  int usableSpace = pBt.usableSize - 12 + leafCorrection;             /* Bytes in pPage beyond the header */
		  for(subtotal=k=i=0; i<nCell; i++){
		    assert( i<nMaxCells );
		    subtotal += szCell[i] + 2;
		    if( subtotal > usableSpace ){
		      szNew[k] = subtotal - szCell[i];
		      cntNew[k] = i;
		      if( leafData ){ i--; }
		      subtotal = 0;
		      k++;
		      if( k>NB+1 ){
		    	  //rc = SqlJetErrorCode.CORRUPT; break balance_cleanup;
		    	  throw new SqlJetException(SqlJetErrorCode.CORRUPT);
		      }
		    }
		  }
		  szNew[k] = subtotal;
		  cntNew[k] = nCell;
		  k++;

		  /*
		  ** The packing computed by the previous block is biased toward the siblings
		  ** on the left side.  The left siblings are always nearly full, while the
		  ** right-most sibling might be nearly empty.  This block of code attempts
		  ** to adjust the packing of siblings to get a better balance.
		  **
		  ** This adjustment is more than an optimization.  The packing above might
		  ** be so out of balance as to be illegal.  For example, the right-most
		  ** sibling might be completely empty.  This adjustment is not optional.
		  */
		  for(i=k-1; i>0; i--){
		    int szRight = szNew[i];  /* Size of sibling on the right */
		    int szLeft = szNew[i-1]; /* Size of sibling on the left */
		    int r;              /* Index of right-most cell in left sibling */
		    int d;              /* Index of first cell to the left of right sibling */

		    r = cntNew[i-1] - 1;
		    d = r + 1 - (leafData?1:0);
		    assert( d<nMaxCells );
		    assert( r<nMaxCells );
		    while( szRight==0 || szRight+szCell[d]+2<=szLeft-(szCell[r]+2) ){
		      szRight += szCell[d] + 2;
		      szLeft -= szCell[r] + 2;
		      cntNew[i-1]--;
		      r = cntNew[i-1] - 1;
		      d = r + 1 - (leafData?1:0);
		    }
		    szNew[i] = szRight;
		    szNew[i-1] = szLeft;
		  }

		  /* Either we found one or more cells (cntnew[0])>0) or pPage is
		  ** a virtual root page.  A virtual root page is when the real root
		  ** page is page 1 and we are the only child of that page.
		  */
		  assert( cntNew[0]>0 || (pParent.pgno==1 && pParent.nCell==0) );

		  traceInt("BALANCE: old: %d %d %d  ",
		    apOld[0].pgno,
		    nOld>=2 ? apOld[1].pgno : 0,
		    nOld>=3 ? apOld[2].pgno : 0
		  );

		  /*
		  ** Allocate k new pages.  Reuse old pages where possible.
		  */
		  if( apOld[0].pgno<=1 ){
		    //rc = SqlJetErrorCode.CORRUPT;
		    //break balance_cleanup;
			throw new SqlJetException(SqlJetErrorCode.CORRUPT);
		  }
		  pageFlags = apOld[0].aData.getByteUnsigned(0);
		  for(i=0; i<k; i++){
		    SqlJetMemPage pNew;
		    if( i<nOld ){
		      pNew = apNew[i] = apOld[i];
		      apOld[i] = null;
		      nNew++;
	    	  pNew.pDbPage.write();
		    }else{
		      assert( i>0 );

	    	  int[] p = {0};
	    	  pNew = pBt.allocatePage(p, pgno, false);
	    	  pgno = p[0];

		      apNew[i] = pNew;
		      nNew++;

		      /* Set the pointer-map entry for the new sibling page. */
		      if( pBt.autoVacuumMode.isAutoVacuum() ){
	    		  pBt.ptrmapPut(pNew.pgno, SqlJetPtrMapType.PTRMAP_BTREE, pParent.pgno);
		      }
		    }
		  }

		  /* Free any old pages that were not reused as new pages.
		  */
		  while( i<nOld ){
			  apOld[i].freePage();
			  SqlJetMemPage.releasePage(apOld[i]);
			  apOld[i] = null;
			  i++;
		  }

		  /*
		  ** Put the new pages in accending order.  This helps to
		  ** keep entries in the disk file in order so that a scan
		  ** of the table is a linear scan through the file.  That
		  ** in turn helps the operating system to deliver pages
		  ** from the disk more rapidly.
		  **
		  ** An O(n^2) insertion sort algorithm is used, but since
		  ** n is never more than NB (a small constant), that should
		  ** not be a problem.
		  **
		  ** When NB==3, this one optimization makes the database
		  ** about 25% faster for large insertions and deletions.
		  */
		  for(i=0; i<k-1; i++){
		    int minV = apNew[i].pgno;
		    int minI = i;
		    for(j=i+1; j<k; j++){
		      if( apNew[j].pgno</*(unsigned)*/minV ){
		        minI = j;
		        minV = apNew[j].pgno;
		      }
		    }
		    if( minI>i ){
		      SqlJetMemPage pT;
		      pT = apNew[i];
		      apNew[i] = apNew[minI];
		      apNew[minI] = pT;
		    }
		  }
		  traceInt("new: %d(%d) %d(%d) %d(%d) %d(%d) %d(%d)\n",
		    apNew[0].pgno, szNew[0],
		    nNew>=2 ? apNew[1].pgno : 0, nNew>=2 ? szNew[1] : 0,
		    nNew>=3 ? apNew[2].pgno : 0, nNew>=3 ? szNew[2] : 0,
		    nNew>=4 ? apNew[3].pgno : 0, nNew>=4 ? szNew[3] : 0,
		    nNew>=5 ? apNew[4].pgno : 0, nNew>=5 ? szNew[4] : 0);

		  assert( pParent.pDbPage.isWriteable() );
		  pRight.putIntUnsigned(0, apNew[nNew-1].pgno);

		  /*
		  ** Evenly distribute the data in apCell[] across the new pages.
		  ** Insert divider cells into pParent as necessary.
		  */
		  j = 0;
		  for(i=0; i<nNew; i++){
		    /* Assemble the new sibling page. */
		    SqlJetMemPage pNew = apNew[i];
		    assert( j<nMaxCells );
		    pNew.zeroPage(pageFlags);
		    pNew.assemblePage(cntNew[i]-j, apCell,j, szCell,j);
		    assert( pNew.nCell>0 || (nNew==1 && cntNew[0]==0) );
		    assert( pNew.aOvfl.isEmpty() );

		    j = cntNew[i];

		    /* If the sibling page assembled above was not the right-most sibling,
		    ** insert a divider cell into the parent page.
		    */
		    assert( i<nNew-1 || j==nCell );
		    if( j<nCell ){
		      assert( j<nMaxCells );
		      ISqlJetMemoryPointer pCell = apCell[j];
		      int sz = szCell[j] + leafCorrection;
		      ISqlJetMemoryPointer pTemp = aOvflSpace.getMoved(iOvflSpace);
		      if( !pNew.leaf ){
		    	  pNew.aData.getMoved(8).copyFrom(pCell, 4);
		      }
		      else if( leafData ){
		        /* If the tree is a leaf-data tree, and the siblings are leaves,
		        ** then there is no divider cell in apCell[]. Instead, the divider
		        ** cell consists of the integer key for the right-most cell of
		        ** the sibling-page assembled above only.
		        */
		    	SqlJetBtreeCellInfo info;
		        j--;
		        info = pNew.parseCellPtr(apCell[j]);
		        pCell = pTemp;
		        sz = 4 + pCell.getMoved(4).putVarint(info.getnKey());
		        // XXX there is no such code in sqlite.
		        pCell.putIntUnsigned(0, pNew.pgno);
		        pTemp = null;
		      } else {
		        pCell = SqlJetUtility.getMoved(j > 0 ? apCell[j - 1] : null, pCell, -4);
		        /* Obscure case for non-leaf-data trees: If the cell at pCell was
		        ** previously stored on a leaf node, and its reported size was 4
		        ** bytes, then it may actually be smaller than this
		        ** (see btreeParseCellPtr(), 4 bytes is the minimum size of
		        ** any cell). But it is important to pass the correct size to
		        ** insertCell(), so reparse the cell now.
		        **
		        ** Note that this can never happen in an SQLite data file, as all
		        ** cells are at least 4 bytes. It only happens in b-trees used
		        ** to evaluate "IN (SELECT ...)" and similar clauses.
		        */
		        if( szCell[j]==4 ){
		          assert(leafCorrection==4);
		          sz = pParent.cellSizePtr(pCell);
		        }
		      }
		      iOvflSpace += sz;
		      assert( sz<=pBt.getMaxLocal()+23 );
		      assert( iOvflSpace <= pBt.pageSize );
		      pParent.insertCell(nxDiv, pCell, sz, pTemp, pNew.pgno);
		      assert( pParent.pDbPage.isWriteable() );

		      j++;
		      nxDiv++;
		    }
		  }
		  assert( j==nCell );
		  assert( nOld>0 );
		  assert( nNew>0 );
		  if( (pageFlags & SqlJetMemPage.PTF_LEAF)==0 ){
		    ISqlJetMemoryPointer zChild = apCopy[nOld-1].aData.getMoved(8);
		    apNew[nNew-1].aData.getMoved(8).copyFrom(zChild, 4);
		  }

		  if( isRoot && pParent.nCell==0 && pParent.getHdrOffset()<=apNew[0].nFree ){
		    /* The root page of the b-tree now contains no cells. The only sibling
		    ** page is the right-child of the parent. Copy the contents of the
		    ** child page into the parent, decreasing the overall height of the
		    ** b-tree structure by one. This is described as the "balance-shallower"
		    ** sub-algorithm in some documentation.
		    **
		    ** If this is an auto-vacuum database, the call to copyNodeContent()
		    ** sets all pointer-map entries corresponding to database image pages
		    ** for which the pointer is stored within the content being copied.
		    **
		    ** The second assert below verifies that the child page is defragmented
		    ** (it must be, as it was just reconstructed using assemblePage()). This
		    ** is important if the parent page happens to be page 1 of the database
		    ** image.  */
		    assert( nNew==1 );
		    assert( apNew[0].nFree ==
		        (apNew[0].aData.getMoved(5).getShortUnsigned()-apNew[0].cellOffset-apNew[0].nCell*2)
		    );
		    apNew[0].copyNodeContent( pParent );
		    apNew[0].freePage();
		  }else if( pBt.autoVacuumMode.isAutoVacuum() ){
		    /* Fix the pointer-map entries for all the cells that were shifted around.
		    ** There are several different types of pointer-map entries that need to
		    ** be dealt with by this routine. Some of these have been set already, but
		    ** many have not. The following is a summary:
		    **
		    **   1) The entries associated with new sibling pages that were not
		    **      siblings when this function was called. These have already
		    **      been set. We don't need to worry about old siblings that were
		    **      moved to the free-list - the freePage() code has taken care
		    **      of those.
		    **
		    **   2) The pointer-map entries associated with the first overflow
		    **      page in any overflow chains used by new divider cells. These
		    **      have also already been taken care of by the insertCell() code.
		    **
		    **   3) If the sibling pages are not leaves, then the child pages of
		    **      cells stored on the sibling pages may need to be updated.
		    **
		    **   4) If the sibling pages are not internal intkey nodes, then any
		    **      overflow pages used by these cells may need to be updated
		    **      (internal intkey nodes never contain pointers to overflow pages).
		    **
		    **   5) If the sibling pages are not leaves, then the pointer-map
		    **      entries for the right-child pages of each sibling may need
		    **      to be updated.
		    **
		    ** Cases 1 and 2 are dealt with above by other code. The next
		    ** block deals with cases 3 and 4 and the one after that, case 5. Since
		    ** setting a pointer map entry is a relatively expensive operation, this
		    ** code only sets pointer map entries for child or overflow pages that have
		    ** actually moved between pages.  */
		    SqlJetMemPage pNew = apNew[0];
		    SqlJetMemPage pOld = apCopy[0];
		    int nOverflow = pOld.aOvfl.size();
		    int iNextOld = pOld.nCell + nOverflow;
		    int iOverflow = (nOverflow>0 ? pOld.aOvfl.get(0).getIdx() : -1);
		    j = 0;                             /* Current 'old' sibling page */
		    k = 0;                             /* Current 'new' sibling page */
		    boolean isDivider = false;
		    for(i=0; i<nCell; i++){
		      while( i==iNextOld ){
		        /* Cell i is the cell immediately following the last cell on old
		        ** sibling page j. If the siblings are not leaf pages of an
		        ** intkey b-tree, then cell i was a divider cell. */
		        assert( j+1 < apCopy.length );
		        pOld = apCopy[++j];
		        iNextOld = i + (!leafData?1:0) + pOld.nCell + pOld.aOvfl.size();
		        if( !pOld.aOvfl.isEmpty() ){
		          nOverflow = pOld.aOvfl.size();
		          iOverflow = i + (!leafData?1:0) + pOld.aOvfl.get(0).getIdx();
		        }
		        isDivider = !leafData;
		      }

		      assert(nOverflow>0 || iOverflow<i );
		      assert(nOverflow<2 || pOld.aOvfl.get(1).getIdx()==pOld.aOvfl.get(0).getIdx()-1);
		      assert(nOverflow<3 || pOld.aOvfl.get(2).getIdx()==pOld.aOvfl.get(1).getIdx()-1);
		      if( i==iOverflow ){
		        isDivider = true;
		        if( (--nOverflow)>0 ){
		          iOverflow++;
		        }
		      }

		      if( i==cntNew[k] ){
		        /* Cell i is the cell immediately following the last cell on new
		        ** sibling page k. If the siblings are not leaf pages of an
		        ** intkey b-tree, then cell i is a divider cell.  */
		        pNew = apNew[++k];
		        if( !leafData ) {
					continue;
				}
		      }
		      assert( j<nOld );
		      assert( k<nNew );

		      /* If the cell was originally divider cell (and is not now) or
		      ** an overflow cell, or if the cell was located on a different sibling
		      ** page before the balancing, then the pointer map entries associated
		      ** with any child or overflow pages need to be updated.  */
		      if( isDivider || pOld.pgno!=pNew.pgno ){
		        if( !(leafCorrection>0) ){
		        	pBt.ptrmapPut(apCell[i].getInt(), SqlJetPtrMapType.PTRMAP_BTREE, pNew.pgno);
		        }
		        if( szCell[i]>pNew.minLocal ){
		        	pNew.ptrmapPutOvflPtr(apCell[i]);
		        }
		      }
		    }

		    if( !(leafCorrection>0) ){
		      for(i=0; i<nNew; i++){
		        int key = apNew[i].aData.getMoved(8).getInt();
		        pBt.ptrmapPut(key, SqlJetPtrMapType.PTRMAP_BTREE, apNew[i].pgno);
		      }
		    }

		  }

		  assert( pParent.isInit );
		  traceInt("BALANCE: finished: old=%d new=%d cells=%d\n",
		          nOld, nNew, nCell);

	} finally {
		  /*
		  ** Cleanup before returning.
		  */
		  // balance_cleanup:
		  //sqlite3ScratchFree(apCell);
		  for(i=0; i<nOld; i++){
		    SqlJetMemPage.releasePage(apOld[i]);
		  }
		  for(i=0; i<nNew; i++){
			  SqlJetMemPage.releasePage(apNew[i]);
		  }
	}


	} // balance_nonroot()

	//#define findCellv2(D,M,O,I) (D+(M&get2byte(D+(O+2*(I)))))
    private ISqlJetMemoryPointer findCellv2(ISqlJetMemoryPointer d, int M, int O, int I) {
    	return (d.getMoved(M & d.getMoved(O+2*(I)).getShortUnsigned()));
	}

    /**
    * This version of balance() handles the common special case where
    * a new entry is being inserted on the extreme right-end of the
    * tree, in other words, when the new entry will become the largest
    * entry in the tree.
    *
    * Instead of trying to balance the 3 right-most leaf pages, just add
    * a new page to the right-hand side and put the one new entry in
    * that page.  This leaves the right side of the tree somewhat
    * unbalanced.  But odds are that we will be inserting new entries
    * at the end soon afterwards so the nearly empty page will quickly
    * fill up.  On average.
    *
    * pPage is the leaf page which is the right-most page in the tree.
    * pParent is its parent.  pPage must have a single overflow entry
    * which is also the right-most entry on the page.
    *
    * The pSpace buffer is used to store a temporary copy of the divider
    * cell that will be inserted into pParent. Such a cell consists of a 4
    * byte page number followed by a variable length integer. In other
    * words, at most 13 bytes. Hence the pSpace buffer must be at
    * least 13 bytes in size.
    */
    private void balanceQuick(SqlJetMemPage pParent, SqlJetMemPage pPage, ISqlJetMemoryPointer pSpace) throws SqlJetException {

    	  final SqlJetBtreeShared pBt = pPage.pBt;    /* B-Tree Database */
    	  SqlJetMemPage pNew;                       /* Newly allocated page */
    	  int[] pgnoNew = {0};                        /* Page number of pNew */

    	  assert( mutexHeld(pPage.pBt.mutex) );
    	  assert( pParent.pDbPage.isWriteable() );
    	  assert( pPage.aOvfl.size()==1 );

    	  /* This error condition is now caught prior to reaching this function */
    	  if( pPage.nCell<=0 ) {
    		  throw new SqlJetException(SqlJetErrorCode.CORRUPT);
    	  }

    	  /* Allocate a new page. This page will become the right-sibling of
    	  ** pPage. Make the parent page writable, so that the new divider cell
    	  ** may be inserted. If both these operations are successful, proceed.
    	  */
    	  pNew = pBt.allocatePage(pgnoNew, 0, false);

    	  try{

    		ISqlJetMemoryPointer pOut = pSpace.getMoved(4);
    		ISqlJetMemoryPointer pCell = pPage.aOvfl.get(0).getpCell();
    	    int szCell = pPage.cellSizePtr(pCell);
    	    ISqlJetMemoryPointer pStop;

    	    assert( pNew.pDbPage.isWriteable() );
    	    assert( pPage.aData.getByteUnsigned(0)==(SqlJetMemPage.PTF_INTKEY|SqlJetMemPage.PTF_LEAFDATA|SqlJetMemPage.PTF_LEAF) );
    	    pNew.zeroPage(SqlJetMemPage.PTF_INTKEY|SqlJetMemPage.PTF_LEAFDATA|SqlJetMemPage.PTF_LEAF);
    	    pNew.assemblePage(1, new ISqlJetMemoryPointer[] { pCell }, 0, new int[] { szCell }, 0);

    	    /* If this is an auto-vacuum database, update the pointer map
    	    ** with entries for the new page, and any pointer from the
    	    ** cell on the page to an overflow page. If either of these
    	    ** operations fails, the return code is set, but the contents
    	    ** of the parent page are still manipulated by thh code below.
    	    ** That is Ok, at this point the parent page is guaranteed to
    	    ** be marked as dirty. Returning an error code will cause a
    	    ** rollback, undoing any changes made to the parent page.
    	    */
    	    if( pBt.autoVacuumMode.isAutoVacuum() ){
    	    	pBt.ptrmapPut(pgnoNew[0], SqlJetPtrMapType.PTRMAP_BTREE, pParent.pgno);
    	    	if( szCell>pNew.minLocal ){
    	    		pNew.ptrmapPutOvflPtr(pCell);
    	    	}
    	    }

    	    /* Create a divider cell to insert into pParent. The divider cell
    	    ** consists of a 4-byte page number (the page number of pPage) and
    	    ** a variable length key value (which must be the same value as the
    	    ** largest key on pPage).
    	    **
    	    ** To find the largest key value on pPage, first find the right-most
    	    ** cell on pPage. The first two fields of this cell are the
    	    ** record-length (a variable length integer at most 32-bits in size)
    	    ** and the key value (a variable length integer, may have any value).
    	    ** The first of the while(...) loops below skips over the record-length
    	    ** field. The second while(...) loop copies the key value from the
    	    ** cell on pPage into the pSpace buffer.
    	    */
    	    pCell = pPage.findCell(pPage.nCell-1);
    	    pStop = pCell.getMoved(9);
    	    //while( (*(pCell++)&0x80) && pCell<pStop );
    	    do{
    	    	boolean b = (pCell.getByteUnsigned()&0x80)==0;
    	    	pCell.movePointer(1);
				if(b) {
    	    		break;
    	    	}
    	    } while( pCell.getPointer()<pStop.getPointer() );
    	    pStop = pCell.getMoved(9);
    	    //while( ((*(pOut++) = *(pCell++))&0x80) && pCell<pStop );
    	    do{
    	    	pOut.putByteUnsigned(pCell.getByteUnsigned());
    	    	pOut.movePointer(1);
    	    	boolean b = (pCell.getByteUnsigned()&0x80)==0;
    	    	pCell.movePointer(1);
				if(b) {
    	    		break;
    	    	}
    	    } while( pCell.getPointer()<pStop.getPointer() );

    	    /* Insert the new divider cell into pParent. */
    	    pParent.insertCell( pParent.nCell, pSpace, pOut.getPointer()-pSpace.getPointer(),
    	               null, pPage.pgno);

    	    /* Set the right-child pointer of pParent to point to the new page. */
    	    pParent.aData.getMoved(pParent.getHdrOffset()+8).putIntUnsigned(0, pgnoNew[0]);

    	  } finally {
    	    /* Release the reference to the new page. */
    	    SqlJetMemPage.releasePage(pNew);
    	  }

    }

    /**
     * This function is called when the root page of a b-tree structure is
     * overfull (has one or more overflow pages).
     *
     * A new child page is allocated and the contents of the current root
     * page, including overflow cells, are copied into the child. The root
     * page is then overwritten to make it an empty page with the right-child
     * pointer pointing to the new page.
     *
     * Before returning, all pointer-map entries corresponding to pages
     * that the new child-page now contains pointers to are updated. The
     * entry corresponding to the new right-child pointer of the root
     * page is also updated.
     *
     * If successful, *ppChild is set to contain a reference to the child
     * page and SQLITE_OK is returned. In this case the caller is required
     * to call releasePage() on *ppChild exactly once. If an error occurs,
     * an error code is returned and *ppChild is set to 0.
     */
    private SqlJetMemPage balanceDeeper(SqlJetMemPage pRoot) throws SqlJetException {

    	  SqlJetMemPage pChild = null;           /* Pointer to a new child page */
    	  int[] pgnoChild = {0};            /* Page number of the new child page */
    	  SqlJetBtreeShared pBt = pRoot.pBt;    /* The BTree */

    	  assert( !pRoot.aOvfl.isEmpty() );
    	  assert( mutexHeld(pBt.mutex) );

    	  /* Make pRoot, the root page of the b-tree, writable. Allocate a new
    	  ** page that will become the new right-child of pPage. Copy the contents
    	  ** of the node stored on pRoot into the new child page.
    	  */
    	  pRoot.pDbPage.write();
    	  try{
    		  pChild = pBt.allocatePage(pgnoChild, pRoot.pgno, false);
    		  pRoot.copyNodeContent(pChild);
    	      if( pBt.autoVacuumMode.isAutoVacuum() ){
    	    	  pBt.ptrmapPut(pgnoChild[0], SqlJetPtrMapType.PTRMAP_BTREE, pRoot.pgno);
    	      }
    	  } catch(SqlJetException e) {
    	    SqlJetMemPage.releasePage(pChild);
    	    throw e;
    	  }
    	  assert( pChild.pDbPage.isWriteable() );
    	  assert( pRoot.pDbPage.isWriteable() );
    	  assert( pChild.nCell==pRoot.nCell );

    	  traceInt("BALANCE: copy root %d into %d\n", pRoot.pgno, pChild.pgno);

    	  /* Copy the overflow cells from pRoot to pChild */
    	  pChild.aOvfl = pRoot.aOvfl.clone();

    	  /* Zero the contents of pRoot. Then install pChild as the right-child. */
    	  pRoot.zeroPage(pChild.aData.getByteUnsigned(0) & ~SqlJetMemPage.PTF_LEAF);
    	  pRoot.aData.putIntUnsigned(pRoot.getHdrOffset()+8, pgnoChild[0]);

    	  return pChild;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#insert(byte[], long,
     * byte[], int, int, boolean)
     */
    @Override
	public void insert(ISqlJetMemoryPointer pKey, long nKey, ISqlJetMemoryPointer pData, int nData, int zero,
            boolean bias) throws SqlJetException {
        SqlJetBtreeShared pBt = this.pBtree.pBt;

        assert (holdsMutex());
        assert (pBt.inTransaction == TransMode.WRITE);
        assert (!pBt.readOnly);
        assert (this.wrFlag);
        /* The table pCur points to has a read lock */
        SqlJetAssert.assertFalse(pBtree.checkReadLocks(this.pgnoRoot, this, nKey), SqlJetErrorCode.LOCKED);
        SqlJetAssert.assertFalse(this.eState == SqlJetCursorState.FAULT, error);

        /*
         * Save the positions of any other cursors open on this table.
         *
         * In some cases, the call to sqlite3BtreeMoveto() below is a no-op. For
         * example, when inserting data into a table with auto-generated integer
         * keys, the VDBE layer invokes sqlite3BtreeLast() to figure out the
         * integer key to use. It then calls this function to actually insert
         * the data into the intkey B-Tree. In this case sqlite3BtreeMoveto()
         * recognizes that the cursor is already where it needs to be and
         * returns without doing any work. To avoid thwarting these
         * optimizations, it is important not to clear the cursor here.
         */
        pBt.saveAllCursors(this.pgnoRoot, this);
        int loc = this.moveTo(pKey, nKey, bias);
        assert (this.eState == SqlJetCursorState.VALID || (this.eState == SqlJetCursorState.INVALID && loc != 0));

        SqlJetMemPage pPage = getCurrentPage();
        assert (pPage.intKey || nKey >= 0);
        assert (pPage.leaf || !pPage.intKey);
        TRACE("INSERT: table=%d nkey=%d ndata=%b page=%d %s\n", Integer.valueOf(this.pgnoRoot), Long.valueOf(nKey), pData, Integer.valueOf(pPage.pgno),
                loc == 0 ? "overwrite" : "new entry");
        assert (pPage.isInit);
        ISqlJetMemoryPointer newCell = pBt.allocateTempSpace();
        int szNew = pPage.fillInCell(newCell, pKey, nKey, pData, nData, zero);
        assert (szNew == pPage.cellSizePtr(newCell));
        assert (szNew <= pBt.mxCellSize());
        int idx = getIndexOnCurrentPage();
        if (loc == 0 && SqlJetCursorState.VALID == this.eState) {
            assert (idx < pPage.nCell);
            pPage.pDbPage.write();
            ISqlJetMemoryPointer oldCell = pPage.findCell(idx);
            if (!pPage.leaf) {
            	newCell.copyFrom(oldCell, 4);
            }
            int szOld = pPage.cellSizePtr(oldCell);
            pPage.clearCell(oldCell);
            pPage.dropCell(idx, szOld);
        } else if (loc < 0 && pPage.nCell > 0) {
            assert (pPage.leaf);
            idx = getCurrentIndexedPage().incrIndex();
        } else {
            assert (pPage.leaf);
        }

        try {
            pPage.insertCell(idx, newCell, szNew, null, 0);

            assert (pPage.nCell > 0 || !pPage.aOvfl.isEmpty());

        } finally {
            /*
             * If no error has occured and pPage has an overflow cell, call
             * balance() to redistribute the cells within the tree. Since
             * balance() may move the cursor, zero the BtCursor.info.nSize and
             * BtCursor.validNKey variables.
             *
             * Previous versions of SQLite called moveToRoot() to move the
             * cursor back to the root page as balance() used to invalidate the
             * contents of BtCursor.apPage[] and BtCursor.aiIdx[]. Instead of
             * doing that, set the cursor state to "invalid". This makes common
             * insert operations slightly faster.
             *
             * There is a subtle but important optimization here too. When
             * inserting multiple records into an intkey b-tree using a single
             * cursor (as can happen while processing an
             * "INSERT INTO ... SELECT" statement), it is advantageous to leave
             * the cursor pointing to the last entry in the b-tree if possible.
             * If the cursor is left pointing to the last entry in the table,
             * and the next row inserted has an integer key larger than the
             * largest existing key, it is possible to insert the row without
             * seeking the cursor. This can be a big performance boost.
             */
            this.info.nSize = 0;
            this.validNKey = false;
        }

        if (!pPage.aOvfl.isEmpty()) {
            try {
                this.balance(true);
            } finally {
                /*
                 * Must make sure nOverflow is reset to zero even if the
                 * balance() fails. Internal data structure corruption will
                 * result otherwise. Also, set the cursor state to invalid. This
                 * stops saveCursorPosition() from trying to save the current
                 * position of the cursor.
                 */
                getCurrentPage().aOvfl.clear();
                this.eState = SqlJetCursorState.INVALID;
            }
        }
        assert (getCurrentPage().aOvfl.isEmpty());

    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#first()
     */
    @Override
	public boolean first() throws SqlJetException {
        assert (holdsMutex());
        assert (this.pBtree.db.getMutex().held());
        this.moveToRoot();
        if (this.eState == SqlJetCursorState.INVALID) {
            assert (getCurrentPage().nCell == 0);
            return true;
        } else {
            assert (getCurrentPage().nCell > 0);
            this.moveToLeftmost();
            return false;
        }
    }

    /**
     * Move the cursor down to the left-most leaf entry beneath the entry to
     * which it is currently pointing.
     *
     * The left-most leaf is the one with the smallest key - the first in
     * ascending order.
     *
     * @throws SqlJetException
     */
    private void moveToLeftmost() throws SqlJetException {
        SqlJetMemPage pPage;
        assert (holdsMutex());
        assert (this.eState == SqlJetCursorState.VALID);
        while (!(pPage = getCurrentPage()).leaf) {
            assert (getIndexOnCurrentPage() < pPage.nCell);
            int pgno = pPage.findCell(getIndexOnCurrentPage()).getInt();
            this.moveToChild(pgno);
        }
    }

    /**
     * Move the cursor down to the right-most leaf entry beneath the page to
     * which it is currently pointing. Notice the difference between
     * moveToLeftmost() and moveToRightmost(). moveToLeftmost() finds the
     * left-most entry beneath the *entry* whereas moveToRightmost() finds the
     * right-most entry beneath the *page*.
     *
     * The right-most entry is the one with the largest key - the last key in
     * ascending order.
     *
     * @throws SqlJetException
     */
    private void moveToRightmost() throws SqlJetException {
        SqlJetMemPage pPage = null;
        assert (holdsMutex());
        assert (this.eState == SqlJetCursorState.VALID);
        while (!(pPage = getCurrentPage()).leaf) {
            int pgno = pPage.aData.getInt(pPage.getHdrOffset() + 8);
            setIndexOnCurrentPage(pPage.nCell);
            this.moveToChild(pgno);
        }
        setIndexOnCurrentPage(pPage.nCell - 1);
        this.info.nSize = 0;
        this.validNKey = false;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#last()
     */
    @Override
	public boolean last() throws SqlJetException {
        assert (holdsMutex());
        assert (this.pBtree.db.getMutex().held());
        this.moveToRoot();
        if (SqlJetCursorState.INVALID == this.eState) {
            assert (getCurrentPage().nCell == 0);
            return true;
        }
        assert (this.eState == SqlJetCursorState.VALID);
        try {
            this.moveToRightmost();
        } catch (SqlJetException e) {
            this.atLast = false;
            throw e;
        } finally {
            this.getCellInfo();
        }
        this.atLast = true;
        return false;
    }

    @Override
	public boolean next() throws SqlJetException {
        assert (holdsMutex());
        this.restoreCursorPosition();
        if (SqlJetCursorState.INVALID == this.eState) {
            return true;
        }
        if (this.skip > 0) {
            this.skip = 0;
            return false;
        }
        this.skip = 0;

        SqlJetMemPage pPage = getCurrentPage();
        int idx = getCurrentIndexedPage().incrIndex();
        assert (pPage.isInit);
        assert (idx <= pPage.nCell);

        this.info.nSize = 0;
        this.validNKey = false;
        if (idx >= pPage.nCell) {
            if (!pPage.leaf) {
                this.moveToChild(pPage.aData.getInt(pPage.getHdrOffset() + 8));
                this.moveToLeftmost();
                return false;
            }
            do {
                if (this.iPage == 0) {
                    this.eState = SqlJetCursorState.INVALID;
                    return true;
                }
                this.moveToParent();
                pPage = getCurrentPage();
            } while (getIndexOnCurrentPage() >= pPage.nCell);
            if (pPage.intKey) {
                return this.next();
            }
            return false;
        }
        if (pPage.leaf) {
            return false;
        }
        this.moveToLeftmost();
        return false;
    }

    /**
     * Move the cursor up to the parent page.
     *
     * pCur->idx is set to the cell index that contains the pointer to the page
     * we are coming from. If we are coming from the right-most child page then
     * pCur->idx is set to one more than the largest cell index.
     *
     * @throws SqlJetException
     *
     */
    private void moveToParent() throws SqlJetException {
        assert (holdsMutex());
        assert (this.eState == SqlJetCursorState.VALID);
        assert (this.iPage > 0);
        SqlJetMemPage currentPage = popCurrentPage();
		assert (currentPage != null);
        getCurrentPage().assertParentIndex(getIndexOnCurrentPage(), currentPage.pgno);
        SqlJetMemPage.releasePage(currentPage);
        this.info.nSize = 0;
        this.validNKey = false;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#previous()
     */
    @Override
	public boolean previous() throws SqlJetException {
        assert (holdsMutex());
        this.restoreCursorPosition();
        this.atLast = false;
        if (SqlJetCursorState.INVALID == this.eState) {
            return true;
        }
        if (this.skip < 0) {
            this.skip = 0;
            return false;
        }
        this.skip = 0;

        SqlJetMemPage pPage = getCurrentPage();
        assert (pPage.isInit);
        if (!pPage.leaf) {
            int idx = getIndexOnCurrentPage();
            this.moveToChild(pPage.findCell(idx).getInt());
            this.moveToRightmost();
        } else {
            while (getIndexOnCurrentPage() == 0) {
                if (this.iPage == 0) {
                    this.eState = SqlJetCursorState.INVALID;
                    return true;
                }
                this.moveToParent();
            }
            this.info.nSize = 0;
            this.validNKey = false;

            getCurrentIndexedPage().decrIndex();
            pPage = getCurrentPage();
            if (pPage.intKey && !pPage.leaf) {
                return this.previous();
            }
        }
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#eof()
     */
    @Override
	public boolean eof() {
        /*
         * TODO: What if the cursor is in CURSOR_REQUIRESEEK but all table
         * entries* have been deleted? This API will need to change to return an
         * error code* as well as the boolean result value.
         */
        return (SqlJetCursorState.VALID != eState);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#flags()
     */
    @Override
	public int flags() throws SqlJetException {
        restoreCursorPosition();
        SqlJetMemPage pPage = getCurrentPage();
        assert (holdsMutex());
        assert (pPage != null);
        assert (pPage.pBt == this.pBt);
        return pPage.aData.getByteUnsigned(pPage.getHdrOffset());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#getKeySize()
     */
    @Override
	public long getKeySize() throws SqlJetException {
        assert (holdsMutex());
        this.restoreCursorPosition();
        assert (this.eState == SqlJetCursorState.INVALID || this.eState == SqlJetCursorState.VALID);
        if (this.eState == SqlJetCursorState.INVALID) {
            return 0;
        } else {
            this.getCellInfo();
            return this.info.getnKey();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#key(int, int, byte[])
     */
    @Override
	public void key(int offset, int amt, ISqlJetMemoryPointer buf) throws SqlJetException {
        assert (holdsMutex());
        this.restoreCursorPosition();
        assert (this.eState == SqlJetCursorState.VALID);
        assert (this.iPage >= 0 && getCurrentPage() != null);
        if (this.apPage[0].getPage().intKey) {
            throw new SqlJetException(SqlJetErrorCode.CORRUPT);
        }
        assert (getIndexOnCurrentPage() < getCurrentPage().nCell);
        this.accessPayload(offset, amt, buf, 0, false);
    }

    /**
     * This function is used to read or overwrite payload information for the
     * entry that the pCur cursor is pointing to. If the eOp parameter is 0,
     * this is a read operation (data copied into buffer pBuf). If it is
     * non-zero, a write (data copied from buffer pBuf).
     *
     * A total of "amt" bytes are read or written beginning at "offset". Data is
     * read to or from the buffer pBuf.
     *
     * This routine does not make a distinction between key and data. It just
     * reads or writes bytes from the payload area. Data might appear on the
     * main page or be scattered out on multiple overflow pages.
     *
     * If the BtCursor.isIncrblobHandle flag is set, and the current cursor
     * entry uses one or more overflow pages, this function allocates space for
     * and lazily popluates the overflow page-list cache array
     * (BtCursor.aOverflow). Subsequent calls use this cache to make seeking to
     * the supplied offset more efficient.
     *
     * Once an overflow page-list cache has been allocated, it may be
     * invalidated if some other cursor writes to the same table, or if the
     * cursor is moved to a different row. Additionally, in auto-vacuum mode,
     * the following events may invalidate an overflow page-list cache.
     *
     * <ul>
     * <li>An incremental vacuum</li>
     * <li>A commit in auto_vacuum="full" mode</li>
     * <li>Creating a table (may require moving an overflow page)</li>
     * </ul>
     *
     *
     * @param offset
     *            Begin reading this far into payload
     * @param amt
     *            Read this many bytes
     * @param buf
     *            Write the bytes into this buffer
     * @param skipKey
     *            offset begins at data if this is true
     * @param eOp
     *            false to read. true to write
     *
     * @throws SqlJetException
     */
    private void accessPayload(int offset, int amt, ISqlJetMemoryPointer pBuf, int skipKey, boolean eOp)
            throws SqlJetException {
        pBuf = SqlJetUtility.pointer(pBuf);

        int iIdx = 0;
        /* Btree page of current entry */
        SqlJetMemPage pPage = getCurrentPage();

        assert (pPage != null);
        assert (this.eState == SqlJetCursorState.VALID);
        assert (getIndexOnCurrentPage() < pPage.nCell);
        assert (holdsMutex());

        this.getCellInfo();
        ISqlJetMemoryPointer aPayload = this.info.pCell.pointer(this.info.nHeader);
        int nKey = (pPage.intKey ? 0 : (int) this.info.getnKey());

        if (skipKey != 0) {
            offset += nKey;
        }
        if (offset + amt > nKey + this.info.nData
                || (aPayload.getPointer() + this.info.nLocal) > (pPage.aData.getPointer() + pBt.usableSize)) {
            /* Trying to read or write past the end of the data is an error */
            throw new SqlJetException(SqlJetErrorCode.CORRUPT);
        }

        /* Check if data must be read/written to/from the btree page itself. */
        if (offset < this.info.nLocal) {
            int a = amt;
            if (a + offset > this.info.nLocal) {
                a = this.info.nLocal - offset;
            }
            copyPayload(aPayload, offset, pBuf, 0, a, eOp, pPage.pDbPage);
            offset = 0;
            pBuf.movePointer(a);
            amt -= a;
        } else {
            offset -= this.info.nLocal;
        }

        if (amt > 0) {
            int ovflSize = pBt.usableSize - 4; /* Bytes content per ovfl page */
            int nextPage;

            nextPage = aPayload.getInt(info.nLocal);

            /*
             * If the isIncrblobHandle flag is set and the BtCursor.aOverflow[]*
             * has not been allocated, allocate it now. The array is sized at*
             * one entry for each overflow page in the overflow chain. The* page
             * number of the first overflow page is stored in aOverflow[0],*
             * etc. A value of 0 in the aOverflow[] array means "not yet known"*
             * (the cache is lazily populated).
             */
            if (this.isIncrblobHandle && this.aOverflow == null) {
                int nOvfl = (this.info.nPayload - this.info.nLocal + ovflSize - 1) / ovflSize;
                this.aOverflow = new int[nOvfl];
            }

            /*
             * If the overflow page-list cache has been allocated and the* entry
             * for the first required overflow page is valid, skip* directly to
             * it.
             */
            if (this.aOverflow != null && this.aOverflow[offset / ovflSize] != 0) {
                iIdx = (offset / ovflSize);
                nextPage = this.aOverflow[iIdx];
                offset = (offset % ovflSize);
            }

            for (; amt > 0 && nextPage != 0; iIdx++) {

                /* If required, populate the overflow page-list cache. */
                if (this.aOverflow != null) {
                    assert (this.aOverflow[iIdx] == 0 || this.aOverflow[iIdx] == nextPage);
                    this.aOverflow[iIdx] = nextPage;
                }

                if (offset >= ovflSize) {
                    /*
                     * The only reason to read this page is to obtain the page*
                     * number for the next page in the overflow chain. The page*
                     * data is not required. So first try to lookup the overflow
                     * * page-list cache, if any, then fall back to the
                     * getOverflowPage()* function.
                     */
                    if (this.aOverflow != null && this.aOverflow[iIdx + 1] != 0) {
                        nextPage = this.aOverflow[iIdx + 1];
                    } else {
                        nextPage = pBt.getOverflowPage(nextPage, null, nextPage);
                    }
                    offset -= ovflSize;
                } else {
                    /*
                     * Need to read this page properly. It contains some of the*
                     * range of data that is being read (eOp==0) or written
                     * (eOp!=0).
                     */
                    ISqlJetPage pDbPage;
                    int a = amt;
                    pDbPage = pBt.pPager.getPage(nextPage);
                    aPayload = pDbPage.getData();
                    nextPage = aPayload.getInt();
                    if (a + offset > ovflSize) {
                        a = ovflSize - offset;
                    }
                    copyPayload(aPayload, offset + 4, pBuf, 0, a, eOp, pDbPage);
                    pDbPage.unref();
                    offset = 0;
                    amt -= a;
                    pBuf.movePointer(a);
                }
            }
        }

        if (amt > 0) {
            throw new SqlJetException(SqlJetErrorCode.CORRUPT);
        }

    }

    /**
     * Copy data from a buffer to a page, or from a page to a buffer.
     *
     * pPayload is a pointer to data stored on database page pDbPage. If
     * argument eOp is false, then nByte bytes of data are copied from pPayload
     * to the buffer pointed at by pBuf. If eOp is true, then
     * sqlite3PagerWrite() is called on pDbPage and nByte bytes of data are
     * copied from the buffer pBuf to pPayload.
     *
     * @param pPayload
     *            Pointer to page data
     * @param pBuf
     *            Pointer to buffer
     * @param nByte
     *            Number of bytes to copy
     * @param eOp
     *            false -> copy from page, true -> copy to page
     * @param pDbPage
     *            Page containing pPayload
     *
     * @throws SqlJetException
     */
    private void copyPayload(ISqlJetMemoryPointer pPayload, int payloadOffset, ISqlJetMemoryPointer pBuf,
            int bufOffset, int nByte, boolean eOp, ISqlJetPage pDbPage) throws SqlJetException {
        if (eOp) {
            /* Copy data from buffer to page (a write operation) */
            pDbPage.write();
            pPayload.copyFrom(payloadOffset, pBuf, bufOffset, nByte);
        } else {
            /* Copy data from page to buffer (a read operation) */
        	pBuf.copyFrom(bufOffset, pPayload, payloadOffset, nByte);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#getCursorDb()
     */
    @Override
	public ISqlJetDbHandle getCursorDb() {
        assert (mutexHeld(pBtree.db.getMutex()));
        return pBtree.db;
    }

    @Override
	public ISqlJetMemoryPointer keyFetch(int[] amt) {
        assert (holdsMutex());
        if (eState == SqlJetCursorState.VALID) {
            return fetchPayload(amt, false);
        }
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#dataFetch(int[])
     */
    @Override
	public ISqlJetMemoryPointer dataFetch(int[] amt) {
        assert (holdsMutex());
        if (eState == SqlJetCursorState.VALID) {
            return fetchPayload(amt, true);
        }
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#getDataSize()
     */
    @Override
	public int getDataSize() throws SqlJetException {
        assert (holdsMutex());
        restoreCursorPosition();
        assert (eState == SqlJetCursorState.INVALID || eState == SqlJetCursorState.VALID);
        if (eState == SqlJetCursorState.INVALID) {
            /* Not pointing at a valid entry - set *pSize to 0. */
            return 0;
        } else {
            getCellInfo();
            return info.nData;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#data(int, int, byte[])
     */
    @Override
	public void data(int offset, int amt, ISqlJetMemoryPointer buf) throws SqlJetException {
    	SqlJetAssert.assertFalse(eState == SqlJetCursorState.INVALID, SqlJetErrorCode.ABORT);

        assert (holdsMutex());
        restoreCursorPosition();
        assert (eState == SqlJetCursorState.VALID);
        assert (iPage >= 0 && getCurrentPage() != null);
        assert (getIndexOnCurrentPage() < getCurrentPage().nCell);
        accessPayload(offset, amt, buf, 1, false);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#putData(int, int,
     * byte[])
     */
    @Override
	public void putData(int offset, int amt, ISqlJetMemoryPointer data) throws SqlJetException {

        assert (holdsMutex());
        assert (mutexHeld(this.pBtree.db.getMutex()));
        assert (this.isIncrblobHandle);

        restoreCursorPosition();
        assert (this.eState != SqlJetCursorState.REQUIRESEEK);
        SqlJetAssert.assertTrue(this.eState == SqlJetCursorState.VALID, SqlJetErrorCode.ABORT);

        /*
         * Check some preconditions:* (a) the cursor is open for writing,* (b)
         * there is no read-lock on the table being modified and* (c) the cursor
         * points at a valid row of an intKey table.
         */
        SqlJetAssert.assertTrue(wrFlag, SqlJetErrorCode.READONLY);
        assert (!pBt.readOnly && pBt.inTransaction == TransMode.WRITE);
        SqlJetAssert.assertFalse(pBtree.checkReadLocks(pgnoRoot, this, 0), SqlJetErrorCode.LOCKED);
        if (!getCurrentPage().intKey) {
            throw new SqlJetException(SqlJetErrorCode.ERROR);
        }

        accessPayload(offset, amt, data, 0, true);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#cacheOverflow()
     */
    @Override
	public void cacheOverflow() {
        assert (holdsMutex());
        assert (mutexHeld(pBtree.db.getMutex()));
        assert (!isIncrblobHandle);
        assert (aOverflow == null);
        isIncrblobHandle = true;
    }

    /**
     * Save the current cursor position in the variables BtCursor.nKey and
     * BtCursor.pKey. The cursor's state is set to CURSOR_REQUIRESEEK.
     *
     */
    @Override
	public void saveCursorPosition() throws SqlJetException {
        assert (SqlJetCursorState.VALID == this.eState);
        assert (null == this.pKey);
        assert (holdsMutex());

        try {
            this.nKey = this.getKeySize();

            /*
             * If this is an intKey table, then the above call to BtreeKeySize()
             * * stores the integer key in pCur->nKey. In this case this value
             * is* all that is required. Otherwise, if pCur is not open on an
             * intKey* table, then malloc space for and store the pCur->nKey
             * bytes of key* data.
             */
            if (!this.apPage[0].getPage().intKey) {
                ISqlJetMemoryPointer pKey = SqlJetUtility.memoryManager.allocatePtr((int) this.nKey);
                this.key(0, (int) this.nKey, pKey);
                this.pKey = pKey;
            }
            assert (!this.apPage[0].getPage().intKey || this.pKey == null);

            clearAllPages();
            this.eState = SqlJetCursorState.REQUIRESEEK;
        } finally {
            this.invalidateOverflowCache();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#enterCursor()
     */
    @Override
	public void enterCursor() {
        if(pBtree!=null) {
            pBtree.enter();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#leaveCursor()
     */
    @Override
	public void leaveCursor() {
        if(pBtree!=null) {
            pBtree.leave();
        }
    }

    public List<SqlJetMemPage> getAllPages() {
    	List<SqlJetMemPage> result = new ArrayList<>();
        for (int i = 0; i <= iPage; i++) {
			result.add(apPage[i].getPage());
		}
        return result;
    }
    
    public void clearAllPages() {
        for (int i = 0; i <= iPage; i++) {
        	apPage[i] = null;
        }
    	iPage = -1;
    }
    
    private IndexedMemPage getCurrentIndexedPage() {
    	return apPage[iPage];
    }
    
    private SqlJetMemPage getCurrentPage() {
    	return getCurrentIndexedPage().getPage();
    }
    
    private SqlJetMemPage popCurrentPage() {
    	SqlJetMemPage currentPage = getCurrentIndexedPage().getPage();
    	iPage--;
		return currentPage;
    }
    
    private int getIndexOnCurrentPage() {
    	return getCurrentIndexedPage().getIndex();
    }
    
    private void setIndexOnCurrentPage(int index) {
    	apPage[iPage].setIndex(index);
    }
    
    private void addNewPage(SqlJetMemPage pPage) {
    	iPage++;
		this.apPage[iPage] = new IndexedMemPage(pPage);
    }
}
