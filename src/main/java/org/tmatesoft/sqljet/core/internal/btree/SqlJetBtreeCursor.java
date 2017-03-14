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

import static org.tmatesoft.sqljet.core.internal.btree.SqlJetBtree.TRACE;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtreeCursor;
import org.tmatesoft.sqljet.core.internal.ISqlJetDbHandle;
import org.tmatesoft.sqljet.core.internal.ISqlJetKeyInfo;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetPage;
import org.tmatesoft.sqljet.core.internal.ISqlJetUnpackedRecord;
import org.tmatesoft.sqljet.core.internal.SqlJetAssert;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.internal.btree.SqlJetBtree.TransMode;
import org.tmatesoft.sqljet.core.internal.vdbe.SqlJetUnpackedRecord;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class SqlJetBtreeCursor implements ISqlJetBtreeCursor {
    /** The Btree to which this cursor belongs */
    private final SqlJetBtree pBtree;

    /** Argument passed to comparison function */
    private final ISqlJetKeyInfo pKeyInfo;

    /** The root page of this tree */
    protected final int pgnoRoot;

    /** A parse of the cell we are pointing at */
    protected SqlJetBtreeCellInfo info = new SqlJetBtreeCellInfo(null, 0, 0, 0, 0, 0, 0);

    /** True if writable */
    private final boolean wrFlag;

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
    
    private final SqlJetIndexedMemPages pages;

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
        if (wrFlag) {
            SqlJetAssert.assertFalse(btree.isReadOnly(), SqlJetErrorCode.READONLY);
        }

        SqlJetBtreeShared pBt = btree.pBt;
        if (pBt.pPage1 == null) {
            btree.lockWithRetry();
        }
        this.pBtree = btree;
        this.pgnoRoot = table;
        int nPage = pBt.pPager.getPageCount();
        this.pages = new SqlJetIndexedMemPages(pBtree.pBt.usableSize * 2 / 3, pBtree.pBt.getPageSize());
        try {
        	SqlJetAssert.assertFalse(table == 1 && nPage == 0, SqlJetErrorCode.EMPTY);
        	pages.addNewPage(pBt.getAndInitPage(pgnoRoot));
        } catch (SqlJetException e) {
        	// create_cursor_exception:
        	pBtree.unlockBtreeIfUnused();
        	throw e;
        }

        /*
         * Now that no other errors can occur, finish filling in the
         * BtCursor* variables, link the cursor into the BtShared list and
         * set *ppCur (the* output argument to this function).
         */
        this.pKeyInfo = keyInfo;
        this.wrFlag = wrFlag;
        pBtree.cursors.add(this);
        this.eState = SqlJetCursorState.INVALID;
    }

    @Override
	public void clearCursor() {
        pKey = null;
        eState = SqlJetCursorState.INVALID;
    }

    @Override
	public void closeCursor() throws SqlJetException {
        clearCursor();
        pBtree.cursors.remove(this);
        pages.releaseAllPages();
        pBtree.unlockBtreeIfUnused();
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
            assert nKey == (int) nKey;
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
        SqlJetAssert.assertNoError(error);
        if (eState == SqlJetCursorState.REQUIRESEEK) {
            this.clearCursor();
        }

        if (pages.hasCurrentPage()) {
        	pages.releaseAfter(0);
        } else {
            try {
            	pages.addNewPage(pBtree.pBt.getAndInitPage(this.pgnoRoot));
            } catch (SqlJetException e) {
                this.eState = SqlJetCursorState.INVALID;
                throw e;
            }
        }

        SqlJetMemPage pRoot = pages.getCurrentPage();
        assert pRoot.pgno == this.pgnoRoot;
        pages.setIndexOnCurrentPage(0);
        this.info.nSize = 0;
        this.atLast = false;
        this.validNKey = false;

        if (pRoot.nCell == 0 && !pRoot.leaf) {
            assert pRoot.pgno == 1;
            int subpage = pRoot.getData().getInt(pRoot.getHdrOffset() + 8);
            assert subpage > 0;
            this.eState = SqlJetCursorState.VALID;
            moveToChild(subpage);
        } else {
            this.eState = pRoot.nCell > 0 ? SqlJetCursorState.VALID : SqlJetCursorState.INVALID;
        }

    }

    /**
     * Move the cursor down to a new child page. The newPgno argument is the
     * page number of the child page to move to.
     */
    private void moveToChild(int newPgno) throws SqlJetException {
        assert this.eState.isValid();
        SqlJetMemPage pNewPage = pBtree.pBt.getAndInitPage(newPgno);
        pages.addNewPage(pNewPage);

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
            this.info = pages.getCurrentPage().parseCell(pages.getIndexOnCurrentPage());
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
    private @Nonnull ISqlJetMemoryPointer fetchPayload(int[] pAmt, boolean skipKey) {
        int nLocal;

        assert pages.hasCurrentPage();
        assert this.eState.isValid();
        SqlJetMemPage pPage = pages.getCurrentPage();
        assert pages.getIndexOnCurrentPage() < pPage.nCell;
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
        assert pBtree.db.getMutex().held();

        /*
         * If the cursor is already positioned at the point we are trying to
         * move to, then just return without doing any work
         */
        if (this.eState.isValid() && this.validNKey && pages.getFirstPage().intKey) {
            if (this.info.getnKey() == intKey) {
                return 0;
            }
            if (this.atLast && this.info.getnKey() < intKey) {
                return -1;
            }
        }

        moveToRoot();

        assert pages.getCurrentPage() != null;
        assert pages.getCurrentPage().isInit;
        if (this.eState.isInvalid()) {
            assert pages.getCurrentPage().nCell == 0;
            return -1;
        }
        assert pages.getFirstPage().intKey || pIdxKey != null;
        while (true) {
            SqlJetMemPage pPage = pages.getCurrentPage();
            int c = -1; /* pRes return if table is empty must be -1 */
            int lwr = 0;
            int upr = pPage.nCell - 1;
            SqlJetAssert.assertTrue(upr >= 0, SqlJetErrorCode.CORRUPT);
            if (biasRight) {
            	pages.setIndexOnCurrentPage(upr);
            } else {
            	pages.setIndexOnCurrentPage((upr + lwr) / 2);
            }
            while (true) {
                long key = 0;
                int idx = pages.getIndexOnCurrentPage();
                this.info.nSize = 0;
                this.validNKey = true;
                if (pPage.intKey) {
                    ISqlJetMemoryPointer pCell = pPage.findCell(idx).pointer(pPage.getChildPtrSize());
                    if (pPage.hasData) {
                        pCell.movePointer(pCell.getVarint32().getOffset());
                    }
                    key = pCell.getVarint().getValue();
                    c = Long.compare(key, intKey);
                } else if (pIdxKey == null) {
                	throw new SqlJetException(SqlJetErrorCode.CORRUPT);
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
                pages.setIndexOnCurrentPage((lwr + upr) / 2);
            }
            assert lwr == upr + 1;
            assert pPage.isInit;
            int chldPg;
            if (pPage.leaf) {
                chldPg = 0;
            } else if (lwr >= pPage.nCell) {
                chldPg = pPage.getData().getInt(pPage.getHdrOffset() + 8);
            } else {
                chldPg = pPage.findCell(lwr).getInt();
            }
            if (chldPg == 0) {
                assert pages.getIndexOnCurrentPage() < pages.getCurrentPage().nCell;
                return c;
            }
            pages.setIndexOnCurrentPage(lwr);
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
        if (this.eState.isValidOrInvalid()) {
			return;
		}
        SqlJetAssert.assertNoError(error);
        
        this.eState = SqlJetCursorState.INVALID;
        this.skip = this.moveTo(this.pKey, this.nKey, false);
        this.pKey = null;
        assert this.eState.isValidOrInvalid();
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
        return !eState.isValid() || skip != 0;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#delete()
     */
	@Override
	public void delete() throws SqlJetException {
		SqlJetBtreeShared pBt = pBtree.pBt;

		assert pBtree.inTrans == TransMode.WRITE;
		assert !pBtree.isReadOnly();
		assert this.wrFlag;

		// assert( hasSharedCacheTableLock(p, pCur->pgnoRoot, pCur->pKeyInfo!=0,
		// 2) );
		// assert( !hasReadConflicts(p, pCur->pgnoRoot) );

		if (pages.getIndexOnCurrentPage() >= pages.getCurrentPage().nCell || !this.eState.isValid()) {
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

		/*
		 * Save the positions of any other cursors open on this table before
		 ** making any modifications. Make the page containing the entry to be
		 ** deleted writable. Then free any overflow pages associated with the
		 ** entry and finally remove the cell itself from within the page.
		 */
		pBtree.cursors.saveAllCursors(this.pgnoRoot, this);

		/* Depth of node containing pCell */
		final int iCellDepth = pages.getNumberOfPages();
		/* Index of cell to delete */
		int iCellIdx = pages.getIndexOnCurrentPage();
		/* Page to delete cell from */
		SqlJetMemPage pPage = pages.getCurrentPage();
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
			SqlJetMemPage pLeaf = pages.getCurrentPage();
			int n = pages.getPage(iCellDepth).pgno;

			pCell = pLeaf.findCell(pLeaf.nCell - 1);
			int nCell = pLeaf.cellSizePtr(pCell);
			assert pBt.mxCellSize() >= nCell;

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
		pages.balance(false);
		if (pages.releaseAfter(iCellDepth-1)) {
			pages.balance(false);
		}
		this.moveToRoot();
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

        assert pBtree.inTrans == TransMode.WRITE;
        assert !pBtree.isReadOnly();
        assert this.wrFlag;
        /* The table pCur points to has a read lock */
        SqlJetAssert.assertNoError(error);

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
        pBtree.cursors.saveAllCursors(this.pgnoRoot, this);
        int loc = this.moveTo(pKey, nKey, bias);
        assert this.eState.isValid() || this.eState.isInvalid() && loc != 0;

        SqlJetMemPage pPage = pages.getCurrentPage();
        assert pPage.intKey || nKey >= 0;
        assert pPage.leaf || !pPage.intKey;
        TRACE("INSERT: table=%d nkey=%d ndata=%b page=%d %s\n", Integer.valueOf(this.pgnoRoot), Long.valueOf(nKey), pData, Integer.valueOf(pPage.pgno),
                loc == 0 ? "overwrite" : "new entry");
        assert pPage.isInit;
        ISqlJetMemoryPointer newCell = pBt.allocateTempSpace();
        int szNew = pPage.fillInCell(newCell, pKey, nKey, pData, nData, zero);
        assert szNew == pPage.cellSizePtr(newCell);
        assert szNew <= pBt.mxCellSize();
        int idx = pages.getIndexOnCurrentPage();
        if (loc == 0 && eState.isValid()) {
            assert idx < pPage.nCell;
            pPage.pDbPage.write();
            ISqlJetMemoryPointer oldCell = pPage.findCell(idx);
            if (!pPage.leaf) {
            	newCell.copyFrom(oldCell, 4);
            }
            int szOld = pPage.cellSizePtr(oldCell);
            pPage.clearCell(oldCell);
            pPage.dropCell(idx, szOld);
        } else if (loc < 0 && pPage.nCell > 0) {
            assert pPage.leaf;
            idx = pages.getCurrentIndexedPage().incrIndex();
        } else {
            assert pPage.leaf;
        }

        try {
            pPage.insertCell(idx, newCell, szNew, null, 0);

            assert pPage.nCell > 0 || !pPage.aOvfl.isEmpty();

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
        		pages.balance(true);
            } finally {
                /*
                 * Must make sure nOverflow is reset to zero even if the
                 * balance() fails. Internal data structure corruption will
                 * result otherwise. Also, set the cursor state to invalid. This
                 * stops saveCursorPosition() from trying to save the current
                 * position of the cursor.
                 */
            	pages.getCurrentPage().aOvfl.clear();
                this.eState = SqlJetCursorState.INVALID;
            }
        }
        assert pages.getCurrentPage().aOvfl.isEmpty();

    }

    /*
     * (non-Javadoc)
     *
     * @see org.tmatesoft.sqljet.core.ISqlJetBtreeCursor#first()
     */
    @Override
	public boolean first() throws SqlJetException {
        assert this.pBtree.db.getMutex().held();
        this.moveToRoot();
        if (this.eState.isInvalid()) {
            assert pages.getCurrentPage().nCell == 0;
            return true;
        } else {
            assert pages.getCurrentPage().nCell > 0;
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
        assert this.eState.isValid();
        while (!(pPage = pages.getCurrentPage()).leaf) {
            assert pages.getIndexOnCurrentPage() < pPage.nCell;
            int pgno = pPage.findCell(pages.getIndexOnCurrentPage()).getInt();
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
        assert this.eState.isValid();
        while (!(pPage = pages.getCurrentPage()).leaf) {
            int pgno = pPage.getData().getInt(pPage.getHdrOffset() + 8);
            pages.setIndexOnCurrentPage(pPage.nCell);
            this.moveToChild(pgno);
        }
        pages.setIndexOnCurrentPage(pPage.nCell - 1);
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
        assert this.pBtree.db.getMutex().held();
        this.moveToRoot();
        if (this.eState.isInvalid()) {
            assert pages.getCurrentPage().nCell == 0;
            return true;
        }
        assert this.eState.isValid();
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
        this.restoreCursorPosition();
        if (this.eState.isInvalid()) {
            return true;
        }
        if (this.skip > 0) {
            this.skip = 0;
            return false;
        }
        this.skip = 0;

        SqlJetMemPage pPage = pages.getCurrentPage();
        int idx = pages.getCurrentIndexedPage().incrIndex();
        assert pPage.isInit;
        assert idx <= pPage.nCell;

        this.info.nSize = 0;
        this.validNKey = false;
        if (idx >= pPage.nCell) {
            if (!pPage.leaf) {
                this.moveToChild(pPage.getData().getInt(pPage.getHdrOffset() + 8));
                this.moveToLeftmost();
                return false;
            }
            do {
                if (pages.hasExactlyOnePage()) {
                    this.eState = SqlJetCursorState.INVALID;
                    return true;
                }
                this.moveToParent();
                pPage = pages.getCurrentPage();
            } while (pages.getIndexOnCurrentPage() >= pPage.nCell);
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
        assert this.eState.isValid();
        assert pages.getNumberOfPages() > 1;
        SqlJetMemPage currentPage = pages.popCurrentPage();
		assert currentPage != null;
		pages.getCurrentPage().assertParentIndex(pages.getIndexOnCurrentPage(), currentPage.pgno);
        SqlJetMemPage.releasePage(currentPage);
        this.info.nSize = 0;
        this.validNKey = false;
    }

    @Override
	public boolean previous() throws SqlJetException {
        this.restoreCursorPosition();
        this.atLast = false;
        if (this.eState.isInvalid()) {
            return true;
        }
        if (this.skip < 0) {
            this.skip = 0;
            return false;
        }
        this.skip = 0;

        SqlJetMemPage pPage = pages.getCurrentPage();
        assert pPage.isInit;
        if (!pPage.leaf) {
            int idx = pages.getIndexOnCurrentPage();
            this.moveToChild(pPage.findCell(idx).getInt());
            this.moveToRightmost();
        } else {
            while (pages.getIndexOnCurrentPage() == 0) {
                if (pages.hasExactlyOnePage()) {
                    this.eState = SqlJetCursorState.INVALID;
                    return true;
                }
                this.moveToParent();
            }
            this.info.nSize = 0;
            this.validNKey = false;

            pages.getCurrentIndexedPage().decrIndex();
            pPage = pages.getCurrentPage();
            if (pPage.intKey && !pPage.leaf) {
                return this.previous();
            }
        }
        return false;
    }

    @Override
	public boolean eof() {
        /*
         * TODO: What if the cursor is in CURSOR_REQUIRESEEK but all table
         * entries* have been deleted? This API will need to change to return an
         * error code* as well as the boolean result value.
         */
        return !eState.isValid();
    }

    @Override
	public int flags() throws SqlJetException {
        restoreCursorPosition();
        SqlJetMemPage pPage = pages.getCurrentPage();
        assert pPage != null;
        assert pPage.pBt == pBtree.pBt;
        return pPage.getData().getByteUnsigned(pPage.getHdrOffset());
    }

    @Override
	public long getKeySize() throws SqlJetException {
        this.restoreCursorPosition();
        assert this.eState.isValidOrInvalid();
        if (this.eState.isInvalid()) {
            return 0;
        } else {
            this.getCellInfo();
            return this.info.getnKey();
        }
    }

    @Override
	public void key(int offset, int amt, @Nonnull ISqlJetMemoryPointer buf) throws SqlJetException {
        this.restoreCursorPosition();
        assert eState.isValid();
        assert pages.hasCurrentPage();
        
        SqlJetAssert.assertFalse(pages.getFirstPage().intKey, SqlJetErrorCode.CORRUPT);
        
        assert pages.getIndexOnCurrentPage() < pages.getCurrentPage().nCell;
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
    private void accessPayload(int offset, int amt, @Nonnull ISqlJetMemoryPointer pBuf, int skipKey, boolean eOp)
            throws SqlJetException {
        pBuf = SqlJetUtility.pointer(pBuf);

        /* Btree page of current entry */
        SqlJetMemPage pPage = pages.getCurrentPage();

        assert this.eState.isValid();
        assert pages.getIndexOnCurrentPage() < pPage.nCell;

        this.getCellInfo();
        ISqlJetMemoryPointer aPayload = this.info.pCell.pointer(this.info.nHeader);
        int nKey = pPage.intKey ? 0 : (int) this.info.getnKey();

        if (skipKey != 0) {
            offset += nKey;
        }
        if (offset + amt > nKey + this.info.nData
                || aPayload.getPointer() + this.info.nLocal > pPage.getData().getPointer() + pBtree.pBt.usableSize) {
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
            int ovflSize = pBtree.pBt.usableSize - 4; /* Bytes content per ovfl page */
            int nextPage;

            nextPage = aPayload.getInt(info.nLocal);

            while (amt > 0 && nextPage != 0) {
                if (offset >= ovflSize) {
                    /*
                     * The only reason to read this page is to obtain the page*
                     * number for the next page in the overflow chain. The page*
                     * data is not required. So first try to lookup the overflow
                     * * page-list cache, if any, then fall back to the
                     * getOverflowPage()* function.
                     */
                    nextPage = pBtree.pBt.getOverflowPage(nextPage, null, nextPage);
                    offset -= ovflSize;
                } else {
                    /*
                     * Need to read this page properly. It contains some of the*
                     * range of data that is being read (eOp==0) or written
                     * (eOp!=0).
                     */
                    int a = amt;
                    ISqlJetPage pDbPage = pBtree.pBt.pPager.getPage(nextPage);
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

    @Override
	public ISqlJetDbHandle getCursorDb() {
        assert pBtree.db.getMutex().held();
        return pBtree.db;
    }

    @Override
	public @Nonnull ISqlJetMemoryPointer keyFetch(int[] amt) throws SqlJetException {
    	assertIsValid();
        return fetchPayload(amt, false);
    }

    @Override
	public @Nonnull ISqlJetMemoryPointer dataFetch(int[] amt) throws SqlJetException {
    	assertIsValid();
        return fetchPayload(amt, true);
    }

    @Override
	public int getDataSize() throws SqlJetException {
        restoreCursorPosition();
        assert eState.isValidOrInvalid();
        if (eState.isInvalid()) {
            /* Not pointing at a valid entry - set *pSize to 0. */
            return 0;
        } else {
            getCellInfo();
            return info.nData;
        }
    }

    @Override
	public void data(int offset, int amt, @Nonnull ISqlJetMemoryPointer buf) throws SqlJetException {
    	SqlJetAssert.assertFalse(eState.isInvalid(), SqlJetErrorCode.ABORT);

        restoreCursorPosition();
        assert eState.isValid();
        assert pages.hasCurrentPage();
        assert pages.getIndexOnCurrentPage() < pages.getCurrentPage().nCell;
        accessPayload(offset, amt, buf, 1, false);
    }

    @Override
	public void putData(int offset, int amt, @Nonnull ISqlJetMemoryPointer data) throws SqlJetException {

        assert this.pBtree.db.getMutex().held();

        restoreCursorPosition();
        SqlJetAssert.assertTrue(eState.isValid(), SqlJetErrorCode.ABORT);

        /*
         * Check some preconditions:* (a) the cursor is open for writing,* (b)
         * there is no read-lock on the table being modified and* (c) the cursor
         * points at a valid row of an intKey table.
         */
        SqlJetAssert.assertTrue(wrFlag, SqlJetErrorCode.READONLY);
        assert !pBtree.isReadOnly() && pBtree.inTrans == TransMode.WRITE;
        SqlJetAssert.assertTrue(pages.getCurrentPage().intKey, SqlJetErrorCode.ERROR);

        accessPayload(offset, amt, data, 0, true);
    }

    /**
     * Save the current cursor position in the variables BtCursor.nKey and
     * BtCursor.pKey. The cursor's state is set to CURSOR_REQUIRESEEK.
     *
     */
    @Override
	public void saveCursorPosition() throws SqlJetException {
        assert this.eState.isValid();
        assert null == this.pKey;

        this.nKey = this.getKeySize();

        /*
         * If this is an intKey table, then the above call to BtreeKeySize()
         * * stores the integer key in pCur->nKey. In this case this value
         * is* all that is required. Otherwise, if pCur is not open on an
         * intKey* table, then malloc space for and store the pCur->nKey
         * bytes of key* data.
         */
        if (!pages.getFirstPage().intKey) {
            ISqlJetMemoryPointer pKey = SqlJetUtility.memoryManager.allocatePtr((int) this.nKey);
            this.key(0, (int) this.nKey, pKey);
            this.pKey = pKey;
        }
        assert !pages.getFirstPage().intKey || this.pKey == null;

        pages.clearAllPages();
        this.eState = SqlJetCursorState.REQUIRESEEK;
    }
    
    public void releaseAllPages() throws SqlJetException {
    	pages.releaseAllPages();
    	pages.clearAllPages();
    }
    
    private void assertIsValid() throws SqlJetException {
    	SqlJetAssert.assertTrue(eState.isValid(), SqlJetErrorCode.MISUSE, "The cursor is in an invalid state!");
    }
}
