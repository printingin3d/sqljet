package org.tmatesoft.sqljet.core.internal.btree;

import static org.tmatesoft.sqljet.core.internal.SqlJetUtility.memcpy;
import static org.tmatesoft.sqljet.core.internal.btree.SqlJetBtree.TRACE;
import static org.tmatesoft.sqljet.core.internal.btree.SqlJetBtree.traceInt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtreeCursor;
import org.tmatesoft.sqljet.core.internal.ISqlJetConfig;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.SqlJetAssert;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.internal.memory.SqlJetMemoryPointer;

public class SqlJetIndexedMemPages {
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

    private final int nMin;
    private final int pageSize;
    
    /** Index of current page in apPage */
    private int iPage = -1;
    
    /**
     * Pages from root to current page
     */
    private final SqlJetIndexedMemPage[] apPage = new SqlJetIndexedMemPage[ISqlJetBtreeCursor.BTCURSOR_MAX_DEPTH];
        
    public SqlJetIndexedMemPages(int nMin, int pageSize) {
		this.nMin = nMin;
		this.pageSize = pageSize;
	}

	public void addNewPage(SqlJetMemPage pPage) throws SqlJetException {
        SqlJetAssert.assertTrue(this.iPage < apPage.length-1, SqlJetErrorCode.CORRUPT);
    	iPage++;
		this.apPage[iPage] = new SqlJetIndexedMemPage(pPage);
    }

    public SqlJetIndexedMemPage getCurrentIndexedPage() {
    	return apPage[iPage];
    }
    
    public SqlJetMemPage getCurrentPage() {
    	return getCurrentIndexedPage().getPage();
    }
    
    public SqlJetMemPage getPage(int num) {
    	return apPage[num].getPage();
    }
    
    public SqlJetMemPage getFirstPage() {
    	return getPage(0);
    }
        
    public void setIndexOnCurrentPage(int index) {
    	apPage[iPage].setIndex(index);
    }

    public boolean hasCurrentPage() {
    	return (this.iPage >= 0 && getCurrentPage() != null);
    }
    
    public boolean hasExactlyOnePage() {
    	return this.iPage == 0;
    }
    
    public SqlJetMemPage popCurrentPage() {
    	SqlJetMemPage currentPage = getCurrentIndexedPage().getPage();
    	iPage--;
		return currentPage;
    }

    public List<SqlJetMemPage> getAllPages() {
    	List<SqlJetMemPage> result = new ArrayList<>();
        for (int i = 0; i <= iPage; i++) {
			result.add(apPage[i].getPage());
		}
        return result;
    }
    
    public void releaseAllPages() throws SqlJetException {
        for (SqlJetMemPage mp : getAllPages()) {
			SqlJetMemPage.releasePage(mp);
        }
    }
    
    public boolean releaseAfter(int num) throws SqlJetException {
    	boolean result = false;
		while (this.iPage > num) {
			result = true;
			SqlJetMemPage.releasePage(popCurrentPage());
		}
		return result;
    }
    
    public void clearAllPages() {
        for (int i = 0; i <= iPage; i++) {
        	apPage[i] = null;
        }
    	iPage = -1;
    }
    
    public int getIndexOnCurrentPage() {
    	return getCurrentIndexedPage().getIndex();
    }
    
    public int getNumberOfPages() {
    	return iPage+1;
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
	public void balance(boolean isInsert) throws SqlJetException {
		ISqlJetMemoryPointer aBalanceQuickSpace = SqlJetUtility.memoryManager.allocatePtr(13);

		int balance_quick_called = 0; // TESTONLY
		int balance_deeper_called = 0; // TESTONLY

		do {
			SqlJetMemPage pPage = getCurrentPage();

			if (hasExactlyOnePage()) {
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
					ISqlJetMemoryPointer pSpace = SqlJetUtility.memoryManager.allocatePtr(pageSize);
					balanceNonroot(pParent, iIdx, pSpace, iPage == 1);
				}

				pPage.aOvfl.clear();

				/*
				 * The next iteration of the do-loop balances the parent page.
				 */
				pPage.releasePage();
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
	private static void balanceNonroot(SqlJetMemPage pParent, int iParentIdx, ISqlJetMemoryPointer aOvflSpace,
			boolean isRoot) throws SqlJetException {

		int nCell = 0; /* Number of cells in apCell[] */
		int nMaxCells = 0; /* Allocated size of apCell, szCell, aFrom. */
		int nNew = 0; /* Number of pages in apNew[] */
		int nOld = 0; /* Number of pages in apOld[] */
		int i, j, k; /* Loop counters */
		int nxDiv; /* Next divider slot in pParent->aCell[] */
		int leafCorrection; /* 4 if pPage is a leaf. 0 if not */
		boolean leafData; /* True if pPage is a leaf of a LEAFDATA tree */
		int subtotal; /* Subtotal of bytes in cells on one page */
		int iSpace1 = 0; /* First unused byte of aSpace1[] */
		int iOvflSpace = 0; /* First unused byte of aOvflSpace[] */
		// int szScratch; /* Size of scratch memory requested */
		SqlJetMemPage[] apOld = new SqlJetMemPage[NB]; /*
														 * pPage and up to two
														 * siblings
														 */
		SqlJetMemPage[] apCopy = new SqlJetMemPage[NB]; /*
														 * Private copies of
														 * apOld[] pages
														 */
		SqlJetMemPage[] apNew = new SqlJetMemPage[NB + 2];
		/* pPage and up to NB siblings after balancing */
		/* u8 */ISqlJetMemoryPointer pRight;
		/* Location in parent of right-sibling pointer */
		/* u8 */ISqlJetMemoryPointer[] apDiv = new ISqlJetMemoryPointer[NB
				- 1]; /* Divider cells in pParent */
		int[] szNew = new int[NB + 2];
		/* u8 */ // SqlJetMemPage[] aSpace1; /* Space for copies of dividers
					// cells */

		SqlJetBtreeShared pBt = pParent.pBt; /* The whole database */
		assert (pParent.pDbPage.isWriteable());

		/*
		 * At this point pParent may have at most one overflow cell. And if this
		 * overflow cell is present, it must be the cell with index iParentIdx.
		 * This scenario comes about when this function is called (indirectly)
		 * from sqlite3BtreeDelete().
		 */
		assert (pParent.aOvfl.size() <= 1);
		assert (pParent.aOvfl.isEmpty() || pParent.aOvfl.get(0).getIdx() == iParentIdx);

		try {
			/*
			 * Find the sibling pages to balance. Also locate the cells in
			 * pParent that divide the siblings. An attempt is made to find NN
			 * siblings on either side of pPage. More siblings are taken from
			 * one side, however, if there are fewer than NN siblings on the
			 * other side. If pParent has NB or fewer children then all children
			 * of pParent are taken.
			 **
			 ** This loop also drops the divider cells from the parent page. This
			 ** way, the remainder of the function does not have to deal with any
			 ** overflow cells in the parent page, since if any existed they will
			 ** have already been removed.
			 */
			i = pParent.aOvfl.size() + pParent.nCell;
			if (i < 2) {
				nxDiv = 0;
				nOld = i + 1;
			} else {
				nOld = 3;
				if (iParentIdx == 0) {
					nxDiv = 0;
				} else if (iParentIdx == i) {
					nxDiv = i - 2;
				} else {
					nxDiv = iParentIdx - 1;
				}
				i = 2;
			}
			if ((i + nxDiv - pParent.aOvfl.size()) == pParent.nCell) {
				pRight = pParent.getData().getMoved(pParent.getHdrOffset() + 8);
			} else {
				pRight = pParent.findCell(i + nxDiv - pParent.aOvfl.size());
			}
			int pgno = pRight.getInt();
			while (true) {
				apOld[i] = pBt.getAndInitPage(pgno);
				nMaxCells += 1 + apOld[i].nCell + apOld[i].aOvfl.size();
				if ((i--) == 0) {
					break;
				}

				if (!pParent.aOvfl.isEmpty() && i + nxDiv == pParent.aOvfl.get(0).getIdx()) {
					apDiv[i] = pParent.aOvfl.get(0).getpCell();
					pgno = apDiv[i].getInt();
					szNew[i] = pParent.cellSizePtr(apDiv[i]);
					pParent.aOvfl.clear();
				} else {
					apDiv[i] = pParent.findCell(i + nxDiv - pParent.aOvfl.size());
					pgno = apDiv[i].getInt();
					szNew[i] = pParent.cellSizePtr(apDiv[i]);

					/*
					 * Drop the cell from the parent page. apDiv[i] still points
					 * to the cell within the parent, even though it has been
					 * dropped. This is safe because dropping a cell only
					 * overwrites the first four bytes of it, and this function
					 * does not need the first four bytes of the divider cell.
					 * So the pointer is safe to use later on.
					 **
					 ** But not if we are in secure-delete mode. In secure-delete
					 * mode, the dropCell() routine will overwrite the entire
					 * cell with zeroes. In this case, temporarily copy the cell
					 * into the aOvflSpace[] buffer. It will be copied out again
					 * as soon as the aSpace[] buffer is allocated.
					 */
					if (ISqlJetConfig.SECURE_DELETE) {
						int iOff = apDiv[i].getPointer() - pParent.getData().getPointer();
						if ((iOff + szNew[i]) > pBt.usableSize) {
							Arrays.fill(apOld, 0, i, null);
							// rc = SqlJetErrorCode.CORRUPT;
							// goto balance_cleanup;
							throw new SqlJetException(SqlJetErrorCode.CORRUPT);
						} else {
							aOvflSpace.getMoved(iOff).copyFrom(apDiv[i], szNew[i]);
							apDiv[i] = aOvflSpace.getMoved(apDiv[i].getPointer() - pParent.getData().getPointer());
						}
					}
					try {
						pParent.dropCell(i + nxDiv - pParent.aOvfl.size(), szNew[i]);
					} catch (SqlJetException e) {
						// rc = e.getErrorCode();
						TRACE("exception in dropCell call: %s", e.getMessage());
						// e.printStackTrace();
					}
				}
			}

			/*
			 * Make nMaxCells a multiple of 4 in order to preserve 8-byte
			 ** alignment
			 */
			nMaxCells = (nMaxCells + 3) & ~3;

			/*
			 ** Allocate space for memory structures
			 */
			/* Combined size of cells place on i-th page */
			/* u8 */ ISqlJetMemoryPointer[] apCell = new SqlJetMemoryPointer[nMaxCells];
			/* All cells begin balanced */
			/* u16 */ int[] szCell = new int[nMaxCells]; /* Local size of all cells in apCell[] */
			// aSpace1 = new SqlJetMemPage[nMaxCells];

			/*
			 ** Load pointers to all cells on sibling pages and the divider cells
			 ** into the local apCell[] array. Make copies of the divider cells
			 ** into space obtained from aSpace1[] and remove the the divider
			 * Cells from pParent.
			 **
			 ** If the siblings are on leaf pages, then the child pointers of the
			 ** divider cells are stripped from the cells before they are copied
			 ** into aSpace1[]. In this way, all cells in apCell[] are without
			 ** child pointers. If siblings are not leaves, then all cell in
			 ** apCell[] include child pointers. Either way, all cells in
			 * apCell[] are alike.
			 **
			 ** leafCorrection: 4 if pPage is a leaf. 0 if pPage is not a leaf.
			 ** leafData: 1 if pPage holds key+data and pParent holds only keys.
			 */
			leafCorrection = apOld[0].leaf ? 4 : 0;
			leafData = apOld[0].hasData;
			for (i = 0; i < nOld; i++) {
				int limit;

				/*
				 * Before doing anything else, take a copy of the i'th original
				 * sibling The rest of this function will use data from the
				 * copies rather that the original pages since the original
				 * pages will be in the process of being overwritten.
				 */
				SqlJetMemPage pOld = apCopy[i] = memcpy(apOld[i]);
				pOld.getData().copyFrom(apOld[i].getData(), pBt.getPageSize());

				limit = pOld.nCell + pOld.aOvfl.size();
				if (!pOld.aOvfl.isEmpty()) {
					for (j = 0; j < limit; j++) {
						assert (nCell < nMaxCells);
						apCell[nCell] = pOld.findOverflowCell(j);
						szCell[nCell] = pOld.cellSizePtr(apCell[nCell]);
						nCell++;
					}
				} else {
					ISqlJetMemoryPointer aData = pOld.getData();
					int maskPage = pOld.maskPage;
					int cellOffset = pOld.cellOffset;
					for (j = 0; j < limit; j++) {
						assert (nCell < nMaxCells);
						apCell[nCell] = findCellv2(aData, maskPage, cellOffset, j);
						szCell[nCell] = pOld.cellSizePtr(apCell[nCell]);
						nCell++;
					}
				}
				if (i < nOld - 1 && !leafData) {
					int sz = szNew[i];
					ISqlJetMemoryPointer pTemp;
					assert (nCell < nMaxCells);
					szCell[nCell] = sz;
					pTemp = SqlJetUtility.memoryManager.allocatePtr(sz);
					// pTemp = &aSpace1[iSpace1];
					iSpace1 += sz;
					assert (sz <= pBt.getMaxLocal() + 23);
					assert (iSpace1 <= pBt.getPageSize());
					pTemp.copyFrom(apDiv[i], sz);
					apCell[nCell] = pTemp.getMoved(leafCorrection);
					szCell[nCell] = szCell[nCell] - leafCorrection;
					if (!pOld.leaf) {
						assert (leafCorrection == 0);
						assert (pOld.getHdrOffset() == 0);
						/*
						 * The right pointer of the child page pOld becomes the
						 * left pointer of the divider cell
						 */
						apCell[nCell].copyFrom(pOld.getData().getMoved(8), 4);
					} else {
						assert (leafCorrection == 4);
						if (szCell[nCell] < 4) {
							/* Do not allow any cells smaller than 4 bytes. */
							szCell[nCell] = 4;
						}
					}
					nCell++;
				}
			}

			/*
			 ** Figure out the number of pages needed to hold all nCell cells.
			 ** Store this number in "k". Also compute szNew[] which is the total
			 ** size of all cells on the i-th page and cntNew[] which is the
			 * index in apCell[] of the cell that divides page i from page i+1.
			 ** cntNew[k] should equal nCell.
			 **
			 ** Values computed by this block:
			 **
			 ** k: The total number of sibling pages szNew[i]: Spaced used on the
			 * i-th sibling page. cntNew[i]: Index in apCell[] and szCell[] for
			 * the first cell to the right of the i-th sibling page.
			 ** usableSpace: Number of bytes of space available on each sibling.
			 **
			 */
			int[] cntNew = new int[NB
					+ 2]; /* Index in aCell[] of cell after i-th page */
			int usableSpace = pBt.usableSize - 12
					+ leafCorrection; /* Bytes in pPage beyond the header */
			for (subtotal = k = i = 0; i < nCell; i++) {
				assert (i < nMaxCells);
				subtotal += szCell[i] + 2;
				if (subtotal > usableSpace) {
					szNew[k] = subtotal - szCell[i];
					cntNew[k] = i;
					if (leafData) {
						i--;
					}
					subtotal = 0;
					k++;
					if (k > NB + 1) {
						// rc = SqlJetErrorCode.CORRUPT; break balance_cleanup;
						throw new SqlJetException(SqlJetErrorCode.CORRUPT);
					}
				}
			}
			szNew[k] = subtotal;
			cntNew[k] = nCell;
			k++;

			/*
			 ** The packing computed by the previous block is biased toward the
			 * siblings on the left side. The left siblings are always nearly
			 * full, while the right-most sibling might be nearly empty. This
			 * block of code attempts to adjust the packing of siblings to get a
			 * better balance.
			 **
			 ** This adjustment is more than an optimization. The packing above
			 * might be so out of balance as to be illegal. For example, the
			 * right-most sibling might be completely empty. This adjustment is
			 * not optional.
			 */
			for (i = k - 1; i > 0; i--) {
				int szRight = szNew[i]; /* Size of sibling on the right */
				int szLeft = szNew[i - 1]; /* Size of sibling on the left */
				int r; /* Index of right-most cell in left sibling */
				int d; /* Index of first cell to the left of right sibling */

				r = cntNew[i - 1] - 1;
				d = r + 1 - (leafData ? 1 : 0);
				assert (d < nMaxCells);
				assert (r < nMaxCells);
				while (szRight == 0 || szRight + szCell[d] + 2 <= szLeft - (szCell[r] + 2)) {
					szRight += szCell[d] + 2;
					szLeft -= szCell[r] + 2;
					cntNew[i - 1]--;
					r = cntNew[i - 1] - 1;
					d = r + 1 - (leafData ? 1 : 0);
				}
				szNew[i] = szRight;
				szNew[i - 1] = szLeft;
			}

			/*
			 * Either we found one or more cells (cntnew[0])>0) or pPage is a
			 * virtual root page. A virtual root page is when the real root page
			 * is page 1 and we are the only child of that page.
			 */
			assert (cntNew[0] > 0 || (pParent.pgno == 1 && pParent.nCell == 0));

			traceInt("BALANCE: old: %d %d %d  ", apOld[0].pgno, nOld >= 2 ? apOld[1].pgno : 0,
					nOld >= 3 ? apOld[2].pgno : 0);

			/*
			 ** Allocate k new pages. Reuse old pages where possible.
			 */
			if (apOld[0].pgno <= 1) {
				// rc = SqlJetErrorCode.CORRUPT;
				// break balance_cleanup;
				throw new SqlJetException(SqlJetErrorCode.CORRUPT);
			}
			int pageFlags = apOld[0].getData().getByteUnsigned(0); /* Value of pPage->aData[0] */
			for (i = 0; i < k; i++) {
				SqlJetMemPage pNew;
				if (i < nOld) {
					pNew = apNew[i] = apOld[i];
					apOld[i] = null;
					nNew++;
					pNew.pDbPage.write();
				} else {
					assert (i > 0);

					int[] p = { 0 };
					pNew = pBt.allocatePage(p, pgno, false);
					pgno = p[0];

					apNew[i] = pNew;
					nNew++;

					/* Set the pointer-map entry for the new sibling page. */
					if (pBt.autoVacuumMode.isAutoVacuum()) {
						pBt.ptrmapPut(pNew.pgno, SqlJetPtrMapType.PTRMAP_BTREE, pParent.pgno);
					}
				}
			}

			/*
			 * Free any old pages that were not reused as new pages.
			 */
			while (i < nOld) {
				apOld[i].freePage();
				apOld[i].releasePage();
				apOld[i] = null;
				i++;
			}

			/*
			 ** Put the new pages in accending order. This helps to keep entries
			 * in the disk file in order so that a scan of the table is a linear
			 * scan through the file. That in turn helps the operating system to
			 * deliver pages from the disk more rapidly.
			 **
			 ** An O(n^2) insertion sort algorithm is used, but since n is never
			 * more than NB (a small constant), that should not be a problem.
			 **
			 ** When NB==3, this one optimization makes the database about 25%
			 * faster for large insertions and deletions.
			 */
			for (i = 0; i < k - 1; i++) {
				int minV = apNew[i].pgno;
				int minI = i;
				for (j = i + 1; j < k; j++) {
					if (apNew[j].pgno < /* (unsigned) */minV) {
						minI = j;
						minV = apNew[j].pgno;
					}
				}
				if (minI > i) {
					SqlJetMemPage pT;
					pT = apNew[i];
					apNew[i] = apNew[minI];
					apNew[minI] = pT;
				}
			}
			traceInt("new: %d(%d) %d(%d) %d(%d) %d(%d) %d(%d)\n", apNew[0].pgno, szNew[0],
					nNew >= 2 ? apNew[1].pgno : 0, nNew >= 2 ? szNew[1] : 0, nNew >= 3 ? apNew[2].pgno : 0,
					nNew >= 3 ? szNew[2] : 0, nNew >= 4 ? apNew[3].pgno : 0, nNew >= 4 ? szNew[3] : 0,
					nNew >= 5 ? apNew[4].pgno : 0, nNew >= 5 ? szNew[4] : 0);

			assert (pParent.pDbPage.isWriteable());
			pRight.putIntUnsigned(0, apNew[nNew - 1].pgno);

			/*
			 ** Evenly distribute the data in apCell[] across the new pages.
			 ** Insert divider cells into pParent as necessary.
			 */
			j = 0;
			for (i = 0; i < nNew; i++) {
				/* Assemble the new sibling page. */
				SqlJetMemPage pNew = apNew[i];
				assert (j < nMaxCells);
				pNew.zeroPage(pageFlags);
				pNew.assemblePage(cntNew[i] - j, apCell, j, szCell, j);
				assert (pNew.nCell > 0 || (nNew == 1 && cntNew[0] == 0));
				assert (pNew.aOvfl.isEmpty());

				j = cntNew[i];

				/*
				 * If the sibling page assembled above was not the right-most
				 * sibling, insert a divider cell into the parent page.
				 */
				assert (i < nNew - 1 || j == nCell);
				if (j < nCell) {
					assert (j < nMaxCells);
					ISqlJetMemoryPointer pCell = apCell[j];
					int sz = szCell[j] + leafCorrection;
					ISqlJetMemoryPointer pTemp = aOvflSpace.getMoved(iOvflSpace);
					if (!pNew.leaf) {
						pNew.getData().getMoved(8).copyFrom(pCell, 4);
					} else if (leafData) {
						/*
						 * If the tree is a leaf-data tree, and the siblings are
						 * leaves, then there is no divider cell in apCell[].
						 * Instead, the divider cell consists of the integer key
						 * for the right-most cell of the sibling-page assembled
						 * above only.
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
						/*
						 * Obscure case for non-leaf-data trees: If the cell at
						 * pCell was previously stored on a leaf node, and its
						 * reported size was 4 bytes, then it may actually be
						 * smaller than this (see btreeParseCellPtr(), 4 bytes
						 * is the minimum size of any cell). But it is important
						 * to pass the correct size to insertCell(), so reparse
						 * the cell now.
						 **
						 ** Note that this can never happen in an SQLite data
						 * file, as all cells are at least 4 bytes. It only
						 * happens in b-trees used to evaluate "IN (SELECT ...)"
						 * and similar clauses.
						 */
						if (szCell[j] == 4) {
							assert (leafCorrection == 4);
							sz = pParent.cellSizePtr(pCell);
						}
					}
					iOvflSpace += sz;
					assert (sz <= pBt.getMaxLocal() + 23);
					assert (iOvflSpace <= pBt.getPageSize());
					pParent.insertCell(nxDiv, pCell, sz, pTemp, pNew.pgno);
					assert (pParent.pDbPage.isWriteable());

					j++;
					nxDiv++;
				}
			}
			assert (j == nCell);
			assert (nOld > 0);
			assert (nNew > 0);
			if ((pageFlags & SqlJetMemPage.PTF_LEAF) == 0) {
				ISqlJetMemoryPointer zChild = apCopy[nOld - 1].getData().getMoved(8);
				apNew[nNew - 1].getData().getMoved(8).copyFrom(zChild, 4);
			}

			if (isRoot && pParent.nCell == 0 && pParent.getHdrOffset() <= apNew[0].nFree) {
				/*
				 * The root page of the b-tree now contains no cells. The only
				 * sibling page is the right-child of the parent. Copy the
				 * contents of the child page into the parent, decreasing the
				 * overall height of the b-tree structure by one. This is
				 * described as the "balance-shallower" sub-algorithm in some
				 * documentation.
				 **
				 ** If this is an auto-vacuum database, the call to
				 * copyNodeContent() sets all pointer-map entries corresponding
				 * to database image pages for which the pointer is stored
				 * within the content being copied.
				 **
				 ** The second assert below verifies that the child page is
				 * defragmented (it must be, as it was just reconstructed using
				 * assemblePage()). This is important if the parent page happens
				 * to be page 1 of the database image.
				 */
				assert (nNew == 1);
				assert (apNew[0].nFree == (apNew[0].getData().getMoved(5).getShortUnsigned() - apNew[0].cellOffset
						- apNew[0].nCell * 2));
				apNew[0].copyNodeContent(pParent);
				apNew[0].freePage();
			} else if (pBt.autoVacuumMode.isAutoVacuum()) {
				/*
				 * Fix the pointer-map entries for all the cells that were
				 * shifted around. There are several different types of
				 * pointer-map entries that need to be dealt with by this
				 * routine. Some of these have been set already, but many have
				 * not. The following is a summary:
				 **
				 ** 1) The entries associated with new sibling pages that were
				 * not siblings when this function was called. These have
				 * already been set. We don't need to worry about old siblings
				 * that were moved to the free-list - the freePage() code has
				 * taken care of those.
				 **
				 ** 2) The pointer-map entries associated with the first overflow
				 ** page in any overflow chains used by new divider cells. These
				 ** have also already been taken care of by the insertCell()
				 * code.
				 **
				 ** 3) If the sibling pages are not leaves, then the child pages
				 * of cells stored on the sibling pages may need to be updated.
				 **
				 ** 4) If the sibling pages are not internal intkey nodes, then
				 * any overflow pages used by these cells may need to be updated
				 ** (internal intkey nodes never contain pointers to overflow
				 * pages).
				 **
				 ** 5) If the sibling pages are not leaves, then the pointer-map
				 ** entries for the right-child pages of each sibling may need to
				 * be updated.
				 **
				 ** Cases 1 and 2 are dealt with above by other code. The next
				 ** block deals with cases 3 and 4 and the one after that, case
				 * 5. Since setting a pointer map entry is a relatively
				 * expensive operation, this code only sets pointer map entries
				 * for child or overflow pages that have actually moved between
				 * pages.
				 */
				SqlJetMemPage pNew = apNew[0];
				SqlJetMemPage pOld = apCopy[0];
				int nOverflow = pOld.aOvfl.size();
				int iNextOld = pOld.nCell + nOverflow;
				int iOverflow = (nOverflow > 0 ? pOld.aOvfl.get(0).getIdx() : -1);
				j = 0; /* Current 'old' sibling page */
				k = 0; /* Current 'new' sibling page */
				boolean isDivider = false;
				for (i = 0; i < nCell; i++) {
					while (i == iNextOld) {
						/*
						 * Cell i is the cell immediately following the last
						 * cell on old sibling page j. If the siblings are not
						 * leaf pages of an intkey b-tree, then cell i was a
						 * divider cell.
						 */
						assert (j + 1 < apCopy.length);
						pOld = apCopy[++j];
						iNextOld = i + (!leafData ? 1 : 0) + pOld.nCell + pOld.aOvfl.size();
						if (!pOld.aOvfl.isEmpty()) {
							nOverflow = pOld.aOvfl.size();
							iOverflow = i + (!leafData ? 1 : 0) + pOld.aOvfl.get(0).getIdx();
						}
						isDivider = !leafData;
					}

					assert (nOverflow > 0 || iOverflow < i);
					assert (nOverflow < 2 || pOld.aOvfl.get(1).getIdx() == pOld.aOvfl.get(0).getIdx() - 1);
					assert (nOverflow < 3 || pOld.aOvfl.get(2).getIdx() == pOld.aOvfl.get(1).getIdx() - 1);
					if (i == iOverflow) {
						isDivider = true;
						if ((--nOverflow) > 0) {
							iOverflow++;
						}
					}

					if (i == cntNew[k]) {
						/*
						 * Cell i is the cell immediately following the last
						 * cell on new sibling page k. If the siblings are not
						 * leaf pages of an intkey b-tree, then cell i is a
						 * divider cell.
						 */
						pNew = apNew[++k];
						if (!leafData) {
							continue;
						}
					}
					assert (j < nOld);
					assert (k < nNew);

					/*
					 * If the cell was originally divider cell (and is not now)
					 * or an overflow cell, or if the cell was located on a
					 * different sibling page before the balancing, then the
					 * pointer map entries associated with any child or overflow
					 * pages need to be updated.
					 */
					if (isDivider || pOld.pgno != pNew.pgno) {
						if (!(leafCorrection > 0)) {
							pBt.ptrmapPut(apCell[i].getInt(), SqlJetPtrMapType.PTRMAP_BTREE, pNew.pgno);
						}
						if (szCell[i] > pNew.minLocal) {
							pNew.ptrmapPutOvflPtr(apCell[i]);
						}
					}
				}

				if (!(leafCorrection > 0)) {
					for (i = 0; i < nNew; i++) {
						int key = apNew[i].getData().getMoved(8).getInt();
						pBt.ptrmapPut(key, SqlJetPtrMapType.PTRMAP_BTREE, apNew[i].pgno);
					}
				}

			}

			assert (pParent.isInit);
			traceInt("BALANCE: finished: old=%d new=%d cells=%d\n", nOld, nNew, nCell);
		} finally {
			/*
			 ** Cleanup before returning.
			 */
			// balance_cleanup:
			// sqlite3ScratchFree(apCell);
			for (i = 0; i < nOld; i++) {
				SqlJetMemPage.releasePage(apOld[i]);
			}
			for (i = 0; i < nNew; i++) {
				SqlJetMemPage.releasePage(apNew[i]);
			}
		}
	} // balance_nonroot()

	//#define findCellv2(D,M,O,I) (D+(M&get2byte(D+(O+2*(I)))))
    private static ISqlJetMemoryPointer findCellv2(ISqlJetMemoryPointer d, int M, int O, int I) {
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
	private static void balanceQuick(SqlJetMemPage pParent, SqlJetMemPage pPage, ISqlJetMemoryPointer pSpace)
			throws SqlJetException {

		final SqlJetBtreeShared pBt = pPage.pBt; /* B-Tree Database */
		SqlJetMemPage pNew; /* Newly allocated page */
		int[] pgnoNew = { 0 }; /* Page number of pNew */

		assert (pParent.pDbPage.isWriteable());
		assert (pPage.aOvfl.size() == 1);

		/* This error condition is now caught prior to reaching this function */
		if (pPage.nCell <= 0) {
			throw new SqlJetException(SqlJetErrorCode.CORRUPT);
		}

		/*
		 * Allocate a new page. This page will become the right-sibling of
		 ** pPage. Make the parent page writable, so that the new divider cell
		 ** may be inserted. If both these operations are successful, proceed.
		 */
		pNew = pBt.allocatePage(pgnoNew, 0, false);

		try {

			ISqlJetMemoryPointer pOut = pSpace.getMoved(4);
			ISqlJetMemoryPointer pCell = pPage.aOvfl.get(0).getpCell();
			int szCell = pPage.cellSizePtr(pCell);
			ISqlJetMemoryPointer pStop;

			assert (pNew.pDbPage.isWriteable());
			assert (pPage.getData().getByteUnsigned(
					0) == (SqlJetMemPage.PTF_INTKEY | SqlJetMemPage.PTF_LEAFDATA | SqlJetMemPage.PTF_LEAF));
			pNew.zeroPage(SqlJetMemPage.PTF_INTKEY | SqlJetMemPage.PTF_LEAFDATA | SqlJetMemPage.PTF_LEAF);
			pNew.assemblePage(1, new ISqlJetMemoryPointer[] { pCell }, 0, new int[] { szCell }, 0);

			/*
			 * If this is an auto-vacuum database, update the pointer map with
			 * entries for the new page, and any pointer from the cell on the
			 * page to an overflow page. If either of these operations fails,
			 * the return code is set, but the contents of the parent page are
			 * still manipulated by thh code below. That is Ok, at this point
			 * the parent page is guaranteed to be marked as dirty. Returning an
			 * error code will cause a rollback, undoing any changes made to the
			 * parent page.
			 */
			if (pBt.autoVacuumMode.isAutoVacuum()) {
				pBt.ptrmapPut(pgnoNew[0], SqlJetPtrMapType.PTRMAP_BTREE, pParent.pgno);
				if (szCell > pNew.minLocal) {
					pNew.ptrmapPutOvflPtr(pCell);
				}
			}

			/*
			 * Create a divider cell to insert into pParent. The divider cell
			 ** consists of a 4-byte page number (the page number of pPage) and a
			 * variable length key value (which must be the same value as the
			 ** largest key on pPage).
			 **
			 ** To find the largest key value on pPage, first find the right-most
			 ** cell on pPage. The first two fields of this cell are the
			 ** record-length (a variable length integer at most 32-bits in size)
			 ** and the key value (a variable length integer, may have any
			 * value). The first of the while(...) loops below skips over the
			 * record-length field. The second while(...) loop copies the key
			 * value from the cell on pPage into the pSpace buffer.
			 */
			pCell = pPage.findCell(pPage.nCell - 1);
			pStop = pCell.getMoved(9);
			// while( (*(pCell++)&0x80) && pCell<pStop );
			do {
				boolean b = (pCell.getByteUnsigned() & 0x80) == 0;
				pCell.movePointer(1);
				if (b) {
					break;
				}
			} while (pCell.getPointer() < pStop.getPointer());
			pStop = pCell.getMoved(9);
			// while( ((*(pOut++) = *(pCell++))&0x80) && pCell<pStop );
			do {
				pOut.putByteUnsigned(pCell.getByteUnsigned());
				pOut.movePointer(1);
				boolean b = (pCell.getByteUnsigned() & 0x80) == 0;
				pCell.movePointer(1);
				if (b) {
					break;
				}
			} while (pCell.getPointer() < pStop.getPointer());

			/* Insert the new divider cell into pParent. */
			pParent.insertCell(pParent.nCell, pSpace, pOut.getPointer() - pSpace.getPointer(), null, pPage.pgno);

			/*
			 * Set the right-child pointer of pParent to point to the new page.
			 */
			pParent.getData().getMoved(pParent.getHdrOffset() + 8).putIntUnsigned(0, pgnoNew[0]);

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
    private static SqlJetMemPage balanceDeeper(SqlJetMemPage pRoot) throws SqlJetException {

    	  SqlJetMemPage pChild = null;           /* Pointer to a new child page */
    	  int[] pgnoChild = {0};            /* Page number of the new child page */
    	  SqlJetBtreeShared pBt = pRoot.pBt;    /* The BTree */

    	  assert( !pRoot.aOvfl.isEmpty() );

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
    	  pRoot.zeroPage(pChild.getData().getByteUnsigned(0) & ~SqlJetMemPage.PTF_LEAF);
    	  pRoot.getData().putIntUnsigned(pRoot.getHdrOffset()+8, pgnoChild[0]);

    	  return pChild;
    }
}
