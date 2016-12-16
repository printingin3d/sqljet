/**
 * PCache.java
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetPage;
import org.tmatesoft.sqljet.core.internal.ISqlJetPageCache;
import org.tmatesoft.sqljet.core.internal.ISqlJetPageCallback;
import org.tmatesoft.sqljet.core.internal.SqlJetPageFlags;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;

/**
 * A complete page cache is an instance of this structure.
 * 
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetPageCache implements ISqlJetPageCache {

    /**
     * System property name for cache size configuration.
     */
    public static final String SQLJET_PAGE_CACHE_SIZE = "SQLJET.PAGE_CACHE_SIZE";
    public static final int PAGE_CACHE_SIZE_DEFAULT = 2000;
    public static final int PAGE_CACHE_SIZE_MINIMUM = 10;

    private static final int N_SORT_BUCKET = 25;

    /** List of dirty pages in LRU order */
    protected SqlJetPage pDirty;
    protected SqlJetPage pDirtyTail;
    /** Last synced page in dirty page list */
    protected SqlJetPage pSynced;
    /** Number of pinned pages */
    protected int nRef;
    /** Configured cache size */
    private int nMax = PAGE_CACHE_SIZE_DEFAULT;
    /** Configured minimum cache size */
    private int nMin = PAGE_CACHE_SIZE_MINIMUM;
    /** Size of every page in this cache */
    private int szPage;
    /** True if pages are on backing store */
    final boolean bPurgeable;
    /** Call to try make a page clean */
    final ISqlJetPageCallback xStress;
    final PCache pCache = new PCache();

    /**
     * Create a new pager cache. Under memory stress, invoke xStress to try to
     * make pages clean. Only clean and unpinned pages can be reclaimed.
     * 
     * @param szPage
     *            Size of every page
     * @param szExtra
     *            Extra space associated with each page
     * @param bPurgeable
     *            True if pages are on backing store
     * @param xDestroy
     *            Called to destroy a page
     * @param xStress
     *            Call to try to make pages clean
     */
    public SqlJetPageCache(int szPage, boolean purgeable, ISqlJetPageCallback stress) {
        final int cacheSize = SqlJetUtility.getIntSysProp(SQLJET_PAGE_CACHE_SIZE, nMax);
        if (cacheSize >= nMin) {
			nMax = cacheSize;
		}

        this.szPage = szPage;
        this.bPurgeable = purgeable;
        this.xStress = stress;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPageCache#setPageSize(int)
     */
    @Override
	public void setPageSize(int pageSize) {
        assert (this.nRef == 0 && this.pDirty == null);
        pCache.clear();
        this.szPage = pageSize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPageCache#fetch(int, boolean)
     */
    @Override
	public ISqlJetPage fetch(int pgno, boolean createFlag) throws SqlJetException {

        SqlJetPage pPage = null;

        assert (pgno > 0);

        /*
         * If the pluggable cache (sqlite3_pcache) has not been allocated,
         * allocate it now.
         */
        pPage = pCache.fetch(pgno, createFlag);

        if (pPage == null && createFlag) {
            SqlJetPage pPg;

            /*
             * Find a dirty page to write-out and recycle. First try to find a
             * page that does not require a journal-sync (one with
             * PGHDR_NEED_SYNC cleared), but if that is not possible settle for
             * any other unreferenced dirty page.
             */
            for (pPg = pSynced; pPg != null && (pPg.nRef > 0 || pPg.flags.contains(SqlJetPageFlags.NEED_SYNC)); pPg = pPg.pDirtyPrev) {
				;
			}
            if (pPg == null) {
                for (pPg = pDirtyTail; pPg != null && pPg.nRef > 0; pPg = pPg.pDirtyPrev) {
					;
				}
            }
            if (pPg != null) {
                xStress.pageCallback(pPg);
            }
            pCache.cleanUnpinned();

            pPage = pCache.fetch(pgno, true);
        }

        if (pPage != null) {
            if (0 == pPage.nRef) {
                nRef++;
            }
            pPage.nRef++;
            if (null == pPage.pData) {
				pPage.pData = SqlJetUtility.allocatePtr(szPage, SqlJetPage.BUFFER_TYPE);
			}
        }
        return pPage;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.tmatesoft.sqljet.core.ISqlJetPageCache#drop(org.tmatesoft.sqljet.
     * core.ISqlJetPage)
     */
    @Override
	public void drop(ISqlJetPage page) {
        SqlJetPage p = (SqlJetPage) page;
        assert (p.nRef >= 1);
        if (p.flags.contains(SqlJetPageFlags.DIRTY)) {
            p.removeFromDirtyList();
        }
        nRef--;
        pCache.unpin(p, true);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPageCache#cleanAll()
     */
    @Override
	public void cleanAll() {
        ISqlJetPage p;
        while ((p = pDirty) != null) {
            p.makeClean();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPageCache#clearSyncFlags()
     */
    @Override
	public void clearSyncFlags() {
        SqlJetPage p;
        for (p = pDirty; p != null; p = p.pDirtyNext) {
            p.flags.remove(SqlJetPageFlags.NEED_SYNC);
        }
        pSynced = pDirtyTail;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPageCache#truncate(int)
     */
    @Override
	public void truncate(int pgno) {
        SqlJetPage pNext;
        for (SqlJetPage p = pDirty; p != null; p = pNext) {
            pNext = p.pDirtyNext;
            if (p.getPageNumber() > pgno) {
                assert (p.flags.contains(SqlJetPageFlags.DIRTY));
                p.makeClean();
            }
        }
        pCache.truncate(pgno + 1);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPageCache#close()
     */
    @Override
	public void close() {
        pCache.clear();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPageCache#clear()
     */
    @Override
	public void clear() {
        truncate(0);
    }

    /*
     * Merge two lists of pages connected by pDirty and in pgno order. Do not
     * both fixing the pDirtyPrev pointers.
     */
    static SqlJetPage mergeDirtyList(SqlJetPage pA, SqlJetPage pB) {
        SqlJetPage result = new SqlJetPage();
        SqlJetPage pTail = result;
        while (pA != null && pB != null) {
            if (pA.getPageNumber() < pB.getPageNumber()) {
                pTail.pDirty = pA;
                pTail = pA;
                pA = pA.pDirty;
            } else {
                pTail.pDirty = pB;
                pTail = pB;
                pB = pB.pDirty;
            }
        }
        if (pA != null) {
            pTail.pDirty = pA;
        } else if (pB != null) {
            pTail.pDirty = pB;
        } else {
            pTail.pDirty = null;
        }
        return result.pDirty;
    }

    /*
     * Sort the list of pages in accending order by pgno. Pages are connected by
     * pDirty pointers. The pDirtyPrev pointers are corrupted by this sort.
     */
    static SqlJetPage sortDirtyList(SqlJetPage pIn) {
        SqlJetPage[] a = new SqlJetPage[N_SORT_BUCKET];
        SqlJetPage p;
        int i;
        while (pIn != null) {
            p = pIn;
            pIn = p.pDirty;
            p.pDirty = null;
            for (i = 0; i < N_SORT_BUCKET - 1; i++) {
                if (a[i] == null) {
                    a[i] = p;
                    break;
                } else {
                    p = mergeDirtyList(a[i], p);
                    a[i] = null;
                }
            }
            if (i == N_SORT_BUCKET - 1) {
                /*
                 * Coverage: To get here, there need to be 2^(N_SORT_BUCKET)
                 * elements in the input list. This is possible, but
                 * impractical. Testing this line is the point of global
                 * variable sqlite3_pager_n_sort_bucket.
                 */
                a[i] = mergeDirtyList(a[i], p);
            }
        }
        p = a[0];
        for (i = 1; i < N_SORT_BUCKET; i++) {
            p = mergeDirtyList(p, a[i]);
        }
        return p;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPageCache#getDirtyList()
     */
    @Override
	public ISqlJetPage getDirtyList() {
        SqlJetPage p;
        for (p = pDirty; p != null; p = p.pDirtyNext) {
            p.pDirty = p.pDirtyNext;
        }
        return sortDirtyList(pDirty);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPageCache#getRefCount()
     */
    @Override
	public int getRefCount() {
        return nRef;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPageCache#getPageCount()
     */
    @Override
	public int getPageCount() {
        return pCache.getPageCount();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPageCache#getCachesize()
     */
    @Override
	public int getCachesize() {
        return nMax;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetPageCache#setCacheSize(int)
     */
    @Override
	public void setCacheSize(int mxPage) {
        nMax = mxPage;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.tmatesoft.sqljet.core.ISqlJetPageCache#iterate(org.tmatesoft.sqljet
     * .core.ISqlJetPageCallback)
     */
    @Override
	public void iterate(ISqlJetPageCallback iter) throws SqlJetException {
        SqlJetPage pDirty;
        for (pDirty = this.pDirty; pDirty != null; pDirty = pDirty.pDirtyNext) {
            iter.pageCallback(pDirty);
        }
    }

    class PCache {

        /** Hash table for fast lookup by key */
        private Map<Integer, SqlJetPage> apHash = new HashMap<>();

        private Set<Integer> unpinned = new HashSet<>();

        public synchronized int getPageCount() {
            return apHash.size();
        }

        /**
         * Fetch a page by key value.
         * 
         * Whether or not a new page may be allocated by this function depends
         * on the value of the createFlag argument.
         * 
         * There are three different approaches to obtaining space for a page,
         * depending on the value of parameter createFlag (which may be 0, 1 or
         * 2).
         * 
         * 1. Regardless of the value of createFlag, the cache is searched for a
         * copy of the requested page. If one is found, it is returned.
         * 
         * 2. If createFlag==0 and the page is not already in the cache, NULL is
         * returned.
         * 
         * 3. If createFlag is 1, the cache is marked as purgeable and the page
         * is not already in the cache, and if either of the following are true,
         * return NULL:
         * 
         * (a) the number of pages pinned by the cache is greater than
         * PCache1.nMax, or (b) the number of pages pinned by the cache is
         * greater than the sum of nMax for all purgeable caches, less the sum
         * of nMin for all other purgeable caches.
         * 
         * 4. If none of the first three conditions apply and the cache is
         * marked as purgeable, and if one of the following is true:
         * 
         * (a) The number of pages allocated for the cache is already
         * PCache1.nMax, or
         * 
         * (b) The number of pages allocated for all purgeable caches is already
         * equal to or greater than the sum of nMax for all purgeable caches,
         * 
         * then attempt to recycle a page from the LRU list. If it is the right
         * size, return the recycled buffer. Otherwise, free the buffer and
         * proceed to step 5.
         * 
         * 5. Otherwise, allocate and return a new page buffer.
         */
        public synchronized SqlJetPage fetch(final int key, final boolean createFlag) {
            SqlJetPage pPage = apHash.get(Integer.valueOf(key));

            if (pPage != null || !createFlag) {
                return pPage;
            }

            /* Step 3 of header comment. */
            if (bPurgeable && getPageCount() == nMax) {
                return null;
            }

            /*
             * If a usable page buffer has still not been found, attempt to
             * allocate a new one.
             */
            pPage = new SqlJetPage(szPage, key);
            pPage.pCache = SqlJetPageCache.this;
            apHash.put(Integer.valueOf(key), pPage);

            return pPage;
        }

        /**
         * Mark a page as unpinned (eligible for asynchronous recycling).
         * 
         * xUnpin() is called by SQLite with a pointer to a currently pinned
         * page as its second argument. If the third parameter, discard, is
         * non-zero, then the page should be evicted from the cache. In this
         * case SQLite assumes that the next time the page is retrieved from the
         * cache using the xFetch() method, it will be zeroed. If the discard
         * parameter is zero, then the page is considered to be unpinned. The
         * cache implementation may choose to reclaim (free or recycle) unpinned
         * pages at any time. SQLite assumes that next time the page is
         * retrieved from the cache it will either be zeroed, or contain the
         * same data that it did when it was unpinned.
         * 
         * The cache is not required to perform any reference counting. A single
         * call to xUnpin() unpins the page regardless of the number of prior
         * calls to xFetch().
         * 
         */
        public synchronized void unpin(ISqlJetPage page, boolean discard) {
            Integer pageNumber = Integer.valueOf(page.getPageNumber());
            if (discard || (bPurgeable && getPageCount() == nMax)) {
                apHash.remove(pageNumber);
            } else {
                unpinned.add(pageNumber);
            }
        }

        /**
         * The xRekey() method is used to change the key value associated with
         * the page passed as the second argument from oldKey to newKey. If the
         * cache previously contains an entry associated with newKey, it should
         * be discarded. Any prior cache entry associated with newKey is
         * guaranteed not to be pinned.
         * 
         */
        public synchronized void rekey(SqlJetPage page, int newKey) {
            apHash.remove(Integer.valueOf(page.getPageNumber()));
            apHash.put(Integer.valueOf(newKey), page);
            page.setPageNumber(newKey);
        }

        /**
         * When SQLite calls the xTruncate() method, the cache must discard all
         * existing cache entries with page numbers (keys) greater than or equal
         * to the value of the iLimit parameter passed to xTruncate(). If any of
         * these pages are pinned, they are implicitly unpinned, meaning that
         * they can be safely discarded.
         * 
         */
        public synchronized void truncate(int iLimit) {
            List<Integer> l = new ArrayList<>();
            for (Integer i : apHash.keySet()) {
                if (i.intValue() >= iLimit) {
                    l.add(i);
                }
            }
            for (Integer i : l) {
				apHash.remove(i);
				unpinned.remove(i);
			}
        }

        /**
         * The xDestroy() method is used to delete a cache allocated by
         * xCreate(). All resources associated with the specified cache should
         * be freed. After calling the xDestroy() method, SQLite considers the
         * [sqlite3_pcache*] handle invalid, and will not use it with any other
         * sqlite3_pcache_methods functions.
         */
        public synchronized void clear() {
            apHash.clear();
            unpinned.clear();
        }

        /**
         * 
         */
        public void cleanUnpinned() {
            final Iterator<Integer> i = unpinned.iterator();
            while (i.hasNext()) {
                final Integer next = i.next();
                final SqlJetPage p = apHash.get(next);
                if (p == null || p.getRefCount()>0) {
                    i.remove();
                    continue;
                }
                final Set<SqlJetPageFlags> flags = p.getFlags();
                if (!flags.contains(SqlJetPageFlags.DIRTY) && !flags.contains(SqlJetPageFlags.NEED_SYNC)) {
	                apHash.remove(next);
	                i.remove();
	                return;
                }
            }
        }

    }

}
