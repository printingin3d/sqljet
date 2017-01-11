/**
 * ISqlJetPageCache.java
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
package org.tmatesoft.sqljet.core.internal;

import java.util.List;

import org.tmatesoft.sqljet.core.SqlJetException;

/**
 * The page cache subsystem
 * 
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public interface ISqlJetPageCache {

    /**
     * Modify the page-size after the cache has been created. Change the page
     * size for PCache object. This can only happen when the cache is empty.
     * 
     * @param pageSize
     */
    void setPageSize(int pageSize);

    /**
     * Try to obtain a page from the cache.
     * 
     * 
     * @param pgno
     *            Page number to obtain
     * @param createFlag
     *            If true, create page if it does not exist already
     * @return
     * @throws SqlJetException
     */
    ISqlJetPage fetch(int pageNumber, boolean createFlag) throws SqlJetException;

    /**
     * Remove page from cache
     * 
     * Drop a page from the cache. There must be exactly one reference to the
     * page. This function deletes that reference, so after it returns the page
     * pointed to by p is invalid.
     * 
     * @param page
     * @throws SqlJetExceptionRemove
     */
    void drop(ISqlJetPage page);

    /**
     * Mark all dirty list pages as clean Make every page in the cache clean.
     * 
     * @throws SqlJetExceptionRemove
     * 
     */
    void cleanAll();

    /**
     * Remove all pages with page numbers more than pageNumber. Reset the cache
     * if pageNumber==0
     * 
     * Drop every cache entry whose page number is greater than "pgno".
     * 
     * @param pageNumber
     * @throws SqlJetExceptionRemove
     */
    void truncate(int pageNumber);

    /**
     * Get a list of all dirty pages in the cache, sorted by page number
     * 
     * @return
     */
    List<ISqlJetPage> getDirtyList();

    /**
     * Reset and close the cache object
     * 
     */
    void close();

    /**
     * Clear flags from pages of the page cache
     * 
     * @throws SqlJetExceptionRemove
     */
    void clearSyncFlags();

    /**
     * Return true if the number of dirty pages is 0 or 1
     */
    // boolean isZeroOrOneDirtyPages();
    /**
     * Discard the contents of the cache
     */
    void clear();

    /**
     * Return the total number of outstanding page references
     */
    int getRefCount();

    /**
     * Return the total number of pages stored in the cache
     */
    int getPageCount();

    /**
     * Iterate through all pages currently stored in the cache.
     * 
     * @param xIter
     * @throws SqlJetException
     */
    void iterate(ISqlJetPageCallback xIter) throws SqlJetException;

    /**
     * Get the cache-size for the pager-cache.
     * 
     * @return
     */
    int getCachesize();

    /**
     * Set the suggested cache-size for the pager-cache.
     * 
     * If no global maximum is configured, then the system attempts to limit the
     * total number of pages cached by purgeable pager-caches to the sum of the
     * suggested cache-sizes.
     * 
     * @param cacheSize
     */
    void setCacheSize(int cacheSize);

}
