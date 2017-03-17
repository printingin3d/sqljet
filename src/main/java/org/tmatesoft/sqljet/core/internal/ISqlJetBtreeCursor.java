/**
 * ISqlJetBtreeCursor.java
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
package org.tmatesoft.sqljet.core.internal;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public interface ISqlJetBtreeCursor {

    /**
     * Maximum depth of an SQLite B-Tree structure. Any B-Tree deeper than this
     * will be declared corrupt. This value is calculated based on a maximum
     * database size of 2^31 pages a minimum fanout of 2 for a root-node and 3
     * for all other internal nodes.
     *
     * If a tree that appears to be taller than this is encountered, it is
     * assumed that the database is corrupt.
     */
    int BTCURSOR_MAX_DEPTH = 20;

    /**
     * Close a cursor. The read lock on the database file is released when the
     * last cursor is closed.
     *
     * @throws SqlJetException
     */
    void closeCursor() throws SqlJetException;

    /**
     * In this version of moveTo(), pKey is a packed index record such as is
     * generated by the OP_MakeRecord opcode. Unpack the record and then call
     * BtreeMovetoUnpacked() to do the work.
     *
     * @param pKey
     *            Packed key if the btree is an index
     * @param nKey
     *            Integer key for tables. Size of pKey for indices
     * @param bias
     *            Bias search to the high end
     * @return
     * @throws SqlJetException
     */
    int moveTo(ISqlJetMemoryPointer pKey, long nKey, boolean bias) throws SqlJetException;

    /**
     * Move the cursor so that it points to an entry near the key specified by
     * pIdxKey or intKey. Return a success code.
     *
     * For INTKEY tables, the intKey parameter is used. pIdxKey must be NULL.
     * For index tables, pIdxKey is used and intKey is ignored.
     *
     * If an exact match is not found, then the cursor is always left pointing
     * at a leaf page which would hold the entry if it were present. The cursor
     * might point to an entry that comes before or after the key.
     *
     * An integer is returned which is the result of comparing the key with the
     * entry to which the cursor is pointing. The meaning of the integer
     * returned is as follows:
     *
     * <0 The cursor is left pointing at an entry that is smaller than
     * intKey/pIdxKey or if the table is empty and the cursor is therefore left
     * point to nothing.
     *
     * ==0 The cursor is left pointing at an entry that exactly matches
     * intKey/pIdxKey.
     *
     * >0 The cursor is left pointing at an entry that is larger than
     * intKey/pIdxKey.
     *
     */
    int moveToUnpacked(ISqlJetUnpackedRecord pUnKey, long intKey, boolean bias) throws SqlJetException;

    /**
     * Determine whether or not a cursor has moved from the position it was last
     * placed at. Cursors can move when the row they are pointing at is deleted
     * out from under them.
     *
     * @return true if the cursor has moved and false if not.
     * @throws SqlJetException
     */
    boolean cursorHasMoved() throws SqlJetException;

    /**
     * Delete the entry that the cursor is pointing to. The cursor is left
     * pointing at a arbitrary location.
     *
     * @throws SqlJetException
     */
    void delete() throws SqlJetException;

    /**
     * Insert a new record into the BTree. The key is given by (pKey,nKey) and
     * the data is given by (pData,nData). The cursor is used only to define
     * what table the record should be inserted into. The cursor is left
     * pointing at a random location.
     *
     * For an INTKEY table, only the nKey value of the key is used. pKey is
     * ignored. For a ZERODATA table, the pData and nData are both ignored.
     *
     * @param pKey
     *            The key of the new record
     * @param nKey
     *            The key of the new record
     * @param pData
     *            The data of the new record
     * @param nData
     *            The data of the new record
     * @param nZero
     *            Number of extra 0 bytes to append to data
     * @param bias
     *            True if this is likely an append
     * @throws SqlJetException
     */
    void insert(ISqlJetMemoryPointer pKey, long nKey, ISqlJetMemoryPointer pData, int nData, int nZero, boolean bias)
            throws SqlJetException;

    /**
     * Move the cursor to the first entry in the table.
     *
     * @return false if the cursor actually points to something or true if the
     *         table is empty.
     * @throws SqlJetException
     */
    boolean first() throws SqlJetException;

    /**
     * Move the cursor to the last entry in the table.
     *
     * @return true if the cursor actually points to something or false if the
     *         table is empty.
     * @throws SqlJetException
     */
    boolean last() throws SqlJetException;

    /**
     * Advance the cursor to the next entry in the database. If successful then
     * return false. If the cursor was already pointing to the last entry in the
     * database before this routine was called, then return true.
     *
     * @return
     */
    boolean next() throws SqlJetException;

    /**
     * Return TRUE if the cursor is not pointing at an entry of the table.
     *
     * TRUE will be returned after a call to sqlite3BtreeNext() moves past the
     * last entry in the table or sqlite3BtreePrev() moves past the first entry.
     * TRUE is also returned if the table is empty.
     *
     * @return
     * @throws SqlJetException
     */
    boolean eof();

    /**
     * Return the flag byte at the beginning of the page that the cursor is
     * currently pointing to.
     *
     * @return
     * @throws SqlJetException
     */
    int flags() throws SqlJetException;

    /**
     * Step the cursor to the back to the previous entry in the database. If
     * successful then return false. If the cursor was already pointing to the
     * first entry in the database before this routine was called, then return
     * true.
     */
    boolean previous() throws SqlJetException;

    /**
     * Returns the size of the buffer needed to hold the value of the key for
     * the current entry. If the cursor is not pointing to a valid entry,
     * returns 0.
     *
     * For a table with the INTKEY flag set, this routine returns the key
     * itself, not the number of bytes in the key.
     */
    long getKeySize() throws SqlJetException;

    /**
     * Read part of the key associated with cursor pCur. Exactly "amt" bytes
     * will be transfered into buf[]. The transfer begins at "offset".
     *
     * Throws error code if anything goes wrong. An error is thrown if
     * "offset+amt" is larger than the available payload.
     */
    void key(int offset, int amt, @Nonnull ISqlJetMemoryPointer buf) throws SqlJetException;

    /**
     * Return the database connection handle for a cursor.
     *
     * @return
     * @throws SqlJetException
     */
    ISqlJetDbHandle getCursorDb() throws SqlJetException;

    /**
     * For the entry that cursor pCur is point to, return as many bytes of the
     * key or data as are available on the local b-tree page. Write the number
     * of available bytes into *pAmt.
     *
     * The pointer returned is ephemeral. The key/data may move or be destroyed
     * on the next call to any Btree routine, including calls from other threads
     * against the same cache. Hence, a mutex on the BtShared should be held
     * prior to calling this routine.
     *
     * These routines is used to get quick access to key and data in the common
     * case where no overflow pages are used.
     *
     * @return
     */
    @Nonnull
    ISqlJetMemoryPointer keyFetch(int[] pAmt) throws SqlJetException;

    /**
     * For the entry that cursor pCur is point to, return as many bytes of the
     * key or data as are available on the local b-tree page. Write the number
     * of available bytes into *pAmt.
     *
     * The pointer returned is ephemeral. The key/data may move or be destroyed
     * on the next call to any Btree routine, including calls from other threads
     * against the same cache. Hence, a mutex on the BtShared should be held
     * prior to calling this routine.
     *
     * These routines is used to get quick access to key and data in the common
     * case where no overflow pages are used.
     *
     * @return
     */
    @Nonnull
    ISqlJetMemoryPointer dataFetch(int[] pAmt) throws SqlJetException;

    /**
     * Return the number of bytes of data in the entry the cursor currently
     * points to. If the cursor is not currently pointing to an entry (which can
     * happen, for example, if the database is empty) then return 0.
     *
     * @return
     * @throws SqlJetException
     */
    int getDataSize() throws SqlJetException;

    /**
     * Read part of the data associated with cursor pCur. Exactly "amt" bytes
     * will be transfered into buf[]. The transfer begins at "offset".
     *
     * Throws error code if anything goes wrong. An error is returned if
     * "offset+amt" is larger than the available payload.
     *
     * @param offset
     * @param amt
     * @param buf
     * @throws SqlJetException
     */
    void data(int offset, int amt, @Nonnull ISqlJetMemoryPointer buf) throws SqlJetException;

    /**
     * Must be a cursor opened for writing on an INTKEY table currently pointing
     * at a valid table entry. This function modifies the data stored as part of
     * that entry. Only the data content may only be modified, it is not
     * possible to change the length of the data stored.
     *
     * @param offset
     * @param amt
     * @param data
     * @throws SqlJetException
     */
    void putData(int offset, int amt, @Nonnull ISqlJetMemoryPointer data) throws SqlJetException;

    /**
     * Clear the current cursor position.
     *
     * @throws SqlJetException
     */
    void clearCursor() throws SqlJetException;

    int getPgnoRoot();
    
    /**
     * Save the current cursor position in the variables BtCursor.nKey and
     * BtCursor.pKey. The cursor's state is set to CURSOR_REQUIRESEEK.
     *
     */
    public void saveCursorPosition() throws SqlJetException;

    /**
     * Restore the cursor to the position it was in (or as close to as possible)
     * when saveCursorPosition() was called. Note that this call deletes the
     * saved position info stored by saveCursorPosition(), so there can be at
     * most one effective restoreCursorPosition() call after each
     * saveCursorPosition().
     */
    public void restoreCursorPosition() throws SqlJetException;

    void tripCursor(@Nonnull SqlJetErrorCode errCode) throws SqlJetException;
}
