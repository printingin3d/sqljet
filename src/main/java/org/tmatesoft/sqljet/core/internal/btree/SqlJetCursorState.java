package org.tmatesoft.sqljet.core.internal.btree;

/**
 * Potential values for BtCursor.eState.
 *
 * <ul>
 *
 * <li>CURSOR_VALID: Cursor points to a valid entry. getPayload() etc. may be
 * called.</li>
 *
 * <li>CURSOR_INVALID: Cursor does not point to a valid entry. This can happen
 * (for example) because the table is empty or because BtreeCursorFirst() has
 * not been called.</li>
 *
 * <li>CURSOR_REQUIRESEEK: The table that this cursor was opened on still
 * exists, but has been modified since the cursor was last used. The cursor
 * position is saved in variables BtCursor.pKey and BtCursor.nKey. When a cursor
 * is in this state, restoreCursorPosition() can be called to attempt to seek
 * the cursor to the saved position.</li>
 *
 * <li>CURSOR_FAULT: A unrecoverable error (an I/O error or a malloc failure)
 * has occurred on a different connection that shares the BtShared cache with
 * this cursor. The error has left the cache in an inconsistent state. Do
 * nothing else with this cursor. Any attempt to use the cursor should return
 * the error code stored in BtCursor.skip</li>
 *
 * </ul>
 *
 */
public enum SqlJetCursorState {
    INVALID, // 0
    VALID, // 1
    REQUIRESEEK, // 2
    FAULT; // 3

    public boolean isValid() {
        return this == VALID;
    }

    public boolean isInvalid() {
        return this == INVALID;
    }

    public boolean isValidOrInvalid() {
        return isValid() || isInvalid();
    }
}