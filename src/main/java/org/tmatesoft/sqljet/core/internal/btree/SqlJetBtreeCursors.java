package org.tmatesoft.sqljet.core.internal.btree;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetBtreeCursor;

public class SqlJetBtreeCursors {
    /** A list of all open cursors */
    private final List<SqlJetBtreeCursor> pCursor = new LinkedList<>();

    /**
     * Save the positions of all cursors except pExcept open on the table with
     * root-page iRoot. Usually, this is called just before cursor pExcept is
     * used to modify the table (BtreeDelete() or BtreeInsert()).
     *
     * @param i
     * @param j
     * @throws SqlJetException
     */
    public void saveAllCursors(int iRoot, SqlJetBtreeCursor pExcept) throws SqlJetException {
        for (SqlJetBtreeCursor p : this.pCursor) {
            if (p != pExcept && (0 == iRoot || p.pgnoRoot == iRoot) && p.eState.isValid()) {
                p.saveCursorPosition();
            }
        }
    }

    public void remove(ISqlJetBtreeCursor cursor) {
    	pCursor.remove(cursor);
    }
    
    public void add(SqlJetBtreeCursor cursor) {
    	pCursor.add(0, cursor);
    }
    
    public void close() throws SqlJetException {
        for (SqlJetBtreeCursor pCur : new ArrayList<>(pCursor)) {
            pCur.closeCursor();
        }
    }
    
    public boolean isEmpty() {
    	return pCursor.isEmpty();
    }
    
	public void tripAllCursors(SqlJetErrorCode errCode) throws SqlJetException {
        for (SqlJetBtreeCursor p : pCursor) {
            p.clearCursor();
            p.eState = SqlJetCursorState.FAULT;
            p.error = errCode;
            p.skip = errCode != null ? 1 : 0;
            p.releaseAllPages();
        }
    }

}
