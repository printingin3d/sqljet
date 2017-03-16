package org.tmatesoft.sqljet.core.internal.btree;

import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;

public class SqlJetOvflCell {
    /** Pointers to the body of the overflow cell */
    private final ISqlJetMemoryPointer pCell;

    /** Insert this cell before idx-th non-overflow cell */
    private final int idx;

    protected SqlJetOvflCell(ISqlJetMemoryPointer pCell, int idx) {
        this.pCell = pCell;
        this.idx = idx;
    }

    public ISqlJetMemoryPointer getpCell() {
        return pCell;
    }

    public int getIdx() {
        return idx;
    }
}