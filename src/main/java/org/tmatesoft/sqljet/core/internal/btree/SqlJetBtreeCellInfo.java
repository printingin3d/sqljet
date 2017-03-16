package org.tmatesoft.sqljet.core.internal.btree;

import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;

/**
 * An instance of the following structure is used to hold information about a
 * cell. The parseCellPtr() function fills in this structure based on
 * information extract from the raw disk page.
 */
public class SqlJetBtreeCellInfo {
    protected final ISqlJetMemoryPointer pCell; /*
                                                 * Pointer to the start of cell
                                                 * content
                                                 */
    private long nKey; /*
                        * The key for INTKEY tables, or number of bytes in key
                        */
    protected final int nData; /* Number of bytes of data */
    protected final int nPayload; /* Total amount of payload */
    protected final int nHeader; /* Size of the cell content header in bytes */
    protected final int nLocal; /* Amount of payload held locally */
    protected final int iOverflow; /*
                                    * Offset to overflow page number. Zero if no
                                    * overflow
                                    */
    protected int nSize; /* Size of the cell content on the main b-tree page */

    public SqlJetBtreeCellInfo(ISqlJetMemoryPointer pCell, int nHeader, int nData, int nPayload, int iOverflow,
            int nLocal, int nSize) {
        this.pCell = pCell;
        this.nHeader = nHeader;
        this.nData = nData;
        this.nPayload = nPayload;
        this.iOverflow = iOverflow;
        this.nLocal = nLocal;
        this.nSize = nSize;
    }

    public long getnKey() {
        return nKey;
    }

    public void setnKey(long nKey) {
        this.nKey = nKey;
    }

}