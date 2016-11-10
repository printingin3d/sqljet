package org.tmatesoft.sqljet.core.internal.pager;

import java.util.BitSet;

import org.tmatesoft.sqljet.core.internal.SqlJetCloneable;

/**
 * An instance of the following structure is allocated for each active
 * savepoint and statement transaction in the system. All such structures
 * are stored in the Pager.aSavepoint[] array, which is allocated and
 * resized using sqlite3Realloc().
 *
 * When a savepoint is created, the PagerSavepoint.iHdrOffset field is set
 * to 0. If a journal-header is written into the main journal while the
 * savepoint is active, then iHdrOffset is set to the byte offset
 * immediately following the last journal record written into the main
 * journal before the journal-header. This is required during savepoint
 * rollback (see pagerPlaybackSavepoint()).
 */

public class SqlJetPagerSavepoint extends SqlJetCloneable {
    /** Starting offset in main journal */
    long iOffset;
    /** See above */
    long iHdrOffset;
    /** Set of pages in this savepoint */
    BitSet pInSavepoint;
    /** Original number of pages in file */
    int nOrig;
    /** Index of first record in sub-journal */
    int iSubRec;
}