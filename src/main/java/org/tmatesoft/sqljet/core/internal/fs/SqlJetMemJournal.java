/**
 * SqlJetMemJournal.java
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
package org.tmatesoft.sqljet.core.internal.fs;

import java.util.Set;

import org.tmatesoft.sqljet.core.internal.ISqlJetFile;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.SqlJetFileOpenPermission;
import org.tmatesoft.sqljet.core.internal.SqlJetFileType;
import org.tmatesoft.sqljet.core.internal.SqlJetLockType;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;

/**
 * This subclass is a subclass of sqlite3_file. Each open memory-journal is an
 * instance of this class.
 * 
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetMemJournal implements ISqlJetFile {

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetFile#isMemJournal()
     */
    @Override
	public boolean isMemJournal() {
        return true;
    }

    /*
     * Space to hold the rollback journal is allocated in increments of* this
     * many bytes.
     */
    private static final int JOURNAL_CHUNKSIZE = 1024;

    /*
     * Macro to find the minimum of two numeric values.
     */
    private static int MIN(int x, int y) {
        return ((x) < (y) ? (x) : (y));
    }

    /**
     * The rollback journal is composed of a linked list of these structures.
     */
    private static class FileChunk {
        /** Next chunk in the journal */
        FileChunk pNext;
        /** Content of this chunk */
        ISqlJetMemoryPointer zChunk = SqlJetUtility.memoryManager.allocatePtr(JOURNAL_CHUNKSIZE);
    };

    /*
     * * An instance of this object serves as a cursor into the rollback
     * journal.* The cursor can be either for reading or writing.
     */
    private static class FilePoint {
        long iOffset; /* Offset from the beginning of the file */
        FileChunk pChunk; /* Specific chunk into which cursor points */
    };

    FileChunk pFirst; /* Head of in-memory chunk-list */
    FilePoint endpoint = new FilePoint(); /* Pointer to the end of the file */
    FilePoint readpoint = new FilePoint(); /*
                                            * Pointer to the end of the last
                                            * xRead()
                                            */

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetFile#read(byte[], int, long)
     */
    @Override
	public int read(ISqlJetMemoryPointer buffer, int amount, long offset) {
        int iAmt = amount;
        long iOfst = offset;

        int zOut = 0;
        int nRead = iAmt;
        int iChunkOffset;
        FileChunk pChunk;

        assert (iOfst + iAmt <= this.endpoint.iOffset);

        if (this.readpoint.iOffset != iOfst || iOfst == 0) {
            long iOff = 0;
            for (pChunk = this.pFirst; pChunk != null && (iOff + JOURNAL_CHUNKSIZE) <= iOfst; pChunk = pChunk.pNext) {
                iOff += JOURNAL_CHUNKSIZE;
            }
        } else {
            pChunk = this.readpoint.pChunk;
        }

        iChunkOffset = (int) (iOfst % JOURNAL_CHUNKSIZE);
        do {
            int iSpace = JOURNAL_CHUNKSIZE - iChunkOffset;
            int nCopy = MIN(nRead, (JOURNAL_CHUNKSIZE - iChunkOffset));
            buffer.copyFrom(zOut, pChunk.zChunk, iChunkOffset, nCopy);
            zOut += nCopy;
            nRead -= iSpace;
            iChunkOffset = 0;
        } while (nRead >= 0 && (pChunk = pChunk.pNext) != null && nRead > 0);
        this.readpoint.iOffset = iOfst + iAmt;
        this.readpoint.pChunk = pChunk;

        return iAmt - nRead;

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetFile#write(byte[], int, long)
     */
    @Override
	public void write(ISqlJetMemoryPointer buffer, int amount, long offset) {
        int iAmt = amount;
        long iOfst = offset;

        int nWrite = iAmt;
        int zWrite = 0;

        /*
         * An in-memory journal file should only ever be appended to. Random*
         * access writes are not required by sqlite.
         */
        assert (iOfst == this.endpoint.iOffset);

        while (nWrite > 0) {
            FileChunk pChunk = this.endpoint.pChunk;
            int iChunkOffset = (int) (this.endpoint.iOffset % JOURNAL_CHUNKSIZE);
            int iSpace = MIN(nWrite, JOURNAL_CHUNKSIZE - iChunkOffset);

            if (iChunkOffset == 0) {
                /* New chunk is required to extend the file. */
                FileChunk pNew = new FileChunk();
                pNew.pNext = null;
                if (pChunk != null) {
                    assert (this.pFirst != null);
                    pChunk.pNext = pNew;
                } else {
                    assert (this.pFirst == null);
                    this.pFirst = pNew;
                }
                this.endpoint.pChunk = pNew;
            }

            this.endpoint.pChunk.zChunk.copyFrom(iChunkOffset, buffer, zWrite, iSpace);
            zWrite += iSpace;
            nWrite -= iSpace;
            this.endpoint.iOffset += iSpace;
        }
    }

    @Override
	public void truncate(long size) {
    }

    @Override
	public void close() {
        truncate(0);
    }

    @Override
	public void sync() {
    }

    @Override
	public long fileSize() {
        return endpoint.iOffset;
    }

    @Override
	public boolean checkReservedLock() {
        return false;
    }

    @Override
	public SqlJetFileType getFileType() {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetFile#getLockType()
     */
    @Override
	public SqlJetLockType getLockType() {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetFile#getPermissions()
     */
    @Override
	public Set<SqlJetFileOpenPermission> getPermissions() {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.tmatesoft.sqljet.core.ISqlJetFile#lock(org.tmatesoft.sqljet.core.
     * SqlJetLockType)
     */
    @Override
	public boolean lock(SqlJetLockType lockType) {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.ISqlJetFile#sectorSize()
     */
    @Override
	public int sectorSize() {
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.tmatesoft.sqljet.core.ISqlJetFile#unlock(org.tmatesoft.sqljet.core
     * .SqlJetLockType)
     */
    @Override
	public boolean unlock(SqlJetLockType lockType) {
        return false;
    }

}
