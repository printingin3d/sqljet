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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.internal.ISqlJetFile;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
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

    private static final int JOURNAL_CHUNKSIZE = 1024;

    private final List<ISqlJetMemoryPointer> chunks = new ArrayList<>();
    private long offset = 0;

    @Override
    public boolean isMemJournal() {
        return true;
    }

    private ISqlJetMemoryPointer findChunk(long offset) {
        int index = (int) Math.floorDiv(offset, JOURNAL_CHUNKSIZE);
        return index >= chunks.size() ? null : chunks.get(index);
    }

    @Override
    public int read(@Nonnull ISqlJetMemoryPointer buffer, int amount, long offset) {
        int zOut = 0;
        int nRead = amount;
        int iChunkOffset;

        assert offset + amount <= this.offset;

        iChunkOffset = (int) (offset % JOURNAL_CHUNKSIZE);
        while (nRead >= 0) {
            int nCopy = Integer.min(nRead, JOURNAL_CHUNKSIZE - iChunkOffset);
            ISqlJetMemoryPointer chunk = findChunk(offset + zOut);
            if (chunk == null) {
                break;
            }
            buffer.copyFrom(zOut, chunk, iChunkOffset, nCopy);
            zOut += nCopy;
            nRead -= JOURNAL_CHUNKSIZE - iChunkOffset;
            iChunkOffset = 0;
        }
        ;

        return amount - nRead;
    }

    @Override
    public void write(@Nonnull ISqlJetMemoryPointer buffer, int amount, long offset) {
        int nWrite = amount;
        int zWrite = 0;

        /*
         * An in-memory journal file should only ever be appended to. Random*
         * access writes are not required by sqlite.
         */
        assert offset == this.offset;

        while (nWrite > 0) {
            ISqlJetMemoryPointer pChunk = findChunk(this.offset);
            int iChunkOffset = (int) (this.offset % JOURNAL_CHUNKSIZE);
            int iSpace = Integer.min(nWrite, JOURNAL_CHUNKSIZE - iChunkOffset);

            if (pChunk == null) {
                /* New chunk is required to extend the file. */
                pChunk = SqlJetUtility.memoryManager.allocatePtr(JOURNAL_CHUNKSIZE);
                chunks.add(pChunk);
            }

            pChunk.copyFrom(iChunkOffset, buffer, zWrite, iSpace);
            zWrite += iSpace;
            nWrite -= iSpace;
            this.offset += iSpace;
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
        return offset;
    }

    @Override
    public boolean checkReservedLock() {
        return false;
    }

    @Override
    public SqlJetLockType getLockType() {
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean isReadWrite() {
        return false;
    }

    @Override
    public boolean lock(@Nonnull SqlJetLockType lockType) {
        return false;
    }

    @Override
    public int sectorSize() {
        return 0;
    }

    @Override
    public boolean unlock(@Nonnull SqlJetLockType lockType) {
        return false;
    }

}
