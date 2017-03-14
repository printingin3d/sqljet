/**
 * ISqlJetPointer.java
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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.internal.memory.SqlJetVarintResult;
import org.tmatesoft.sqljet.core.internal.memory.SqlJetVarintResult32;

/**
 * Pointer in SqlJet's memory buffer.
 *
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public interface ISqlJetMemoryPointer {

    /**
     * Get buffer which contains pointer.
     *
     * @return
     */
    ISqlJetMemoryBuffer getBuffer();

    /**
     * Get pointer address (offset in buffer).
     *
     * @return
     */
    int getPointer();

    /**
     * Move pointer. Add some count to pointer address. Count may be negative.
     *
     * @param count
     *            count which added to address. May be negative.
     */
    void movePointer(int count);
    
    /**
     * Implements address arithmetic on byte buffer.
     *
     * @param p
     * @param pos
     * @return
     */
    @Nonnull ISqlJetMemoryPointer pointer(int pos);
    
    /**
     * Implements address arithmetic on byte buffer.
     *
     * @param p
     * @param pos
     * @return
     */
    @Nonnull ISqlJetMemoryPointer pointer(int pos, int limit);

    /**
     * Read int at current address.
     *
     * @return
     */
    int getInt();

    /**
     * Write int at current address.
     *
     * @param value
     */
    void putInt(int value);

    /**
     * Read unsigned byte at current address.
     *
     * @return
     */
    int getByteUnsigned();

    /**
     * Write unsigned byte at current address.
     *
     * @param value
     */
    void putByteUnsigned(int value);

    /**
     * Read unsigned short at current address.
     *
     * @return
     */
    int getShortUnsigned();

    /**
     * Write unsigned short at current address.
     *
     * @param value
     */
    void putShortUnsigned(int value);

    /**
     * Read unsigned int at current address.
     *
     * @return
     */
    long getIntUnsigned();

    /**
     * Write unsigned int at current address.
     *
     * @param value
     */
    void putIntUnsigned(long value);

    /**
     * Read from file at current address.
     *
     * @param file
     * @param position
     * @param count
     * @return
     * @throws IOException
     */
    int readFromFile(@Nonnull RandomAccessFile file, @Nonnull FileChannel channel, long position, int count) throws IOException;

    /**
     * Write to file at current address.
     *
     * @param file
     * @param position
     * @param count
     * @return
     * @throws IOException
     */
    int writeToFile(@Nonnull RandomAccessFile file, @Nonnull FileChannel channel, long position, int count) throws IOException;

    /**
     * Read byte at pointer.
     *
     * @param pointer
     * @return
     */
    byte getByte(int pointer);

    /**
     * Write byte at pointer.
     *
     * @param pointer
     * @param value
     */
    void putByte(int pointer, byte value);

    /**
     * Read int at pointer.
     *
     * @param pointer
     * @return
     */
    int getInt(int pointer);

    /**
     * Write int at pointer.
     *
     * @param pointer
     * @param value
     */
    void putInt(int pointer, int value);

    /**
     * Read long at pointer.
     *
     * @param pointer
     * @return
     */
    long getLong(int pointer);

    /**
     * Write long at pointer.
     *
     * @param pointer
     * @param value
     */
    void putLong(int pointer, long value);

    /**
     * Read unsigned byte at pointer.
     *
     * @param pointer
     * @return
     */
    int getByteUnsigned(int pointer);

    /**
     * Write unsigned byte at pointer.
     *
     * @param pointer
     * @param value
     */
    ISqlJetMemoryPointer putByteUnsigned(int pointer, int value);

    /**
     * Read unsigned short at pointer.
     *
     * @param pointer
     * @return
     */
    int getShortUnsigned(int pointer);

    /**
     * Write unsigned short at pointer.
     *
     * @param pointer
     * @param value
     */
    void putShortUnsigned(int pointer, int value);

    /**
     * Read unsigned int at pointer.
     *
     * @param pointer
     * @return
     */
    long getIntUnsigned(int pointer);

    /**
     * Write unsigned int at pointer.
     *
     * @param pointer
     * @param value
     */
    void putIntUnsigned(int pointer, long value);

    /**
     * @return
     */
    int remaining();

    /**
     * @param pointer
     * @return
     */
    int getAbsolute(int pointer);
    
    void copyFrom(int dstPos, ISqlJetMemoryPointer src, int srcPos, int length);

    void copyFrom(ISqlJetMemoryPointer src, int length);

    /**
     * @param from
     * @param count
     * @param value
     */
    void fill(int from, int count, byte value);
    /**
     * @param from
     * @param count
     * @param value
     */
    void fill(int count, byte value);

    /**
     * @param bytes
     */
    byte[] getBytes();

    /**
     * @param bytes
     */
    void putBytes(byte[] bytes);

    int getLimit();

    @Nonnull ISqlJetMemoryPointer getMoved(int count);

    SqlJetVarintResult32 getVarint32(int offset);
    /**
     * Read a 32-bit variable-length integer from memory starting at p[0].
     * Return the number of bytes read. The value is stored in *v. A MACRO
     * version, getVarint32, is provided which inlines the single-byte case. All
     * code should use the MACRO version as this function assumes the
     * single-byte case has already been handled.
     *
     * @throws SqlJetExceptionRemove
     */
    SqlJetVarintResult32 getVarint32();
    
    SqlJetVarintResult getVarint(int offset);
    /**
     * Read a 64-bit variable-length integer from memory starting at p[0].
     * Return the number of bytes read. The value is stored in *v.
     */
    SqlJetVarintResult getVarint();
    
    /**
     * <p>Write a 64-bit variable-length integer to memory starting at p[0]. The
     * length of data write will be between 1 and 9 bytes. The number of bytes
     * written is returned.
     *
     * A variable-length integer consists of the lower 7 bits of each byte for
     * all bytes that have the 8th bit set and one byte with the 8th bit clear.
     * Except, if we get to the 9th byte, it stores the full 8 bits and is the
     * last byte.</p>
     * 
     * <p>
     * The variable-length integer encoding is as follows:
     *
     * KEY: A = 0xxxxxxx 7 bits of data and one flag bit B = 1xxxxxxx 7 bits of
     * data and one flag bit C = xxxxxxxx 8 bits of data
     *
     * 7 bits - A 14 bits - BA 21 bits - BBA 28 bits - BBBA 35 bits - BBBBA 42
     * bits - BBBBBA 49 bits - BBBBBBA 56 bits - BBBBBBBA 64 bits - BBBBBBBBC</p>
     */
    int putVarint(long v);
    int putVarint(int pointer, long v);
    
    /**
     * This routine is a faster version of sqlite3PutVarint() that only works
     * for 32-bit positive integers and which is optimized for the common case
     * of small integers. A MACRO version, putVarint32, is provided which
     * inlines the single-byte case. All code should use the MACRO version as
     * this function assumes the single-byte case has already been handled.
     */
    public int putVarint32(int pointer, int v);
}
