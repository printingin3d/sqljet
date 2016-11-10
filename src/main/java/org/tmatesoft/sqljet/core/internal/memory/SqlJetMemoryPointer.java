/**
 * SqlJetMemoryPointer.java
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
package org.tmatesoft.sqljet.core.internal.memory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryBuffer;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public final class SqlJetMemoryPointer implements ISqlJetMemoryPointer {

    private ISqlJetMemoryBuffer buffer;
    private int pointer;
    private int limit;

    public SqlJetMemoryPointer(ISqlJetMemoryBuffer buffer, int pointer) {
        assert (buffer != null);
        assert (buffer.isAllocated());
        assert (pointer >= 0);
        assert (pointer <= buffer.getSize());

        this.buffer = buffer;
        this.pointer = pointer;
        this.limit = buffer.getSize();
    }

    public SqlJetMemoryPointer(ISqlJetMemoryBuffer buffer, int pointer, int limit) {
        assert (buffer != null);
        assert (buffer.isAllocated());
        assert (pointer >= 0);
        assert (pointer <= buffer.getSize());

        this.buffer = buffer;
        this.pointer = pointer;
        this.limit = limit;
    }

    @Override
	final public ISqlJetMemoryBuffer getBuffer() {
        return buffer;
    }

    @Override
	final public int getPointer() {
        return pointer;
    }

    @Override
	final public void setPointer(int pointer) {
        assert (buffer != null);
        assert (buffer.isAllocated());

        this.pointer = pointer;
    }

    @Override
	final public void movePointer(int count) {
        assert (buffer != null);
        assert (buffer.isAllocated());
        assert (pointer + count >= 0);
        assert (pointer + count <= buffer.getSize());

        pointer += count;
    }

	@Override
	public ISqlJetMemoryPointer pointer(int pos) {
		return getBuffer().getPointer(getAbsolute(pos));
	}

    @Override
	final public byte getByte() {
        assert (buffer != null);
        assert (buffer.isAllocated());

        return buffer.getByte(pointer);
    }

    @Override
	final public int getInt() {
        assert (buffer != null);
        assert (buffer.isAllocated());

        return buffer.getInt(pointer);
    }

    @Override
	final public long getLong() {
        assert (buffer != null);
        assert (buffer.isAllocated());

        return buffer.getLong(pointer);
    }

    @Override
	final public short getShort() {
        assert (buffer != null);
        assert (buffer.isAllocated());

        return buffer.getShort(pointer);
    }

    @Override
	final public int getByteUnsigned() {
        assert (buffer != null);
        assert (buffer.isAllocated());

        return buffer.getByteUnsigned(pointer);
    }

    @Override
	final public long getIntUnsigned() {
        assert (buffer != null);
        assert (buffer.isAllocated());

        return buffer.getIntUnsigned(pointer);
    }

    @Override
	final public int getShortUnsigned() {
        assert (buffer != null);
        assert (buffer.isAllocated());

        return buffer.getShortUnsigned(pointer);
    }

    @Override
	final public void putByte(byte value) {
        assert (buffer != null);
        assert (buffer.isAllocated());

        buffer.putByte(pointer, value);
    }

    @Override
	final public void putInt(int value) {
        assert (buffer != null);
        assert (buffer.isAllocated());

        buffer.putInt(pointer, value);
    }

    @Override
	final public void putLong(long value) {
        assert (buffer != null);
        assert (buffer.isAllocated());

        buffer.putLong(pointer, value);
    }

    @Override
	final public void putShort(short value) {
        assert (buffer != null);
        assert (buffer.isAllocated());

        buffer.putShort(pointer, value);
    }

    @Override
	final public void putByteUnsigned(int value) {
        assert (buffer != null);
        assert (buffer.isAllocated());

        buffer.putByteUnsigned(pointer, value);
    }

    @Override
	final public void putIntUnsigned(long value) {
        assert (buffer != null);
        assert (buffer.isAllocated());

        buffer.putIntUnsigned(pointer, value);
    }

    @Override
	final public void putShortUnsigned(int value) {
        assert (buffer != null);
        assert (buffer.isAllocated());

        buffer.putShortUnsigned(pointer, value);
    }

    @Override
	final public int readFromFile(RandomAccessFile file, FileChannel channel, long position, int count) throws IOException {
        assert (buffer != null);
        assert (buffer.isAllocated());
        assert (file != null);
        assert (channel != null);
        assert (position >= 0);
        assert (count > 0);
        assert (pointer + count <= buffer.getSize());

        return buffer.readFromFile(pointer, file, channel, position, count);
    }

    @Override
	final public int writeToFile(RandomAccessFile file, FileChannel channel, long position, int count) throws IOException {
        assert (buffer != null);
        assert (file != null);
        assert (channel != null);
        assert (position >= 0);
        assert (count > 0);
        assert (pointer + count <= buffer.getSize());

        return buffer.writeToFile(pointer, file, channel, position, count);
    }

    @Override
	final public int getAbsolute(int pointer) {
        return this.pointer + pointer;
    }

    @Override
	final public byte getByte(int pointer) {
        return buffer.getByte(getAbsolute(pointer));
    }

    @Override
	final public int getByteUnsigned(int pointer) {
        return buffer.getByteUnsigned(getAbsolute(pointer));
    }

    @Override
	final public int getInt(int pointer) {
        return buffer.getInt(getAbsolute(pointer));
    }

    @Override
	final public long getIntUnsigned(int pointer) {
        return buffer.getIntUnsigned(getAbsolute(pointer));
    }

    @Override
	final public long getLong(int pointer) {
        return buffer.getLong(getAbsolute(pointer));
    }

    @Override
	final public short getShort(int pointer) {
        return buffer.getShort(getAbsolute(pointer));
    }

    @Override
	final public int getShortUnsigned(int pointer) {
        return buffer.getShortUnsigned(getAbsolute(pointer));
    }

    @Override
	final public void putByte(int pointer, byte value) {
        buffer.putByte(getAbsolute(pointer), value);
    }

    @Override
	final public ISqlJetMemoryPointer putByteUnsigned(int pointer, int value) {
        buffer.putByteUnsigned(getAbsolute(pointer), value);
        return this;
    }

    @Override
	final public void putInt(int pointer, int value) {
        buffer.putInt(getAbsolute(pointer), value);
    }

    @Override
	final public void putIntUnsigned(int pointer, long value) {
        buffer.putIntUnsigned(getAbsolute(pointer), value);
    }

    @Override
	final public void putLong(int pointer, long value) {
        buffer.putLong(getAbsolute(pointer), value);
    }

    @Override
	final public void putShort(int pointer, short value) {
        buffer.putShort(getAbsolute(pointer), value);
    }

    @Override
	final public void putShortUnsigned(int pointer, int value) {
        buffer.putShortUnsigned(getAbsolute(pointer), value);
    }

    @Override
	final public int readFromFile(int pointer, RandomAccessFile file, FileChannel channel, long position, int count) throws IOException {
        return buffer.readFromFile(getAbsolute(pointer), file, channel, position, count);
    }

    @Override
	final public int writeToFile(int pointer, RandomAccessFile file, FileChannel channel, long position, int count) throws IOException {
        return buffer.writeToFile(getAbsolute(pointer), file, channel, position, count);
    }

    @Override
	final public int remaining() {
        return limit - pointer;
    }

    @Override
	final public void copyFrom(int dstPos, ISqlJetMemoryPointer src, int srcPos, int length) {
        buffer.copyFrom(getAbsolute(dstPos), src.getBuffer(), src.getAbsolute(srcPos), length);
    }

    @Override
	final public void copyFrom(ISqlJetMemoryPointer src, int srcPos, int length) {
        buffer.copyFrom(pointer, src.getBuffer(), src.getAbsolute(srcPos), length);
    }

    @Override
	final public void copyFrom(ISqlJetMemoryPointer src, int length) {
        buffer.copyFrom(pointer, src.getBuffer(), src.getPointer(), length);
    }

    @Override
	final public void fill(int count, byte value) {
        buffer.fill(pointer, count, value);
    }

    @Override
	final public void fill(int from, int count, byte value) {
        buffer.fill(getAbsolute(from), count, value);
    }

    @Override
	final public byte[] getBytes() {
        byte[] bytes = new byte[remaining()];
        buffer.getBytes(pointer, bytes, 0, bytes.length);
        return bytes;
    }

    @Override
	final public void getBytes(int pointer, byte[] bytes) {
        buffer.getBytes(getAbsolute(pointer), bytes, 0, bytes.length);
    }

    @Override
	final public void getBytes(int pointer, byte[] bytes, int count) {
        buffer.getBytes(getAbsolute(pointer), bytes, 0, count);
    }

    @Override
	final public void putBytes(byte[] bytes) {
        buffer.putBytes(pointer, bytes, 0, bytes.length);
    }

    @Override
	final public void putBytes(int pointer, byte[] bytes, int count) {
        buffer.putBytes(getAbsolute(pointer), bytes, 0, count);
    }

    @Override
	final public void limit(int n) {
        this.limit = n;
    }
    
    @Override
	final public int getLimit() {
        return this.limit;
    }

    @Override
	public ISqlJetMemoryPointer getMoved(int count) {
    	return new SqlJetMemoryPointer(buffer, pointer + count, limit);
    }
    

    /**
     * Read a 64-bit variable-length integer from memory starting at p[0].
     * Return the number of bytes read. The value is stored in *v.
     */
    @Override
	public byte getVarint(long[] v) {
        return getVarint(0, v);
    }

    @Override
	public byte getVarint(int offset, long[] v) {
        long l = 0;
        for (byte i = 0; i < 8; i++) {
            final int b = getByteUnsigned(i + offset);
            l = (l << 7) | (b & 0x7f);
            if ((b & 0x80) == 0) {
                v[0] = l;
                return ++i;
            }
        }
        final int b = getByteUnsigned(8 + offset);
        l = (l << 8) | b;
        v[0] = l;
        return 9;
    }

    /**
     * Read a 32-bit variable-length integer from memory starting at p[0].
     * Return the number of bytes read. The value is stored in *v. A MACRO
     * version, getVarint32, is provided which inlines the single-byte case. All
     * code should use the MACRO version as this function assumes the
     * single-byte case has already been handled.
     *
     * @throws SqlJetExceptionRemove
     */
    @Override
	public byte getVarint32(int[] v) {
        return getVarint32(0, v);
    }

    @Override
	public byte getVarint32(int offset, int[] v) {
        int x = getByteUnsigned(0 + offset);
        if (x < 0x80) {
            v[0] = x;
            return 1;
        }

        int a, b;
        int i = 0;

        a = getByteUnsigned(i + offset);
        /* a: p0 (unmasked) */
        if ((a & 0x80) == 0) {
            v[0] = a;
            return 1;
        }

        i++;
        b = getByteUnsigned(i + offset);
        /* b: p1 (unmasked) */
        if ((b & 0x80) == 0) {
            a &= 0x7f;
            a = a << 7;
            v[0] = a | b;
            return 2;
        }

        i++;
        a = a << 14;
        a |= getByteUnsigned(i + offset);
        /* a: p0<<14 | p2 (unmasked) */
        if ((a & 0x80) == 0) {
            a &= (0x7f << 14) | (0x7f);
            b &= 0x7f;
            b = b << 7;
            v[0] = a | b;
            return 3;
        }

        i++;
        b = b << 14;
        b |= getByteUnsigned(i + offset);
        /* b: p1<<14 | p3 (unmasked) */
        if ((b & 0x80) == 0) {
            b &= (0x7f << 14) | (0x7f);
            a &= (0x7f << 14) | (0x7f);
            a = a << 7;
            v[0] = a | b;
            return 4;
        }

        i++;
        a = a << 14;
        a |= getByteUnsigned(i + offset);
        /* a: p0<<28 | p2<<14 | p4 (unmasked) */
        if ((a & 0x80) == 0) {
            a &= (0x7f << 28) | (0x7f << 14) | (0x7f);
            b &= (0x7f << 28) | (0x7f << 14) | (0x7f);
            b = b << 7;
            v[0] = a | b;
            return 5;
        }

        /*
         * We can only reach this point when reading a corrupt database file. In
         * that case we are not in any hurry. Use the (relatively slow)
         * general-purpose sqlite3GetVarint() routine to extract the value.
         */
        {
            long[] v64 = new long[1];
            byte n;

            i -= 4;
            n = getVarint(offset, v64);
            assert (n > 5 && n <= 9);
            v[0] = (int) v64[0];
            return n;
        }
    }

    @Override
	public int putVarint(long v) {
        int i, j, n;
        if ((v & (((long) 0xff000000) << 32)) != 0) {
            putByteUnsigned(8, (byte) v);
            v >>= 8;
            for (i = 7; i >= 0; i--) {
                putByteUnsigned(i, (byte) ((v & 0x7f) | 0x80));
                v >>= 7;
            }
            return 9;
        }
        n = 0;
        byte[] buf = new byte[10];
        do {
            buf[n++] = (byte) ((v & 0x7f) | 0x80);
            v >>= 7;
        } while (v != 0);
        buf[0] &= 0x7f;
        assert (n <= 9);
        for (i = 0, j = n - 1; j >= 0; j--, i++) {
            putByteUnsigned(i, buf[j]);
        }
        return n;
    }

    @Override
	public int putVarint32(int v) {
        if (v < 0x80) {
            putByteUnsigned(0, (byte) v);
            return 1;
        }

        if ((v & ~0x7f) == 0) {
            putByteUnsigned(0, (byte) v);
            return 1;
        }
        if ((v & ~0x3fff) == 0) {
            putByteUnsigned(0, (byte) ((v >> 7) | 0x80));
            putByteUnsigned(1, (byte) (v & 0x7f));
            return 2;
        }
        return putVarint(v);
    }

    @Override
	public int strlen30() {
        int i = 0;
        while (i < pointer && getByteUnsigned(i) != 0) {
			i++;
		}
        return 0x3fffffff & (i);
    }
    
}
