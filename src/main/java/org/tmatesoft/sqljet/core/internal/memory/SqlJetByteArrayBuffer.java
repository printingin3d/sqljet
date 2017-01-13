/**
 * SqlJetByteArrayBuffer.java
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
import java.util.Arrays;

import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryBuffer;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryManager;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class SqlJetByteArrayBuffer extends SqlJetAbstractMemoryBuffer implements ISqlJetMemoryBuffer {
    private final byte[] buffer;

    public SqlJetByteArrayBuffer(int size) {
        assert (size >= 0);

        this.buffer = new byte[size];
    }
    
    public SqlJetByteArrayBuffer(byte[] bytes) {
    	assert(bytes != null);
    	
    	this.buffer = bytes;
    }

    @Override
	public int getSize() {
        return buffer.length;
    }

    @Override
	public byte getByte(final int pointer) {
        assert (pointer >= 0);
        assert (pointer < buffer.length);

        return buffer[pointer];
    }

    @Override
	public int getInt(final int pointer) {
        assert (pointer >= 0);
        assert (pointer <= buffer.length - ISqlJetMemoryManager.INT_SIZE);

        return SqlJetBytesUtility.getInt(buffer, pointer);
    }

    @Override
	public long getLong(final int pointer) {
        assert (pointer >= 0);
        assert (pointer <= buffer.length - ISqlJetMemoryManager.LONG_SIZE);

        return SqlJetBytesUtility.getLong(buffer, pointer);
    }

    @Override
	public short getShort(final int pointer) {
        assert (pointer >= 0);
        assert (pointer <= buffer.length - ISqlJetMemoryManager.SHORT_SIZE);

        return SqlJetBytesUtility.getShort(buffer, pointer);
    }

    @Override
	public void putByte(final int pointer, final byte value) {
        assert (pointer >= 0);
        assert (pointer < buffer.length);

        buffer[pointer] = value;
    }

    @Override
	public void putInt(final int pointer, final int value) {
        assert (pointer >= 0);
        assert (pointer <= buffer.length - ISqlJetMemoryManager.INT_SIZE);

        SqlJetBytesUtility.putInt(buffer, pointer, value);
    }

    @Override
	public void putLong(final int pointer, final long value) {
        assert (pointer >= 0);
        assert (pointer <= buffer.length - ISqlJetMemoryManager.LONG_SIZE);

        SqlJetBytesUtility.putLong(buffer, pointer, value);
    }

    @Override
	public void putShort(final int pointer, final short value) {
        assert (pointer >= 0);
        assert (pointer <= buffer.length - ISqlJetMemoryManager.SHORT_SIZE);

        SqlJetBytesUtility.putShort(buffer, pointer, value);
    }

    @Override
	public int readFromFile(final int pointer, final RandomAccessFile file, final FileChannel channel, final long position, final int count)
            throws IOException {
        assert (pointer >= 0);
        assert (pointer < buffer.length);
        assert (file != null);
        assert (position >= 0);
        assert (count > 0);

        file.seek(position);
        return file.read(buffer, pointer, count);
    }

    @Override
	public int writeToFile(final int pointer, final RandomAccessFile file, final FileChannel channel, final long position, final int count)
            throws IOException {
        assert (pointer >= 0);
        assert (pointer < buffer.length);
        assert (file != null);
        assert (position >= 0);
        assert (count > 0);

        file.seek(position);
        file.write(buffer, pointer, count);
        return count;
    }

    @Override
	public byte[] asArray() {
        return buffer;
    }

    @Override
	public void copyFrom(int dstPos, ISqlJetMemoryBuffer src, int srcPos, int count) {
        if (src instanceof SqlJetByteArrayBuffer) {
            final SqlJetByteArrayBuffer srcBuf = (SqlJetByteArrayBuffer) src;
            System.arraycopy(srcBuf.buffer, srcPos, buffer, dstPos, count);
        } else {
            final byte[] b = new byte[count];
            src.getBytes(srcPos, b, 0, count);
            putBytes(dstPos, b, 0, count);
        }
    }

    @Override
	public void fill(int from, int count, byte value) {
        Arrays.fill(buffer, from, from + count, value);
    }

    @Override
	public void getBytes(int pointer, byte[] bytes, int to, int count) {
        System.arraycopy(buffer, pointer, bytes, to, count);
    }

    @Override
	public void putBytes(int pointer, byte[] bytes, int from, int count) {
        System.arraycopy(bytes, from, buffer, pointer, count);
    }

    @Override
	public int compareTo(int pointer, ISqlJetMemoryBuffer buffer, int bufferPointer) {
        final int thisCount = getSize() - pointer;
        final int bufferCount = buffer.getSize() - bufferPointer;
        final int count = thisCount > bufferCount ? bufferCount : thisCount;
        if (buffer instanceof SqlJetByteArrayBuffer) {
            final SqlJetByteArrayBuffer b = (SqlJetByteArrayBuffer) buffer;
            final int cmp = SqlJetUtility.memcmp(this.buffer, pointer, b.buffer, bufferPointer, count);
            if (cmp != 0) {
                return cmp;
            }
        } else {
            final byte[] b = new byte[thisCount];
            buffer.getBytes(bufferPointer, b, 0, thisCount);
            final int cmp = SqlJetUtility.memcmp(this.buffer, pointer, b, bufferPointer, count);
            if (cmp != 0) {
                return cmp;
            }
        }
        if (thisCount != bufferCount) {
            if (thisCount > bufferCount) {
                return 1;
            } else {
                return -1;
            }
        } else {
            return 0;
        }
    }
}
