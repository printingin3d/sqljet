/**
 * SqlJetByteBuffer.java
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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryBuffer;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class SqlJetByteBuffer extends SqlJetAbstractMemoryBuffer implements ISqlJetMemoryBuffer {
    protected final ByteBuffer buffer;

    @SuppressWarnings("null")
	public SqlJetByteBuffer(int size) {
    	this(ByteBuffer.allocate(size));
    }
    
    @SuppressWarnings("null")
	public SqlJetByteBuffer(byte[] bytes) {
    	this(ByteBuffer.wrap(bytes));
    }

    /**
     * @param b
     */
    public SqlJetByteBuffer(@Nonnull ByteBuffer b) {
        buffer = b;
    }

    @Override
	public int getSize() {
        return buffer.capacity();
    }

    @Override
	public byte getByte(int pointer) {
        return buffer.get(pointer);
    }

    @Override
	public int getInt(int pointer) {
        return buffer.getInt(pointer);
    }

    @Override
	public long getLong(int pointer) {
        return buffer.getLong(pointer);
    }

    @Override
	public short getShort(int pointer) {
        return buffer.getShort(pointer);
    }

    @Override
	public void putByte(int pointer, byte value) {
        buffer.put(pointer, value);
    }

    @Override
	public void putInt(int pointer, int value) {
        buffer.putInt(pointer, value);
    }

    @Override
	public void putLong(int pointer, long value) {
        buffer.putLong(pointer, value);
    }

    @Override
	public void putShort(int pointer, short value) {
        buffer.putShort(pointer, value);
    }

    @Override
	public int readFromFile(int pointer, @Nonnull RandomAccessFile file, @Nonnull FileChannel channel, long position, int count) 
			throws IOException {
        assert pointer >= 0;
        assert pointer < buffer.capacity();
        assert position >= 0;
        assert count > 0;

        buffer.limit(pointer + count).position(pointer);
        try {
            return channel.read(buffer, position);
        } finally {
            buffer.clear();
        }
    }

    @Override
	public int writeToFile(int pointer, @Nonnull RandomAccessFile file, @Nonnull FileChannel channel, long position, int count) throws IOException {
        assert pointer >= 0;
        assert pointer < buffer.capacity();
        assert position >= 0;
        assert count > 0;

        buffer.limit(pointer + count).position(pointer);
        try {
            return channel.write(buffer, position);
        } finally {
            buffer.clear();
        }
    }

    @Override
	public byte[] asArray() {
        return buffer.array();
    }

    @Override
	public void copyFrom(int dstPos, ISqlJetMemoryBuffer src, int srcPos, int count) {
        if (src instanceof SqlJetByteBuffer&& !(src instanceof SqlJetDirectByteBuffer)) {
            final SqlJetByteBuffer srcBuf = (SqlJetByteBuffer) src;
            System.arraycopy(srcBuf.buffer.array(), srcPos, buffer.array(), dstPos, count);
        } else {
            final byte[] b = new byte[count];
            src.getBytes(srcPos, b, 0, count);
            putBytes(dstPos, b, 0, count);
        }
    }

    @Override
	public void fill(int from, int count, byte value) {
        Arrays.fill(buffer.array(), from, from + count, value);
    }

    @Override
	public void getBytes(int pointer, byte[] bytes, int to, int count) {
        System.arraycopy(buffer.array(), pointer, bytes, to, count);
    }

    @Override
	public void putBytes(int pointer, byte[] bytes, int from, int count) {
        System.arraycopy(bytes, from, buffer.array(), pointer, count);
    }

    @Override
	public int compareTo(int pointer, ISqlJetMemoryBuffer buffer, int bufferPointer) {
        final int thisCount = getSize() - pointer;
        final int bufferCount = buffer.getSize() - bufferPointer;
        if (thisCount != bufferCount) {
            if (thisCount > bufferCount) {
                return 1;
            } else {
                return -1;
            }
        }
        if (buffer instanceof SqlJetByteBuffer && !(buffer instanceof SqlJetDirectByteBuffer)) {
            final SqlJetByteBuffer b = (SqlJetByteBuffer) buffer;
            return SqlJetUtility.memcmp(this.buffer.array(), pointer, b.buffer.array(), bufferPointer, thisCount);
        } else {
            final byte[] b = new byte[thisCount];
            buffer.getBytes(bufferPointer, b, 0, thisCount);
            return SqlJetUtility.memcmp(this.buffer.array(), pointer, b, bufferPointer, thisCount);
        }
    }

}
