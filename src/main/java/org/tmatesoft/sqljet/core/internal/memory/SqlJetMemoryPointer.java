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

	private final ISqlJetMemoryBuffer buffer;
	private int pointer;
	private final int limit;

	public SqlJetMemoryPointer(ISqlJetMemoryBuffer buffer, int pointer) {
		this(buffer, pointer, buffer.getSize());
	}

	public SqlJetMemoryPointer(ISqlJetMemoryBuffer buffer, int pointer, int limit) {
		assert buffer != null;
		assert pointer >= 0;
		assert pointer <= buffer.getSize();

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
	final public void movePointer(int count) {
		assert pointer + count >= 0;
		assert pointer + count <= buffer.getSize();

		pointer += count;
	}

	@Override
	public ISqlJetMemoryPointer pointer(int pos) {
		return buffer.getPointer(getAbsolute(pos));
	}

	@Override
	public ISqlJetMemoryPointer pointer(int pos, int limit) {
		return new SqlJetMemoryPointer(buffer, getAbsolute(pos), getAbsolute(pos + limit));
	}

	@Override
	public ISqlJetMemoryPointer getMoved(int count) {
		return new SqlJetMemoryPointer(buffer, pointer + count, limit);
	}

	@Override
	final public int getInt() {
		return buffer.getInt(pointer);
	}

	@Override
	final public long getLong() {
		return buffer.getLong(pointer);
	}

	@Override
	final public int getByteUnsigned() {
		return buffer.getByteUnsigned(pointer);
	}

	@Override
	final public long getIntUnsigned() {
		return buffer.getIntUnsigned(pointer);
	}

	@Override
	final public int getShortUnsigned() {
		return buffer.getShortUnsigned(pointer);
	}

	@Override
	final public void putInt(int value) {
		buffer.putInt(pointer, value);
	}

	@Override
	final public void putLong(long value) {
		buffer.putLong(pointer, value);
	}

	@Override
	final public void putByteUnsigned(int value) {
		buffer.putByteUnsigned(pointer, value);
	}

	@Override
	final public void putIntUnsigned(long value) {
		buffer.putIntUnsigned(pointer, value);
	}

	@Override
	final public void putShortUnsigned(int value) {
		buffer.putShortUnsigned(pointer, value);
	}

	@Override
	final public int readFromFile(RandomAccessFile file, FileChannel channel, long position, int count)
			throws IOException {
		assert file != null;
		assert channel != null;
		assert position >= 0;
		assert count > 0;
		assert pointer + count <= buffer.getSize();

		return buffer.readFromFile(pointer, file, channel, position, count);
	}

	@Override
	final public int writeToFile(RandomAccessFile file, FileChannel channel, long position, int count)
			throws IOException {
		assert file != null;
		assert channel != null;
		assert position >= 0;
		assert count > 0;
		assert pointer + count <= buffer.getSize();

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
	final public int remaining() {
		return limit - pointer;
	}

	@Override
	final public void copyFrom(int dstPos, ISqlJetMemoryPointer src, int srcPos, int length) {
		buffer.copyFrom(getAbsolute(dstPos), src.getBuffer(), src.getAbsolute(srcPos), length);
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
	final public void putBytes(byte[] bytes) {
		buffer.putBytes(pointer, bytes, 0, bytes.length);
	}

	@Override
	final public int getLimit() {
		return limit - pointer;
	}

	/**
	 * Read a 64-bit variable-length integer from memory starting at p[0].
	 * Return the number of bytes read. The value is stored in *v.
	 */
	@Override
	public SqlJetVarintResult getVarint() {
		return getVarint(0);
	}

	@Override
	public SqlJetVarintResult getVarint(int offset) {
		long l = 0;
		for (byte i = 0; i < 8; i++) {
			final int b = getByteUnsigned(i + offset);
			l = l << 7 | b & 0x7f;
			if ((b & 0x80) == 0) {
				return new SqlJetVarintResult(++i, l);
			}
		}
		final int b = getByteUnsigned(8 + offset);
		l = l << 8 | b;
		return new SqlJetVarintResult(9, l);
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
	public SqlJetVarintResult32 getVarint32() {
		return getVarint32(0);
	}

	@Override
	public SqlJetVarintResult32 getVarint32(int offset) {
		int i = offset;
		int x = getByteUnsigned(i);
		if (x < 0x80) {
			return new SqlJetVarintResult32(1, x);
		}

		int a, b;

		a = getByteUnsigned(i);
		/* a: p0 (unmasked) */
		if ((a & 0x80) == 0) {
			return new SqlJetVarintResult32(1, a);
		}

		i++;
		b = getByteUnsigned(i);
		/* b: p1 (unmasked) */
		if ((b & 0x80) == 0) {
			a &= 0x7f;
			a = a << 7;
			return new SqlJetVarintResult32(2, a | b);
		}

		i++;
		a = a << 14;
		a |= getByteUnsigned(i);
		/* a: p0<<14 | p2 (unmasked) */
		if ((a & 0x80) == 0) {
			a &= 0x7f << 14 | 0x7f;
			b &= 0x7f;
			b = b << 7;
			return new SqlJetVarintResult32(3, a | b);
		}

		i++;
		b = b << 14;
		b |= getByteUnsigned(i);
		/* b: p1<<14 | p3 (unmasked) */
		if ((b & 0x80) == 0) {
			b &= 0x7f << 14 | 0x7f;
			a &= 0x7f << 14 | 0x7f;
			a = a << 7;
			return new SqlJetVarintResult32(4, a | b);
		}

		i++;
		a = a << 14;
		a |= getByteUnsigned(i);
		/* a: p0<<28 | p2<<14 | p4 (unmasked) */
		if ((a & 0x80) == 0) {
			a &= 0x7f << 28 | 0x7f << 14 | 0x7f;
			b &= 0x7f << 28 | 0x7f << 14 | 0x7f;
			b = b << 7;
			return new SqlJetVarintResult32(5, a | b);
		}

		/*
		 * We can only reach this point when reading a corrupt database file. In
		 * that case we are not in any hurry. Use the (relatively slow)
		 * general-purpose sqlite3GetVarint() routine to extract the value.
		 */
		return getVarint(offset).to32();
	}

	@Override
	public int putVarint(long v) {
		return putVarint(0, v);
	}
	
	@Override
	public int putVarint(int pointer, long v) {
		int i, j, n;
		if ((v & (long) 0xff000000 << 32) != 0) {
			putByteUnsigned(pointer+8, (byte) v);
			v >>= 8;
		for (i = 7; i >= 0; i--) {
			putByteUnsigned(pointer+i, (byte) (v & 0x7f | 0x80));
			v >>= 7;
		}
		return 9;
		}
		n = 0;
		byte[] buf = new byte[10];
		do {
			buf[n++] = (byte) (v & 0x7f | 0x80);
			v >>= 7;
		} while (v != 0);
		buf[0] &= 0x7f;
		assert n <= 9;
		for (i = 0, j = n - 1; j >= 0; j--, i++) {
			putByteUnsigned(pointer+i, buf[j]);
		}
		return n;
	}

	@Override
	public int putVarint32(int pointer, int v) {
		if (v < 0x80) {
			putByteUnsigned(pointer, (byte) v);
			return 1;
		}

		if ((v & ~0x7f) == 0) {
			putByteUnsigned(pointer, (byte) v);
			return 1;
		}
		if ((v & ~0x3fff) == 0) {
			putByteUnsigned(pointer, (byte) (v >> 7 | 0x80));
			putByteUnsigned(pointer+1, (byte) (v & 0x7f));
			return 2;
		}
		return putVarint(pointer, v);
	}

}
