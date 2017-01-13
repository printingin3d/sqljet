package org.tmatesoft.sqljet.core.internal.memory;

import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryBuffer;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;

public abstract class SqlJetAbstractMemoryBuffer implements ISqlJetMemoryBuffer {

	@Override
	public final ISqlJetMemoryPointer getPointer(int pointer) {
        assert (pointer >= 0);
        assert (pointer <= getSize());

        return new SqlJetMemoryPointer(this, pointer);
	}

	@Override
	public final int getByteUnsigned(int pointer) {
        assert (pointer >= 0);
        assert (pointer < getSize());

        return SqlJetBytesUtility.toUnsignedByte(getByte(pointer));
	}

	@Override
	public final void putByteUnsigned(int pointer, int value) {
		putByte(pointer, (byte) SqlJetBytesUtility.toUnsignedByte(value));
	}

	@Override
	public final int getShortUnsigned(int pointer) {
		return SqlJetBytesUtility.toUnsignedShort(getShort(pointer));
	}

	@Override
	public final void putShortUnsigned(int pointer, int value) {
		putShort(pointer, (short) SqlJetBytesUtility.toUnsignedShort(value));
	}

	@Override
	public final long getIntUnsigned(int pointer) {
		return SqlJetBytesUtility.toUnsignedInt(getInt(pointer));
	}

	@Override
	public final void putIntUnsigned(int pointer, long value) {
		putInt(pointer, (int) SqlJetBytesUtility.toUnsignedInt(value));
	}
}
