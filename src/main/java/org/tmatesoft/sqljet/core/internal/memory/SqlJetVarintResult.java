package org.tmatesoft.sqljet.core.internal.memory;

public class SqlJetVarintResult {
	private final int offset;
	private final long value;
	public SqlJetVarintResult(int offset, long value) {
		this.offset = offset;
		this.value = value;
	}
	public int getOffset() {
		return offset;
	}
	public long getValue() {
		return value;
	}
	public SqlJetVarintResult32 to32() {
		return new SqlJetVarintResult32(offset, (int)value);
	}
}
