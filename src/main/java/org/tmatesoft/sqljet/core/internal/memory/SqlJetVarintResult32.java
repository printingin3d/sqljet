package org.tmatesoft.sqljet.core.internal.memory;

public class SqlJetVarintResult32 {
    private final int offset;
    private final int value;

    public SqlJetVarintResult32(int offset, int value) {
        this.offset = offset;
        this.value = value;
    }

    public int getOffset() {
        return offset;
    }

    public int getValue() {
        return value;
    }
}
