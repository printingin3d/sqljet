package org.tmatesoft.sqljet.core.internal;

public class SqlJetResultWithOffset<T> {
    private final T value;
    private final int offset;

    public SqlJetResultWithOffset(T value, int offset) {
        this.value = value;
        this.offset = offset;
    }

    public T getValue() {
        return value;
    }

    public int getOffset() {
        return offset;
    }
}
