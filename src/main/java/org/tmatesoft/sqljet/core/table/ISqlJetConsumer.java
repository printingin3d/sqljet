package org.tmatesoft.sqljet.core.table;

import org.tmatesoft.sqljet.core.SqlJetException;

public interface ISqlJetConsumer<T> {
    public void run(T param) throws SqlJetException;
}
