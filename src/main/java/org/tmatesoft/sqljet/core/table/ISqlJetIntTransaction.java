package org.tmatesoft.sqljet.core.table;

import org.tmatesoft.sqljet.core.SqlJetException;

public interface ISqlJetIntTransaction<U> {
    public int run(U item) throws SqlJetException;
}
