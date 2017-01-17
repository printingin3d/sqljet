package org.tmatesoft.sqljet.core.table;

import org.tmatesoft.sqljet.core.SqlJetException;

public interface ISqlJetLongTransaction<U> {
    public long run(U item) throws SqlJetException;
}
