package org.tmatesoft.sqljet.core.table;

import org.tmatesoft.sqljet.core.SqlJetException;

public interface ISqlJetDoubleTransaction<U> {
    public double run(U item) throws SqlJetException;
}
