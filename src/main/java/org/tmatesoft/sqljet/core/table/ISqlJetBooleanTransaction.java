package org.tmatesoft.sqljet.core.table;

import org.tmatesoft.sqljet.core.SqlJetException;

/**
 * Interface for actions (closures) which will be performed atomically within
 * transaction.
 * 
 * @author Ivan Suller
 * @author PrintingIn3D
 * 
 */
public interface ISqlJetBooleanTransaction<U> {
    public boolean run(U item) throws SqlJetException;
}
