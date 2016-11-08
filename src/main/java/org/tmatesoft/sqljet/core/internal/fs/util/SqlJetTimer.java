package org.tmatesoft.sqljet.core.internal.fs.util;

import org.tmatesoft.sqljet.core.SqlJetLogDefinitions;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;

public class SqlJetTimer {
    private static final boolean SQLJET_LOG_FILES_PERFORMANCE = SqlJetUtility.getBoolSysProp(
            SqlJetLogDefinitions.SQLJET_LOG_FILES_PERFORMANCE, false);

    private long start = 0;
    private long elapsed = 0;

    public SqlJetTimer() {
    	start();
    }
    
    /**
     * @return
     */
    public String format() {
        return elapsed+"ns";
    }

    /**
     *
     */
    public void end() {
        if (SQLJET_LOG_FILES_PERFORMANCE)
            elapsed = System.nanoTime() - start;
    }

    /**
     *
     */
    public void start() {
        if (SQLJET_LOG_FILES_PERFORMANCE)
            start = System.nanoTime();
    }
}
