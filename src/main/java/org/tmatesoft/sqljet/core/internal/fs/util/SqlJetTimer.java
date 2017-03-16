package org.tmatesoft.sqljet.core.internal.fs.util;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.internal.SqlJetUtility;

public class SqlJetTimer {
    /**
     * Activates logging of files operations performance.
     */
    private static final @Nonnull String SQLJET_LOG_FILES_PERFORMANCE_PROP = "SQLJET_LOG_FILES_PERFORMANCE";

    private static final boolean SQLJET_LOG_FILES_PERFORMANCE = SqlJetUtility
            .getBoolSysProp(SQLJET_LOG_FILES_PERFORMANCE_PROP, false);

    private long start = 0;
    private long elapsed = 0;

    public SqlJetTimer() {
        start();
    }

    /**
     * @return
     */
    public String format() {
        return elapsed + "ns";
    }

    /**
     *
     */
    public void end() {
        if (SQLJET_LOG_FILES_PERFORMANCE) {
            elapsed = System.nanoTime() - start;
        }
    }

    /**
     *
     */
    public void start() {
        if (SQLJET_LOG_FILES_PERFORMANCE) {
            start = System.nanoTime();
        }
    }
}
