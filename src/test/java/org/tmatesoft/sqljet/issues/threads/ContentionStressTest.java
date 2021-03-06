/**
 * ContentionTest.java
 * Copyright (C) 2009-2013 TMate Software Ltd
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * For information on how to redistribute this software under
 * the terms of a license other than GNU General Public License
 * contact TMate Software at support@sqljet.com
 */
package org.tmatesoft.sqljet.issues.threads;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.ParametersAreNonnullByDefault;

import org.junit.Assert;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.table.ISqlJetBusyHandler;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;
import org.tmatesoft.sqljet.core.table.ISqlJetTransaction;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class ContentionStressTest extends AbstractNewDbTest {

    private static final AtomicBoolean exit = new AtomicBoolean(false);

    private static final String TABLE = "record";

    private static int BUSY_WAIT = 600;
    private static int BUSY_SLEEP = 50;

    private static long BETWEEN = 50;
    private static long TOTAL = 100;

    private static final Logger logger = Logger.getLogger(ContentionStressTest.class.getName());

    private static class BusyHandler implements ISqlJetBusyHandler {

        private final String name;
        private final int retries;
        private final int sleep;
        private boolean busy = false;

        public BusyHandler(String name, int retries, int sleep) {
            this.name = name;
            this.retries = retries;
            this.sleep = sleep;
        }

        public boolean isBusy() {
            return busy;
        }

        @Override
        public boolean call(int number) {
            logger.log(Level.INFO, name + " retry " + number);
            if (number > retries) {
                busy = true;
                exit.set(true);
                return false;
            } else {
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    busy = true;
                    exit.set(true);
                    return false;
                }
                return true;
            }
        }
    }

    @ParametersAreNonnullByDefault
    private static class Action implements Runnable {

        private final String name;
        private final SqlJetDb db;
        private final SqlJetTransactionMode mode;
        private final ISqlJetTransaction<Object, SqlJetDb> transaction;
        private final BusyHandler busyHandler;

        public Action(String name, SqlJetDb db, SqlJetTransactionMode mode,
                ISqlJetTransaction<Object, SqlJetDb> transaction) {
            this.name = name;
            this.db = db;
            this.mode = mode;
            this.transaction = transaction;
            this.busyHandler = new BusyHandler(this.name, BUSY_WAIT / BUSY_SLEEP, BUSY_SLEEP);
            this.db.setBusyHandler(busyHandler);
        }

        public boolean isBusy() {
            return this.busyHandler.isBusy();
        }

        @Override
        public void run() {
            try {
                long count = 0;
                while (count++ < TOTAL && !exit.get()) {
                    logger.log(Level.INFO, name + " ran for " + count);
                    try {
                        db.runTransaction(transaction, mode);
                        logger.log(Level.INFO, "done " + name);
                    } catch (SqlJetException e) {
                        logger.log(Level.WARNING, name + " error", e);
                    }
                    try {
                        Thread.sleep(BETWEEN);
                    } catch (InterruptedException e) {
                    }
                }
            } finally {
                try {
                    db.close();
                } catch (SqlJetException e) {
                    logger.log(Level.WARNING, "db.close()", e);
                }
            }
        }
    }

    private static class Writer implements ISqlJetTransaction<Object, SqlJetDb> {
        private long batch = 0;
        private long id = 0;

        @Override
        public Object run(SqlJetDb db) throws SqlJetException {
            final ISqlJetTable table = db.getTable(TABLE);
            int written = 0;
            batch++;
            while (written < TOTAL && !exit.get()) {
                table.insert(Long.valueOf(batch), Long.valueOf(++id));
                written++;
            }
            logger.log(Level.INFO, "writer: inserted " + written);
            return null;
        }
    }

    private static class Reader implements ISqlJetTransaction<Object, SqlJetDb> {
        @Override
        public Object run(SqlJetDb db) throws SqlJetException {
            final ISqlJetTable table = db.getTable(TABLE);
            int read = 0;
            final ISqlJetCursor cursor = table.open();
            try {
                boolean more = !cursor.eof();
                while (read < TOTAL && more && !exit.get()) {
                    read++;
                    cursor.getInteger(0);
                    more = cursor.next();
                }
            } finally {
                cursor.close();
            }
            logger.log(Level.INFO, "reader: scanned " + read);
            return null;
        }
    }

    @Test
    public void testContention() throws Exception {
        logger.setLevel(Level.WARNING);

        db.createTable("CREATE TABLE record (a INTEGER NOT NULL, b INTEGER NOT NULL PRIMARY KEY)");

        Action writerAction = new Action("writer", SqlJetDb.open(file, true), SqlJetTransactionMode.WRITE,
                new Writer());
        Action readerAction = new Action("reader", SqlJetDb.open(file, false), SqlJetTransactionMode.READ_ONLY,
                new Reader());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(writerAction);
        // give writer time to write some records
        Thread.sleep(1000);
        executor.submit(readerAction);

        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);

        Assert.assertFalse(writerAction.isBusy());
        Assert.assertFalse(readerAction.isBusy());
    }

}
