/**
 * RepCacheFail.java
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
package org.tmatesoft.sqljet.issues.length;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractDataCopyTest;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class RepCacheFailStressTest extends AbstractDataCopyTest {

    private static final String DB_ARCHIVE = "src/test/data/db/rep-cache/fail/rep-cache.zip";
    private static final String DB_FILE_NAME = "rep-cache.db";
    private static final String TABLE = "rep_cache";

    @Test
    public void repCacheFail() throws Exception {
        final File dbFile1 = File.createTempFile("repCacheFail", null);
        dbFile1.deleteOnExit();
        final File dbFile2 = File.createTempFile("repCacheFail", null);
        dbFile2.deleteOnExit();
        deflate(new File(DB_ARCHIVE), DB_FILE_NAME, dbFile1);

        final SqlJetDb db1 = SqlJetDb.open(dbFile1, false);
        final SqlJetDb db2 = SqlJetDb.open(dbFile2, true);
        
        db2.write().asVoid(db -> {
                db.createTable("create table "+TABLE+" (hash text not null primary key, "
                        + "                        revision integer not null, "
                        + "                        offset integer not null, "
                        + "                        size integer not null, "
                        + "                        expanded_size integer not null); ");
        });
        
        db1.read().asVoid(db -> {
                final Collection<Object[]> block = new ArrayList<>();
                ISqlJetCursor c = db.getTable(TABLE).open();
                long currentRev = 0;
                while (!c.eof()) {
                    long rev = c.getInteger(1);
                    if (rev != currentRev && block.size()>100) {
                        db2.write().asVoid(db3 -> {
                        		ISqlJetTable table = db3.getTable(TABLE);
                                for (Object[] row : block) {
									table.insert(row);
                                }
                        });
                        
                        currentRev = rev;
                        block.clear();
                    }
                    Object[] values = c.getRowValues();
                    block.add(values);
                    c.next();
                }
                if (!block.isEmpty()) {
                    db2.write().asVoid(db3 -> {
                			ISqlJetTable table = db3.getTable(TABLE);
	                        for (Object[] row : block) {
	                        	table.insert(row);
	                        }
                    });
                }
        });
    }
}
