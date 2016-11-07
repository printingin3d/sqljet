/**
 * UniqueIndexTest.java
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
package org.tmatesoft.sqljet.issues._147;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.schema.SqlJetConflictAction;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class UniqueIndexTest extends AbstractNewDbTest {

    @Override
	@Before
    public void setUp() throws Exception {
        super.setUp();
        db.createTable("CREATE TABLE IF NOT EXISTS names (name_id INTEGER PRIMARY KEY AUTOINCREMENT,unique_name TEXT NOT NULL)");
        db.createIndex("CREATE UNIQUE INDEX IF NOT EXISTS names_unique_name_idx ON names(unique_name)");
    }

    @Test
    public void testUniqueIndex() throws Exception {

        db.runVoidWriteTransaction(db -> {
                ISqlJetTable table = db.getTable("names");
                table.insertOr(SqlJetConflictAction.REPLACE, null, "same name");
                table.insertOr(SqlJetConflictAction.REPLACE, null, "same name");
            });                          // I would expect transaction fails or
                                         // one record in table. But in table
                                         // are two records!

        Integer count = db.runReadTransaction(db -> {
                ISqlJetTable table = db.getTable("names");
                ISqlJetCursor cur = table.order(null);
                int counter = 0;
                if (!cur.eof()) {
                    do {
                        ++counter;
                    } while (cur.next());

                }
                ;
                return Integer.valueOf(counter);
        });

        assertEquals(Integer.valueOf(1), count); // count is 2 but correctly is
                                                 // 1

    }

}
