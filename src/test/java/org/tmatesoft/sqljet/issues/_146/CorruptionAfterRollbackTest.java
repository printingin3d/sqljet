/**
 * CorruptionAterRollback.java
 * Copyright (C) 2009-2011 TMate Software Ltd
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
package org.tmatesoft.sqljet.issues._146;

import static org.tmatesoft.sqljet.core.IntConstants.FOUR;
import static org.tmatesoft.sqljet.core.IntConstants.SEVEN;
import static org.tmatesoft.sqljet.core.IntConstants.SIX;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.tmatesoft.sqljet.core.AbstractInMemoryTest;
import org.tmatesoft.sqljet.core.IntConstants;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.schema.SqlJetConflictAction;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class CorruptionAfterRollbackTest extends AbstractInMemoryTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(IntConstants.DEFAULT_TIMEOUT);

    @Before
    public void setUp() throws Exception {
        db.createTable(
                "CREATE TABLE IF NOT EXISTS places (place_id INTEGER PRIMARY KEY AUTOINCREMENT,geoid INTEGER NOT NULL,class_id INTEGER NOT NULL,country_id INTEGER NOT NULL,name TEXT NOT NULL,user_defined INTEGER NOT NULL,location_lon INTEGER,location_lat INTEGER)");
    }

    @Test
    public void testTransaction() throws Exception {
        ISqlJetTable table = db.getTable("places");
        db.beginTransaction(SqlJetTransactionMode.WRITE);

        for (int i = 0; i < 2000; ++i) {
            table.insertOr(SqlJetConflictAction.REPLACE, null, Integer.valueOf(i), Integer.valueOf(i % 2),
                    Integer.valueOf(i % 3), FOUR, "hhhh", SIX, SEVEN);
        }
        db.rollback();
        db.beginTransaction(SqlJetTransactionMode.WRITE);

        for (int i = 0; i < 2000; ++i) {
            table.insertOr(SqlJetConflictAction.REPLACE, null, Integer.valueOf(i), Integer.valueOf(i % 2),
                    Integer.valueOf(i % 3), FOUR, "hhhh", SIX, SEVEN);
        }
        db.commit();
    }

}
