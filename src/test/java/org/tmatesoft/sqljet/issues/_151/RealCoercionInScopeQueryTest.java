/**
 * RealCoercionInScopeQueryTest.java
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
package org.tmatesoft.sqljet.issues._151;

import static org.tmatesoft.sqljet.core.IntConstants.FIVE;
import static org.tmatesoft.sqljet.core.IntConstants.TWO;

import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;
import org.tmatesoft.sqljet.core.table.SqlJetScope;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class RealCoercionInScopeQueryTest extends AbstractNewDbTest {

    @Before
    public void setup() throws SqlJetException {
        db.createTable("CREATE TABLE halves (a REAL NOT NULL)");
        db.createIndex("CREATE INDEX halves_idx ON halves (a)");

        ISqlJetTable halves = db.getTable("halves");
        for (int i = 0; i < 10; i++) {
            halves.insert(Double.valueOf(i + .5));
        }

        db.createTable("CREATE TABLE wholes (a REAL NOT NULL)");
        db.createIndex("CREATE INDEX wholes_idx ON wholes (a)");

        ISqlJetTable wholes = db.getTable("wholes");
        for (int i = 0; i < 10; i++) {
            wholes.insert(Double.valueOf(i));
        }
    }

    @Test
    public void testScopeWithCoercion() throws SqlJetException {
        SqlJetScope halvesScope = new SqlJetScope(new Object[] {TWO}, new Object[] {FIVE});
        assertScope(halvesScope, "halves", "halves_idx", Double.valueOf(2.5), Double.valueOf(3.5), Double.valueOf(4.5));

        SqlJetScope wholesScope = new SqlJetScope(new Object[] {TWO}, new Object[] {FIVE});
        assertScope(wholesScope, "wholes", "wholes_idx", Double.valueOf(2.0), Double.valueOf(3.0), Double.valueOf(4.0), Double.valueOf(5.0));
    }

}
