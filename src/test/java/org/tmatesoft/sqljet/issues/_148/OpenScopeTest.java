/**
 * OpenScopeTest.java
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
package org.tmatesoft.sqljet.issues._148;

import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.table.SqlJetScope;
import org.tmatesoft.sqljet.core.table.SqlJetScope.SqlJetScopeBound;

import static org.tmatesoft.sqljet.core.IntConstants.*;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class OpenScopeTest extends AbstractNewDbTest {

    @Override
	@Before
    public void setUp() throws Exception {
        super.setUp();
        db.createTable("CREATE TABLE IF NOT EXISTS table (name TEXT, count INTEGER)");
        db.createIndex("CREATE UNIQUE INDEX IF NOT EXISTS names_idx ON table(name)");
        
        db.runVoidWriteTransaction(db -> {
                db.getTable("table").insert("XYZ", ONE);
                db.getTable("table").insert("XYZZ", ONE);
                db.getTable("table").insert("ABC", ONE);
                db.getTable("table").insert("ABCD", ONE);
                db.getTable("table").insert("ABCDEF", ONE);
                db.getTable("table").insert("A", ONE);
        });

        db.createTable("CREATE TABLE IF NOT EXISTS noindex (id INTEGER PRIMARY KEY)");
        db.runVoidWriteTransaction(db -> {
                db.getTable("noindex").insert(ONE);
                db.getTable("noindex").insert(TWO);
                db.getTable("noindex").insert(THREE);
                db.getTable("noindex").insert(FOUR);
                db.getTable("noindex").insert(FIVE);
        });
    }

    @Test
    public void testOpenScope() throws SqlJetException {
        
        SqlJetScope closedScope = new SqlJetScope(new Object[] {"ABC"}, new Object[] {"XYZ"});
        SqlJetScope openScope = new SqlJetScope(new Object[] {"ABC"}, false, new Object[] {"XYZ"}, false);
        SqlJetScope emptyOpenScope = new SqlJetScope(new Object[] {"ABCD"}, false, new Object[] {"ABCDEF"}, false);
        SqlJetScope notMatchingOpenScope = new SqlJetScope(new Object[] {"AB"}, false, new Object[] {"XY"}, false);
        SqlJetScope notMatchingClosedScope = new SqlJetScope(new Object[] {"AB"}, true, new Object[] {"XY"}, true);
        SqlJetScope outOfBoundsClosedScope = new SqlJetScope(new Object[] {"XYZZZ"}, true, new Object[] {"XYZZZZZ"}, true);
        SqlJetScope outOfBoundsOpenScope = new SqlJetScope(new Object[] {"XYZZ"}, false, new Object[] {"XYZZZZZ"}, true);
        SqlJetScope unbounded = new SqlJetScope((SqlJetScopeBound) null, (SqlJetScopeBound) null);
        SqlJetScope unbounded2 = new SqlJetScope((Object[]) null, (Object[]) null);
        
        assertIndexScope(closedScope, "ABC", "ABCD", "ABCDEF", "XYZ");
        assertIndexScope(closedScope.reverse(), "XYZ", "ABCDEF", "ABCD", "ABC");
        assertIndexScope(openScope, "ABCD", "ABCDEF");
        assertIndexScope(openScope.reverse(), "ABCDEF", "ABCD");
        assertIndexScope(emptyOpenScope);
        assertIndexScope(emptyOpenScope.reverse());
        assertIndexScope(notMatchingClosedScope, "ABC", "ABCD", "ABCDEF");
        assertIndexScope(notMatchingOpenScope, "ABC", "ABCD", "ABCDEF");
        assertIndexScope(outOfBoundsClosedScope);
        assertIndexScope(outOfBoundsOpenScope);

        assertIndexScope(unbounded, "A", "ABC", "ABCD", "ABCDEF", "XYZ", "XYZZ" );
        assertIndexScope(unbounded.reverse(), "A", "ABC", "ABCD", "ABCDEF", "XYZ", "XYZZ" );
        assertIndexScope(unbounded2, "A", "ABC", "ABCD", "ABCDEF", "XYZ", "XYZZ" );
        assertIndexScope(unbounded2.reverse(), "A", "ABC", "ABCD", "ABCDEF", "XYZ", "XYZZ" );
    }

    @Test
    public void testNoIndexScope() throws SqlJetException {
        SqlJetScope closedScope = new SqlJetScope(new Object[] {TWO}, new Object[] {FIVE});
        SqlJetScope openScope = new SqlJetScope(new Object[] {TWO}, false, new Object[] {FIVE}, false);
        SqlJetScope emptyScope = new SqlJetScope(new Object[] {THREE}, false, new Object[] {FOUR}, false);
        
        assertNoIndexScope(closedScope, new Long(2), new Long(3), new Long(4), new Long(5));
        assertNoIndexScope(openScope, new Long(3), new Long(4));
        assertNoIndexScope(emptyScope);

        assertNoIndexScope(closedScope.reverse(), Long.valueOf(5),Long.valueOf(4),Long.valueOf(3), Long.valueOf(2));
        assertNoIndexScope(openScope.reverse(), new Long(4), new Long(3));
        assertNoIndexScope(emptyScope.reverse());
        
        db.runVoidWriteTransaction(db -> db.getTable("noindex").lookup(null, Long.valueOf(3)).delete());

        assertNoIndexScope(openScope, new Long(4));
        assertNoIndexScope(openScope.reverse(), new Long(4));

        db.runVoidWriteTransaction(db -> {
                db.getTable("noindex").insert(THREE);
                db.getTable("noindex").insert(SIX);
        });
        assertNoIndexScope(openScope, new Long(3), new Long(4));
        assertNoIndexScope(openScope.reverse(), new Long(4), new Long(3));
        assertNoIndexScope(new SqlJetScope((SqlJetScopeBound) null, (SqlJetScopeBound)null), 
                 new Long(1), new Long(2), new Long(3), new Long(4), new Long(5), new Long(6));
    }
    
    private void assertIndexScope(SqlJetScope scope, Object... expectedKeysInScope) throws SqlJetException {
        assertScope(scope, "table", "names_idx", expectedKeysInScope);
    }

    private void assertNoIndexScope(SqlJetScope scope, Object... expectedKeysInScope) throws SqlJetException {
        assertScope(scope, "noindex", null, expectedKeysInScope);
    }

}
