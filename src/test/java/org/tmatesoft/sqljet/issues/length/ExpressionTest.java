/**
 * LengthTest.java
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

import org.junit.Assert;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractInMemoryTest;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class ExpressionTest extends AbstractInMemoryTest {

    @Test
    public void testLength() throws SqlJetException {
        db.write().asVoid(db -> {
                final String sql = "CREATE TABLE contacts ( " + "id INTEGER PRIMARY KEY,"
                        + "name TEXT NOT NULL COLLATE NOCASE," + "phone TEXT NOT NULL DEFAULT 'UNKNOWN',"
                        + "UNIQUE (name,phone),CHECK(LENGTH(phone)>=7) );";
                db.createTable(sql);
        });
        final ISqlJetTable table = db.getTable("contacts");
        final String sql = table.getDefinition().toSQL();

        Assert.assertTrue(sql.matches("(?i).*LENGTH\\s*\\(\\s*phone\\s*\\).*"));
        Assert.assertFalse(sql.matches("(?i).*LENGTH\\s*\\(\\s*\\).*"));
    }
    
    @Test
    public void testMatch() throws SqlJetException {
    	db.write().asVoid(db -> {
    		final String sql = "CREATE TABLE contacts ( " + "id INTEGER PRIMARY KEY,"
    				+ "name TEXT NOT NULL COLLATE NOCASE," + "phone TEXT NOT NULL DEFAULT 'UNKNOWN',"
    				+ "UNIQUE (name,phone),CHECK(phone regexp '(0-7){6-12}') );";
    		db.createTable(sql);
    	});
        final ISqlJetTable table = db.getTable("contacts");
        final String sql = table.getDefinition().toSQL();

        Assert.assertTrue(sql.matches("(?i).*phone\\s+regexp\\s+.*"));
    }

}
