/**
 * AlterTableTest.java
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
package org.tmatesoft.sqljet.core.internal.schema;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.table.ISqlJetBtreeSchemaTable;
import org.tmatesoft.sqljet.core.simpleschema.SqlJetSimpleSchemaField;
import org.tmatesoft.sqljet.core.simpleschema.types.SqlJetSimpleBlobField;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SchemaModificationTest extends AbstractNewDbTest {

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.AbstractNewDbTest#setUp()
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        db.createTable("create table t(a text primary key);");
        db.createIndex("create index i on t(a);");
        db.createTable("create table t2(a int primary key);");
    }


    @Test
    public void addFieldAndCheckNoDuplicates() throws SqlJetException {
    	db.addColumn("t2", new SqlJetSimpleSchemaField("t2", SqlJetSimpleBlobField.getInstance(), false, 0));
        
    	db.read().asVoid(db -> {
            final ISqlJetBtreeSchemaTable table = ((SqlJetSchema) db.getSchema()).openSchemaTable(false);
            final Set<String> tableNames = new HashSet<>();
            for (table.first(); !table.eof(); table.next()) {
                final String type = table.getTypeField();
                if (!"table".equals(type)) {
                    continue;
                }
                final String name = table.getNameField();
                if (null == name) {
                    continue;
                }
                Assert.assertTrue(tableNames.add(name));
            }
            table.close();
            
            Assert.assertEquals(2, tableNames.size());
            Assert.assertTrue(tableNames.contains("t"));
            Assert.assertTrue(tableNames.contains("t2"));
            
        });
    }
}
