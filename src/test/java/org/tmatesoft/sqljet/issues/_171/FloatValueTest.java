package org.tmatesoft.sqljet.issues._171;

import static org.junit.Assert.assertNotEquals;

import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractInMemoryTest;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableDef;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;

public class FloatValueTest extends AbstractInMemoryTest {

    @Test
    public void testFloatValue() throws SqlJetException {
        final ISqlJetTableDef d = db.createTable("create table t(a float);");
        final ISqlJetTable t = db.getTable(d.getName());
        t.insert("1.1");
        t.insert(".1");
        t.insert("1.");
        t.insert("1");
        t.insert(Double.valueOf(1.1));
        t.insert(Double.valueOf(.1));
        t.insert(Double.valueOf(1.));
        t.insert(Double.valueOf(1));
        db.read().asVoid(db -> {
            final ISqlJetCursor c = t.open();
            try {
                for (c.first(); !c.eof(); c.next()) {
                    final double f = c.getFloat(0);
                    assertNotEquals(0, f, 1E-6);
                }
            } finally {
                c.close();
            }
        });
    }

}
