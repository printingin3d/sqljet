package org.tmatesoft.sqljet.issues._173;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractInMemoryTest;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableDef;

public class BooleanDefaultFalseTest extends AbstractInMemoryTest {

    @Test
    public void testBooleanDefaultFalse() throws SqlJetException {
        final ISqlJetTableDef t = db
                .createTable("CREATE TABLE requires (" + "  name TEXT,  flags TEXT,  epoch TEXT,  version TEXT,"
                        + "  release TEXT,  pkgKey INTEGER , pre BOOLEAN DEFAULT FALSE)");
        assertNotNull(t);
    }

}
