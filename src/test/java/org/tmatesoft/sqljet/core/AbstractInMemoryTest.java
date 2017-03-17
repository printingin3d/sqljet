package org.tmatesoft.sqljet.core;

import org.junit.After;
import org.junit.Before;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

public abstract class AbstractInMemoryTest {
    protected SqlJetDb db;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUpDb() throws Exception {
        db = SqlJetDb.open(SqlJetDb.IN_MEMORY, true);
    }
    
    @After
    public void tearDownDb() throws Exception {
        db.close();
    }

}
