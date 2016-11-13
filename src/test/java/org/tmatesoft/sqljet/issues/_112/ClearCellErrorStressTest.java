package org.tmatesoft.sqljet.issues._112;

import java.security.SecureRandom;

import org.junit.Assert;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.IntConstants;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;

public class ClearCellErrorStressTest extends AbstractNewDbTest {

    private static final int INSERTS_COUNT = 10000;

    private static final String TABLE_DDL = "CREATE TABLE IF NOT EXISTS tiles (x int, y int, z int, s int, image blob, PRIMARY KEY (x,y,z,s))";
    private static final String INDEX_DDL = "CREATE INDEX IF NOT EXISTS IND on tiles (x,y,z,s)";

    @Test
    public void clearCellError() throws Exception {

        db.getOptions().setAutovacuum(true);
        db.runVoidWriteTransaction(db -> db.getOptions().setUserVersion(1));
        db.beginTransaction(SqlJetTransactionMode.WRITE);
        db.createTable(TABLE_DDL);
        db.createIndex(INDEX_DDL);
        db.commit();

        int conflictCount = 0;
        SecureRandom rnd = new SecureRandom();
        ISqlJetTable table = db.getTable("tiles");
        for (int i = 0; i < INSERTS_COUNT; i++) {
            byte[] blob = new byte[1024 + rnd.nextInt(4096)];
            rnd.nextBytes(blob);

            Integer x = Integer.valueOf(rnd.nextInt(2048));
            db.beginTransaction(SqlJetTransactionMode.WRITE);
            try {
                table.insert(x, IntConstants.ZERO, IntConstants.TEN, IntConstants.ZERO, blob);
            } catch (SqlJetException e) {
                if (SqlJetErrorCode.CONSTRAINT.equals(e.getErrorCode())) {
                    // insert failed because record already exists -> update
                    // it
                	conflictCount++;
                    Object[] key = new Object[] { x, IntConstants.ZERO, IntConstants.TEN, IntConstants.ZERO };
                    ISqlJetCursor updateCursor = table.lookup("IND", key);
                    do {
                        updateCursor.update(x, IntConstants.ZERO, IntConstants.TEN, IntConstants.ZERO, blob);
                    } while (updateCursor.next());
                    updateCursor.close();

                } else
                    throw e;
            }
            db.commit();
        }
        
        int cc = conflictCount;
        db.runVoidReadTransaction(db -> {
            ISqlJetCursor c = table.open();
            Assert.assertEquals(INSERTS_COUNT-cc, c.getRowCount());
        });

    }
}
