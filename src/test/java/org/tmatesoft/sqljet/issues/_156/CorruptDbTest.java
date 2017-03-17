package org.tmatesoft.sqljet.issues._156;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.tmatesoft.sqljet.core.AbstractInMemoryTest;
import org.tmatesoft.sqljet.core.IntConstants;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;

public class CorruptDbTest extends AbstractInMemoryTest {

    private static final int TAILLE = 15000;

    private Random random = new Random();
    private byte[] tab = new byte[TAILLE];

    @Rule
    public Timeout globalTimeout = Timeout.seconds(IntConstants.DEFAULT_TIMEOUT);

    @Test
    public void testCorruptDB() throws SqlJetException {

        db.createTable(
                "CREATE TABLE test (" + "    pk INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL," + "    preview BLOB)");

        addRows();
        removeAllRows();
        addRows();

    }

    private void addRows() throws SqlJetException {
        final int rowCount = 100;

        db.runTransaction(db -> {
            ISqlJetTable testTable = db.getTable("test");
            for (int i = 0; i < rowCount; i++) {
                final Map<String, Object> values = new HashMap<>();
                values.put("preview", createByte());

                testTable.insertByFieldNames(values);
            }
            return null;
        }, SqlJetTransactionMode.EXCLUSIVE);
    }

    private byte[] createByte() {
        random.nextBytes(tab);
        return tab;
    }

    private void removeAllRows() throws SqlJetException {
        db.runTransaction(db -> {
            // Remove all rows
            ISqlJetTable tableDossiers = db.getTable("test");
            ISqlJetCursor curseur = tableDossiers.open();
            while (!curseur.eof()) {
                curseur.delete();
            }
            return null;
        }, SqlJetTransactionMode.EXCLUSIVE);
    }

}
