package org.tmatesoft.sqljet.core.table;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;

public class BlobsTest extends AbstractNewDbTest {

    private static final String T_DDL = "CREATE TABLE t (x int, image blob, PRIMARY KEY (x))";
    private static final String T_DDL_2 = "CREATE TABLE t (x int, image blob, t text, PRIMARY KEY (x))";
    
    @Rule
    public Timeout globalTimeout = new Timeout(15000); // 15 seconds max per method tested

    @Test
    public void readBlob() throws Exception {
        db.getOptions().setAutovacuum(true);
        db.createTable(T_DDL);

        final SecureRandom rnd = new SecureRandom();
        final ISqlJetTable t = db.getTable("t");
        final byte[] blob = new byte[1024 + 4096];

        rnd.nextBytes(blob);

        db.runVoidWriteTransaction(db -> t.insert(Integer.valueOf(rnd.nextInt(2048)), blob));

        db.runReadTransaction(db -> {
                final ISqlJetCursor c = t.open();
                try {
                    if (!c.eof()) {
                        do {
                            final byte[] b = c.getBlobAsArray(1).orElse(null);
                            Assert.assertArrayEquals(blob, b);
                        } while (c.next());
                    }
                } finally {
                    c.close();
                }
                return null;
        });

    }
    
    @Test
    public void updateButBlob() throws Exception {

        db.getOptions().setAutovacuum(true);
        db.createTable(T_DDL_2);

        final ISqlJetTable t = db.getTable("t");
        final byte[] blob = "text".getBytes();
        final String text = "text";

		db.runWriteTransaction(db -> {
			for (int i = 0; i < 2047; i++) {
				t.insert(Integer.valueOf(i), blob, text);
			}
			return null;
		});

		db.runVoidWriteTransaction(db -> {
			final ISqlJetCursor c = t.open();
			Map<String, Object> values = new HashMap<String, Object>();
			if (!c.eof()) {
				do {
					values.put("x", Long.valueOf(c.getInteger("x") + 2048));
					c.updateByFieldNames(values);
				} while (c.next());
			}
		});

		db.runReadTransaction(db -> {
			final ISqlJetCursor c = t.open();
			if (!c.eof()) {
				do {
					byte[] b = c.getBlobAsArray("image").orElse(null);
					long xValue = c.getInteger("x");
					String tValue = c.getString("t");
					Assert.assertArrayEquals(b, blob);
					Assert.assertTrue(xValue >= 1024);
					Assert.assertEquals(text, tValue);
				} while (c.next());
			}
			return null;
		});
    }

    @Test
    public void updateBlob() throws Exception {
		db.getOptions().setAutovacuum(true);
		db.createTable(T_DDL);

		final SecureRandom rnd = new SecureRandom();
		final ISqlJetTable t = db.getTable("t");
		final byte[] blob = new byte[1024 + 4096];

		rnd.nextBytes(blob);

		db.runVoidWriteTransaction(db -> {
			try {
				t.insert(Integer.valueOf(rnd.nextInt(2048)), blob);
			} catch (SqlJetException e) {
				if (!SqlJetErrorCode.CONSTRAINT.equals(e.getErrorCode())) {
					throw e;
				}
			}
		});

		db.runVoidWriteTransaction(db -> {
			final ISqlJetCursor c = t.open();
			if (!c.eof()) {
				do {
					rnd.nextBytes(blob);

					try {
						c.update(Integer.valueOf(rnd.nextInt(2048)), blob);
					} catch (SqlJetException e) {
						if (!SqlJetErrorCode.CONSTRAINT.equals(e.getErrorCode())) {
							throw e;
						}
					}

				} while (c.next());
			}
		});
    }

}
