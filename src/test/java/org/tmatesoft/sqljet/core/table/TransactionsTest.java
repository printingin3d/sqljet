package org.tmatesoft.sqljet.core.table;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;

public class TransactionsTest extends AbstractNewDbTest {

	@Test
	public void testWriteInRead() throws SqlJetException {
		assertEquals(Boolean.TRUE,
				db.runReadTransaction(db2 -> db2.runWriteTransaction(db3 -> {
					doWrite(db3);
					return Boolean.TRUE;
				})));
	}

	@Test
	public void testReadInRead() throws SqlJetException {
		assertEquals(Boolean.TRUE,
				db.runReadTransaction(db2 -> db2.runReadTransaction(db3 -> Boolean.TRUE)));
	}

	@Test
	public void testReadInWrite() throws SqlJetException {
		assertEquals(Boolean.TRUE,
				db.runWriteTransaction(db2 -> db2.runReadTransaction(db3 -> {
								doWrite(db3);
								return Boolean.TRUE;
							})));
	}

	@Test
	public void testWriteInWrite() throws SqlJetException {
		assertEquals(Boolean.TRUE,
				db.runWriteTransaction(db2 -> db2.runWriteTransaction(db3 -> {
								doWrite(db3);
								return Boolean.TRUE;
				})));
	}

	private void doWrite(SqlJetDb db) throws SqlJetException {
		ISqlJetTable t = db
				.getTable(db
						.createTable(
								"create table t(a integer primary key, b text);")
						.getName());
		t.insert("test");
	}

}
