package org.tmatesoft.sqljet.core.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;

public class TransactionsTest extends AbstractNewDbTest {

	@Test
	public void testWriteInRead() throws SqlJetException {
		assertTrue(
				db.read().asBool(db2 -> db2.write().asBool(db3 -> {
					doWrite(db3);
					return true;
				})));
	}

	@Test
	public void testReadInRead() throws SqlJetException {
		assertEquals(Boolean.TRUE,
				db.read().as(db2 -> db2.read().as(db3 -> Boolean.TRUE)));
	}

	@Test
	public void testReadInWrite() throws SqlJetException {
		assertEquals(Boolean.TRUE,
				db.write().as(db2 -> db2.read().as(db3 -> {
								doWrite(db3);
								return Boolean.TRUE;
							})));
	}

	@Test
	public void testWriteInWrite() throws SqlJetException {
		assertEquals(Boolean.TRUE,
				db.write().as(db2 -> db2.write().as(db3 -> {
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
