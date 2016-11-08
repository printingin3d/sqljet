package org.tmatesoft.sqljet.issues._169;

import java.io.File;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.schema.ISqlJetTableDef;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

import static org.tmatesoft.sqljet.core.IntConstants.*;

public class CompoundIndexesCriteriaTest {

	protected File file;
	protected SqlJetDb db;

	@Before
	public void setUp() throws Exception {
		file = File.createTempFile(this.getClass().getSimpleName(), null);
		file.deleteOnExit();
		db = SqlJetDb.open(file, true);
	}

	@After
	public void tearDown() throws Exception {
		if (db != null) {
			db.close();
		}
	}

	@Test
	public void testCompoundIndexesCriteriaABC() throws Exception {

		final ISqlJetTableDef t = db.createTable("create table t(a,b,c);");
		db.createIndex("create index i on t(a,b)");
		final ISqlJetTable table = db.getTable(t.getName());
		
		table.insert("a", "b", "c");
		table.insert("d", "e", "f");
		table.insert("a", "c", "b");
		table.insert("d", "f", "e");
		table.insert("a", "b", "c");
		table.insert("d", "e", "f");
		
		db.runReadTransaction(db -> {
				final ISqlJetCursor lookup = table.lookup("i", "a", "b");
				Assert.assertEquals(2, lookup.getRowCount());
				return null;
		});

		db.runReadTransaction(db -> {
				final ISqlJetCursor lookup = table.lookup("i", "d", "e");
				Assert.assertEquals(2, lookup.getRowCount());
				return null;
		});

	}

	@Test
	public void testCompoundIndexesCriteria123() throws Exception {
		final ISqlJetTableDef t = db.createTable("create table t(a,b,c);");
		db.createIndex("create index i on t(a,b)");
		final ISqlJetTable table = db.getTable(t.getName());
		table.insert(ONE, TWO, THREE);
		table.insert(FOUR, FIVE, SIX);
		table.insert(ONE, THREE, TWO);
		table.insert(FOUR, SEVEN, SEVEN);
		table.insert(ONE, TWO, THREE);
		table.insert(FOUR, FIVE, SIX);
		db.runReadTransaction(db -> {
				final ISqlJetCursor lookup = table.lookup("i", ONE, TWO);
				Assert.assertEquals(2, lookup.getRowCount());
				return null;
		});
		db.runReadTransaction(db -> {
				final ISqlJetCursor lookup = table.lookup("i", FOUR, FIVE);
				Assert.assertEquals(2, lookup.getRowCount());
				return null;
		});
	}

}
