package org.tmatesoft.sqljet.issues._110;

import java.io.File;

import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.internal.fs.util.SqlJetFileUtil;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

public class MalformedDB {
	public static void main(String[] args) {
		try {
			File dbFile = new File("./src/test/data/db/test.db");
			SqlJetFileUtil.deleteFile(dbFile);
			SqlJetDb db = SqlJetDb.open(dbFile, true);
			db.getOptions().setAutovacuum(true);
			db.runVoidWriteTransaction(db2 -> db2.getOptions().setUserVersion(1));
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			String sql1 = "CREATE TABLE IF NOT EXISTS TESTXX (a int, b int, c int, "
					+ "d int, blob blob, PRIMARY KEY (a,b,c,d))";
			String sql2 = "CREATE INDEX IF NOT EXISTS IND on TESTXX (a,b,c,d)";
			db.createTable(sql1);
			db.createIndex(sql2);
			db.commit();
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
