package org.tmatesoft.sqljet.core.internal;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;

public class SqlJetAssert {
	public static void assertTrue(boolean value, SqlJetErrorCode errorCode) throws SqlJetException {
		if (!value) {
			throw new SqlJetException(errorCode);
		}
	}
	
	public static void assertFalse(boolean value, SqlJetErrorCode errorCode) throws SqlJetException {
		assertTrue(!value, errorCode);
	}
	
}
