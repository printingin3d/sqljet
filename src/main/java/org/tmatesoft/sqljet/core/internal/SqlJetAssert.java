package org.tmatesoft.sqljet.core.internal;

import java.util.Collection;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;

public class SqlJetAssert {
	public static void assertTrue(boolean value, SqlJetErrorCode errorCode) throws SqlJetException {
		if (!value) {
			throw new SqlJetException(errorCode);
		}
	}
	
	public static void assertTrue(boolean value, SqlJetErrorCode errorCode, String message) throws SqlJetException {
		if (!value) {
			throw new SqlJetException(errorCode, message);
		}
	}
	
	public static void assertFalse(boolean value, SqlJetErrorCode errorCode) throws SqlJetException {
		assertTrue(!value, errorCode);
	}
	
	public static void assertFalse(boolean value, SqlJetErrorCode errorCode, String message) throws SqlJetException {
		assertTrue(!value, errorCode, message);
	}
	
	public static void assertNotNull(Object value, SqlJetErrorCode errorCode, String message) throws SqlJetException {
		assertTrue(value!=null, errorCode, message);
	}
	
	public static void assertNotNull(Object value, SqlJetErrorCode errorCode) throws SqlJetException {
		assertTrue(value!=null, errorCode);
	}
	
	public static void assertNull(Object value, SqlJetErrorCode errorCode) throws SqlJetException {
		assertTrue(value==null, errorCode);
	}
	
	public static void assertNotEmpty(String value, SqlJetErrorCode errorCode) throws SqlJetException {
		assertTrue(value!=null && !value.isEmpty(), errorCode);
	}
	
	public static void assertNotEmpty(String value, SqlJetErrorCode errorCode, String message) throws SqlJetException {
		assertTrue(value!=null && !value.isEmpty(), errorCode, message);
	}
	
	public static void assertNotEmpty(Collection<?> value, SqlJetErrorCode errorCode, String message) throws SqlJetException {
		assertFalse(value==null || value.isEmpty(), errorCode, message);
	}
	
}
