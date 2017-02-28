package org.tmatesoft.sqljet.core.internal;

import java.util.Collection;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;

public class SqlJetAssert {
	public static void assertTrue(boolean value, @Nonnull SqlJetErrorCode errorCode) throws SqlJetException {
		if (!value) {
			throw new SqlJetException(errorCode);
		}
	}
	
	public static void assertTrue(boolean value, @Nonnull SqlJetErrorCode errorCode, String message) throws SqlJetException {
		if (!value) {
			throw new SqlJetException(errorCode, message);
		}
	}
	
	public static void assertFalse(boolean value, @Nonnull SqlJetErrorCode errorCode) throws SqlJetException {
		assertTrue(!value, errorCode);
	}
	
	public static void assertFalse(boolean value, @Nonnull SqlJetErrorCode errorCode, String message) throws SqlJetException {
		assertTrue(!value, errorCode, message);
	}
	
	@SuppressWarnings("null")
	public static @Nonnull <T> T assertNotNull(T value, @Nonnull SqlJetErrorCode errorCode, String message) throws SqlJetException {
		assertTrue(value!=null, errorCode, message);
		return value;
	}
	
	@SuppressWarnings("null")
	public static @Nonnull <T> T assertNotNull(T value, @Nonnull SqlJetErrorCode errorCode) throws SqlJetException {
		assertTrue(value!=null, errorCode);
		return value;
	}
	
	public static void assertNull(Object value, @Nonnull SqlJetErrorCode errorCode) throws SqlJetException {
		assertTrue(value==null, errorCode);
	}
	
	@SuppressWarnings("null")
	public static @Nonnull String assertNotEmpty(String value, @Nonnull SqlJetErrorCode errorCode) throws SqlJetException {
		assertTrue(value!=null && !value.isEmpty(), errorCode);
		return value;
	}
	
	public static void assertNotEmpty(String value, @Nonnull SqlJetErrorCode errorCode, String message) throws SqlJetException {
		assertTrue(value!=null && !value.isEmpty(), errorCode, message);
	}
	
	public static void assertNotEmpty(Collection<?> value, @Nonnull SqlJetErrorCode errorCode) throws SqlJetException {
		assertFalse(value==null || value.isEmpty(), errorCode);
	}
	
	public static void assertNotEmpty(Collection<?> value, @Nonnull SqlJetErrorCode errorCode, String message) throws SqlJetException {
		assertFalse(value==null || value.isEmpty(), errorCode, message);
	}
	
	public static void assertNoError(SqlJetErrorCode errorCode) throws SqlJetException {
		if (errorCode!=null) {
			throw new SqlJetException(errorCode);
		}
	}
	
}
