package org.tmatesoft.sqljet.core.internal.vdbe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.internal.table.ISqlJetBtreeRecord;

public class SqlJetBtreeRecordTest {
	private static final double EPSILON = 1e-6;
	
	@Test
	public void testNull() throws SqlJetException {
		ISqlJetBtreeRecord record = SqlJetBtreeRecord.getRecord(SqlJetEncoding.UTF8, (Object)null);
		assertEquals(SqlJetValueType.NULL, record.getRawField(0).getType());
		assertTrue(record.getRawField(0).isNull());
	}
	
	@Test
	public void testString() throws SqlJetException {
		ISqlJetBtreeRecord record = SqlJetBtreeRecord.getRecord(SqlJetEncoding.UTF8, "Dummy string");
		assertEquals(SqlJetValueType.TEXT, record.getRawField(0).getType());
		assertEquals("Dummy string", record.getStringField(0));
	}
	
	@Test
	public void testBoolean() throws SqlJetException {
		ISqlJetBtreeRecord record = SqlJetBtreeRecord.getRecord(SqlJetEncoding.UTF8, Boolean.TRUE, Boolean.FALSE);
		assertEquals(SqlJetValueType.INTEGER, record.getRawField(0).getType());
		assertEquals(1, record.getIntField(0));
		assertEquals(0, record.getIntField(1));
	}
	
	@Test
	public void testFloat() throws SqlJetException {
		ISqlJetBtreeRecord record = SqlJetBtreeRecord.getRecord(SqlJetEncoding.UTF8, Float.valueOf(0.4f));
		assertEquals(SqlJetValueType.FLOAT, record.getRawField(0).getType());
		assertEquals(0.4d, record.getRawField(0).realValue(), EPSILON);
	}
	
	@Test
	public void testDouble() throws SqlJetException {
		ISqlJetBtreeRecord record = SqlJetBtreeRecord.getRecord(SqlJetEncoding.UTF8, Double.valueOf(1.4d));
		assertEquals(SqlJetValueType.FLOAT, record.getRawField(0).getType());
		assertEquals(1.4d, record.getRawField(0).realValue(), EPSILON);
	}
	
	@Test
	public void testInt() throws SqlJetException {
		ISqlJetBtreeRecord record = SqlJetBtreeRecord.getRecord(SqlJetEncoding.UTF8, Integer.valueOf(23));
		assertEquals(SqlJetValueType.INTEGER, record.getRawField(0).getType());
		assertEquals(23l, record.getRawField(0).intValue());
	}
	
	@Test
	public void testLong() throws SqlJetException {
		ISqlJetBtreeRecord record = SqlJetBtreeRecord.getRecord(SqlJetEncoding.UTF8, Long.valueOf(3123123));
		assertEquals(SqlJetValueType.INTEGER, record.getRawField(0).getType());
		assertEquals(3123123l, record.getRawField(0).intValue());
	}
	
	@Test(expected = SqlJetException.class)
	public void testIllegalValue() throws SqlJetException {
		SqlJetBtreeRecord.getRecord(SqlJetEncoding.UTF8, new Object());
	}
}
