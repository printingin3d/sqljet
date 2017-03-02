package org.tmatesoft.sqljet.core.simpleschema.types;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

@RunWith(Parameterized.class)
public class SqlJetSimpleFieldsTest {
	private static class TestCase {
		private final ISqlJetSimpleFieldType testSubject;
		private final SqlJetTypeAffinity affinity;
		private final boolean integer;
		private final String sql;
		private final String name;
		private final Integer size1;
		private final Integer size2;
		public TestCase(ISqlJetSimpleFieldType testSubject, SqlJetTypeAffinity affinity, boolean integer, String sql, String name, 
				Integer size1, Integer size2) {
			this.testSubject = testSubject;
			this.affinity = affinity;
			this.integer = integer;
			this.sql = sql;
			this.name = name;
			this.size1 = size1;
			this.size2 = size2;
		}
		@Override
		public String toString() {
			return testSubject.getClass().getSimpleName()+(size1==null ? "" : "("+size1+(size2==null ? "" : ","+size2)+")");
		}
	}
	
    @Parameters(name = "{index}: {0}")
    public static Collection<? extends Object> data() {
    	return Arrays.asList(
    			new TestCase(SqlJetSimpleIntField.getInstance(), SqlJetTypeAffinity.INTEGER, true, "integer", "int", null, null),
    			new TestCase(SqlJetSimpleDoubleField.getInstance(), SqlJetTypeAffinity.REAL, false, "double", "double", null, null),
    			new TestCase(new SqlJetSimpleVarCharField(15), SqlJetTypeAffinity.TEXT, false, "varchar(15)", "varchar", Integer.valueOf(15), null),
    			new TestCase(SqlJetSimpleTextField.getInstance(), SqlJetTypeAffinity.TEXT, false, "text", "text", null, null),
    			new TestCase(new SqlJetSimpleDecimalField(15, 2), SqlJetTypeAffinity.NUMERIC, false, "decimal(15,2)", "decimal", Integer.valueOf(15), Integer.valueOf(2))
    		);
    }
    
    private final TestCase testCase;
    
    public SqlJetSimpleFieldsTest(TestCase testCase) {
		this.testCase = testCase;
	}

	@Test
    public void toInnerRepresentationShouldReturnTheObjectItself() {
    	Assert.assertSame(testCase.testSubject, testCase.testSubject.toInnerRepresentation());
    }
	
	@Test
	public void testAffinity() {
		Assert.assertEquals(testCase.affinity, testCase.testSubject.getTypeAffinity());
		if (testCase.integer) {
			Assert.assertTrue(testCase.testSubject.isInteger());
		} else { 
			Assert.assertFalse(testCase.testSubject.isInteger());
		}
	}
	
	@Test
	public void testSql() {
		Assert.assertEquals(testCase.sql, testCase.testSubject.toSql());
	}
	
	@Test
	public void testNames() {
		Assert.assertEquals(Collections.singletonList(testCase.name), testCase.testSubject.toInnerRepresentation().getNames());
	}
	
	@Test
	public void testSizes() {
		Assert.assertEquals(testCase.size1, testCase.testSubject.toInnerRepresentation().getSize1());
		Assert.assertEquals(testCase.size2, testCase.testSubject.toInnerRepresentation().getSize2());
	}
}
