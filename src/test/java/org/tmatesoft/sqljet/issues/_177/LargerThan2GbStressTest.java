package org.tmatesoft.sqljet.issues._177;

import org.junit.Assert;
import org.junit.Test;
import org.tmatesoft.sqljet.core.AbstractNewDbTest;
import org.tmatesoft.sqljet.core.SqlJetException;

public class LargerThan2GbStressTest extends AbstractNewDbTest {
	
	@Test
	public void testIntOverflow() {
		final long i1 = 2*Integer.MAX_VALUE;
		final long i2 = 2*(long)Integer.MAX_VALUE;
		Assert.assertTrue( i1 != i2 );
	}
    
    @Test
    public void testLargeDb() throws SqlJetException {
    	System.out.println(file.getAbsolutePath());
    	
        db.runVoidWriteTransaction(db -> db.createTable("CREATE TABLE test (x BLOB)"));
        
        byte[] megabyte = new byte[1048576];
        for(int i = 0; i < 4096; i++) {
            db.getTable("test").insert(megabyte);
            if (i>0 && i%64==0) {
				System.out.print('.');
			}
        }
    }
}
