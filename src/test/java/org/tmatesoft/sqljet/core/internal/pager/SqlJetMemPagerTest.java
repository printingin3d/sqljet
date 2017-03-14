package org.tmatesoft.sqljet.core.internal.pager;

import java.util.logging.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.SqlJetAbstractLoggedTest;
import org.tmatesoft.sqljet.core.internal.ISqlJetFileSystem;
import org.tmatesoft.sqljet.core.internal.ISqlJetPage;
import org.tmatesoft.sqljet.core.internal.fs.SqlJetFileSystemsManager;

public class SqlJetMemPagerTest extends SqlJetAbstractLoggedTest {
    private ISqlJetFileSystem fileSystem;
    private SqlJetMemPager pager;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        fileSystem = SqlJetFileSystemsManager.getManager().find(null);
    	pager = new SqlJetMemPager(fileSystem);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        if (pager != null) {
            try {
                pager.close();
            } catch (Exception e) {
                logger.log(Level.INFO, "pager.close():", e);
            }
            pager = null;
        }
        fileSystem = null;
    }

    /**
     * Test method for
     * {@link org.tmatesoft.sqljet.core.internal.pager.SqlJetPager#open(org.tmatesoft.sqljet.core.internal.ISqlJetFileSystem, java.io.File, org.tmatesoft.sqljet.core.ISqlJetPageDestructor, int, java.util.Set, org.tmatesoft.sqljet.core.internal.SqlJetFileType, java.util.Set)}
     * .
     */
    @Test
    public final void testWrite() throws Exception {
        final ISqlJetPage page = pager.acquirePage(1, true);
        pager.begin(true);
        page.write();
        page.getData().fill(pager.getPageSize(), (byte) 1);
        pager.commitPhaseOne(false);
        pager.commitPhaseTwo();
        page.unref();
    }
    
    /**
     * Test method for
     * {@link org.tmatesoft.sqljet.core.internal.pager.SqlJetPager#open(org.tmatesoft.sqljet.core.internal.ISqlJetFileSystem, java.io.File, org.tmatesoft.sqljet.core.ISqlJetPageDestructor, int, java.util.Set, org.tmatesoft.sqljet.core.internal.SqlJetFileType, java.util.Set)}
     * .
     */
    @Test
    public final void testRollback() throws Exception {
    	final ISqlJetPage page = pager.acquirePage(1, true);
    	pager.begin(true);
    	page.write();
    	page.getData().fill(pager.getPageSize(), (byte) 1);
    	pager.rollback();
    	page.unref();
    }

}
