package org.tmatesoft.sqljet.core.internal.pager;

import static org.tmatesoft.sqljet.core.internal.SqlJetAbstractFileSystemMockTest.PERM_TEMPORARY;

import java.io.File;
import java.util.logging.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.SqlJetAbstractLoggedTest;
import org.tmatesoft.sqljet.core.internal.ISqlJetFileSystem;
import org.tmatesoft.sqljet.core.internal.ISqlJetLimits;
import org.tmatesoft.sqljet.core.internal.ISqlJetPage;
import org.tmatesoft.sqljet.core.internal.SqlJetFileType;
import org.tmatesoft.sqljet.core.internal.fs.SqlJetFileSystemsManager;
import org.tmatesoft.sqljet.core.internal.fs.util.SqlJetFileUtil;

public class SqlJetTempPagerTest extends SqlJetAbstractLoggedTest {

    private ISqlJetFileSystem fileSystem;
    private SqlJetTempPager pager;
    private File file;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        fileSystem = SqlJetFileSystemsManager.getManager().find(null);
        file = fileSystem.getTempFile();
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
        if (null != file) {
            SqlJetFileUtil.deleteFile(file);
        }
    }

    @Test
    public final void testWriteTemp() throws Exception {
    	pager = new SqlJetTempPager(fileSystem, SqlJetFileType.TEMP_DB, PERM_TEMPORARY);
        final ISqlJetPage page = pager.acquirePage(1, true);
        pager.begin(true);
        page.write();
        page.getData().fill(ISqlJetLimits.SQLJET_DEFAULT_PAGE_SIZE, (byte) 1);
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
    public final void testOpenTemp() throws Exception {
    	pager = new SqlJetTempPager(fileSystem, SqlJetFileType.TEMP_DB, PERM_TEMPORARY);
    }

}
