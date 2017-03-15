/**
 * SqlJetPagerTest.java
 * Copyright (C) 2008 TMate Software Ltd
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * For information on how to redistribute this software under
 * the terms of a license other than GNU General Public License
 * contact TMate Software at support@sqljet.com
 */
package org.tmatesoft.sqljet.core.internal.pager;

import static org.tmatesoft.sqljet.core.internal.SqlJetAbstractFileSystemMockTest.PERM_CREATE;

import java.io.File;
import java.util.EnumSet;
import java.util.Set;
import java.util.logging.Level;

import javax.annotation.Nonnull;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.sqljet.core.SqlJetAbstractLoggedTest;
import org.tmatesoft.sqljet.core.internal.ISqlJetFileSystem;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetPage;
import org.tmatesoft.sqljet.core.internal.SqlJetFileOpenPermission;
import org.tmatesoft.sqljet.core.internal.SqlJetFileType;
import org.tmatesoft.sqljet.core.internal.SqlJetPagerFlags;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;
import org.tmatesoft.sqljet.core.internal.fs.SqlJetFileSystemsManager;
import org.tmatesoft.sqljet.core.internal.fs.util.SqlJetFileUtil;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetPagerTest extends SqlJetAbstractLoggedTest {

    private ISqlJetFileSystem fileSystem;
    private SqlJetPager pager;
    @SuppressWarnings("null")
	private @Nonnull File file;
    private @Nonnull File testDataBase = new File("src/test/data/db/testdb.sqlite");
	private Set<SqlJetPagerFlags> flags = EnumSet.noneOf(SqlJetPagerFlags.class);

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
        SqlJetFileUtil.deleteFile(file);
    }

    /**
     * Test method for
     * {@link org.tmatesoft.sqljet.core.internal.pager.SqlJetPager#open(org.tmatesoft.sqljet.core.internal.ISqlJetFileSystem, java.io.File, org.tmatesoft.sqljet.core.ISqlJetPageDestructor, int, java.util.Set, org.tmatesoft.sqljet.core.internal.SqlJetFileType, java.util.Set)}
     * .
     */
    @Test
    public final void testOpenMain() throws Exception {
    	pager = new SqlJetPager(fileSystem, file, flags, SqlJetFileType.MAIN_DB, PERM_CREATE);
    }

    /**
     * Test method for
     * {@link org.tmatesoft.sqljet.core.internal.pager.SqlJetPager#open(org.tmatesoft.sqljet.core.internal.ISqlJetFileSystem, java.io.File, org.tmatesoft.sqljet.core.ISqlJetPageDestructor, int, java.util.Set, org.tmatesoft.sqljet.core.internal.SqlJetFileType, java.util.Set)}
     * .
     */
    @Test
    public final void testReadDataBase() throws Exception {
    	pager = new SqlJetPager(fileSystem, testDataBase, flags, SqlJetFileType.MAIN_DB, SqlJetUtility
                .of(SqlJetFileOpenPermission.READONLY));
        ISqlJetMemoryPointer zDbHeader = SqlJetUtility.memoryManager.allocatePtr(100);
        pager.readFileHeader(zDbHeader.remaining(), zDbHeader);
        final int pageCount = pager.getPageCount();
        for (int pageNumber = 1; pageNumber <= pageCount; pageNumber++) {
            final ISqlJetPage page = pager.acquirePage(pageNumber, true);
            ISqlJetMemoryPointer data = page.getData();
            // logger.info("page#"+pageNumber+":"+Arrays.toString(data));
            Assert.assertThat(data, new BaseMatcher<ISqlJetMemoryPointer>() {
                @Override
				public boolean matches(Object a) {
                    for (byte b : ((ISqlJetMemoryPointer) a).getBuffer().asArray()) {
						if (b != 0) {
							return true;
						}
					}
                    return false;
                }

                @Override
				public void describeTo(Description d) {
                    d.appendText("Any non zero byte item in byte[] array");
                }
            });
            page.unref();
        }
    }

    /**
     * Test method for
     * {@link org.tmatesoft.sqljet.core.internal.pager.SqlJetPager#open(org.tmatesoft.sqljet.core.internal.ISqlJetFileSystem, java.io.File, org.tmatesoft.sqljet.core.ISqlJetPageDestructor, int, java.util.Set, org.tmatesoft.sqljet.core.internal.SqlJetFileType, java.util.Set)}
     * .
     */
    @Test
    public final void testWriteMain() throws Exception {
    	pager = new SqlJetPager(fileSystem, file, flags, SqlJetFileType.MAIN_DB, PERM_CREATE);
        int pageSize = pager.getPageSize();
        final int pageNumber = 2;
        final ISqlJetPage page1 = pager.acquirePage(pageNumber, true);
        pager.begin(true);
        page1.write();
        page1.getData().fill(pageSize, (byte) 1);
        final ISqlJetPage page2 = pager.acquirePage(pageNumber + 1, true);
        page2.write();
        page2.getData().fill(pageSize, (byte) 2);
        pager.commitPhaseOne(false);
        pager.commitPhaseTwo();
        page1.unref();
        page2.unref();
        pager.close();

        pager = new SqlJetPager(fileSystem, file, flags, SqlJetFileType.MAIN_DB, PERM_CREATE);
        int pageCount = pager.getPageCount();
        long fileSize = pager.getFile().fileSize();
        logger.info("pages count " + Integer.toString(pageCount));
        logger.info("file size " + Long.toString(fileSize));
        final ISqlJetPage page$1 = pager.acquirePage(pageNumber, true);
        // logger.info("page#"+pageNumber+":"+Arrays.toString(page$1.getData()));
        Assert.assertArrayEquals(page1.getData().getBuffer().asArray(), page$1.getData().getBuffer().asArray());

        final ISqlJetPage page$2 = pager.acquirePage(pageNumber + 1, true);
        // logger.info("page#"+(pageNumber+1)+":"+Arrays.toString(page$2.getData()));
        Assert.assertArrayEquals(page2.getData().getBuffer().asArray(), page$2.getData().getBuffer().asArray());

        pager.begin(true);
        page$1.write();
        page$1.getData().fill(pageSize, (byte) 2);
        page$2.write();
        page$2.getData().fill(pageSize, (byte) 1);
        pager.rollback();
        page$1.unref();
        page$2.unref();
        pager.close();

        pager = new SqlJetPager(fileSystem, file, flags, SqlJetFileType.MAIN_DB, PERM_CREATE);
        pageCount = pager.getPageCount();
        fileSize = pager.getFile().fileSize();
        logger.info("pages count " + Integer.toString(pageCount));
        logger.info("file size " + Long.toString(fileSize));
        final ISqlJetPage page$$1 = pager.acquirePage(pageNumber, true);
        // logger.info("page#"+pageNumber+":"+Arrays.toString(page$$1.getData()));
        Assert.assertArrayEquals(page1.getData().getBuffer().asArray(), page$$1.getData().getBuffer().asArray());

        final ISqlJetPage page$$2 = pager.acquirePage(pageNumber + 1, true);
        // logger.info("page#"+(pageNumber+1)+":"+Arrays.toString(page$$2.getData()));
        Assert.assertArrayEquals(page2.getData().getBuffer().asArray(), page$$2.getData().getBuffer().asArray());

    }

}
