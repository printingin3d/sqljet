/**
 * SqlJetFileTest.java
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
package org.tmatesoft.sqljet.core.internal;

import java.util.Set;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;
import org.tmatesoft.sqljet.core.internal.fs.SqlJetFileSystem;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public class SqlJetFileTest extends SqlJetAbstractFileSystemMockTest {

    protected ISqlJetFile file;
    protected ISqlJetFile file2;
    protected ISqlJetFile file3;

    @Override
    protected void setUpInstances() throws Exception {
    	fileSystem = new SqlJetFileSystem();
        super.setUpInstances();
        file = fileSystem.open(path, SqlJetFileType.MAIN_DB, PERM_CREATE);
        file2 = fileSystem.open(path, SqlJetFileType.MAIN_DB, PERM_CREATE);
        file3 = fileSystem.open(path, SqlJetFileType.MAIN_DB, PERM_CREATE);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.tmatesoft.sqljet.core.SqlJetAbstractFileSystemMockTest#cleanUpInstances
     * ()
     */
    @Override
    protected void cleanUpInstances() throws Exception {
        try {
            try {
                if (file != null) {
					file.close();
				}
            } finally {
                try {
                    if (file2 != null) {
						file2.close();
					}
                } finally {
                    if (file3 != null) {
						file3.close();
					}
                }
            }
        } finally {
            super.cleanUpInstances();
        }
    }

	@Test
    public void testPermissions() throws Exception {
        final String msg = "File must have permissions to which was opened";
        final Set<SqlJetFileOpenPermission> p = file.getPermissions();
        Assert.assertNotNull(msg, p);
        Assert.assertTrue(msg, p.containsAll(PERM_CREATE));

    }

	@Test(expected = AssertionError.class)
    public void testClose() throws Exception {
        file.close();
        file.sync(null);
        Assert.fail("Closed file should not allow perform any input-output");

    }

	@Test
    public void testReadEmpty() throws Exception {
        Assert.assertTrue(0 == path.length());
        final ISqlJetMemoryPointer b = SqlJetUtility.memoryManager.allocatePtr(1);
        final int r = file.read(b, 1, 0);
        Assert.assertEquals("Read empty file should return empty data", 0, r);
    }

	@Test
    public void testWriteRead() throws Exception {
        Assert.assertTrue(0 == path.length());
        final ISqlJetMemoryPointer wb = SqlJetUtility.wrapPtr(new byte[] { 1 });
        file.write(wb, 1, 0);
        final ISqlJetMemoryPointer rb = SqlJetUtility.memoryManager.allocatePtr(1);
        file.read(rb, 1, 0);
        Assert.assertArrayEquals("Reading should get the same data as it was written", wb.getBuffer().asArray(), rb
                .getBuffer().asArray());
    }

	@Test
    public void testSize() throws Exception {
        final long fileSize = file.fileSize();
        final ISqlJetMemoryPointer wb = SqlJetUtility.wrapPtr(new byte[] { 1 });
        file.write(wb, wb.remaining(), fileSize);
        Assert.assertTrue("File size should be increased after writing after end of file", file.fileSize() > fileSize);
    }

	@Test
    public void testTruncate() throws Exception {
        Assert.assertTrue(0 == path.length());
        final ISqlJetMemoryPointer wb = SqlJetUtility.wrapPtr(new byte[] { 1 });
        final long fileSize = file.fileSize();
        file.write(wb, wb.remaining(), fileSize);
        file.truncate(0);
        Assert.assertTrue("File size should be decreased after truncating", 0 == file.fileSize());
    }

	@Test
    public void testLockShared() throws Exception {
        Assert.assertTrue(file.lock(SqlJetLockType.SHARED));
        Assert.assertTrue(file2.lock(SqlJetLockType.SHARED));
    }

	@Test
    public void testLockReserved() throws Exception {
        Assert.assertTrue(file.lock(SqlJetLockType.SHARED));
        Assert.assertTrue(file.lock(SqlJetLockType.RESERVED));
        Assert.assertTrue(file2.lock(SqlJetLockType.SHARED));
        Assert.assertFalse(file2.lock(SqlJetLockType.RESERVED));
    }

	@Test
    public void testLockExclusive() throws Exception {
        Assert.assertTrue(file.lock(SqlJetLockType.SHARED));
        Assert.assertTrue(file2.lock(SqlJetLockType.SHARED));
        Assert.assertTrue(file.lock(SqlJetLockType.RESERVED));
        Assert.assertFalse(file.lock(SqlJetLockType.EXCLUSIVE));
    }

	@Test
    public void testCheckReserved() throws Exception {
        Assert.assertTrue(file.lock(SqlJetLockType.SHARED));
        Assert.assertTrue(file.lock(SqlJetLockType.RESERVED));
        Assert.assertTrue(file.checkReservedLock());
        Assert.assertTrue(file.lock(SqlJetLockType.EXCLUSIVE));
        Assert.assertTrue(file.checkReservedLock());
    }

	@Test
    public void testLockSharedThreads() throws Exception {
        Future<Boolean> lock = execThread(() -> Boolean.valueOf(file.lock(SqlJetLockType.SHARED)));
        Assert.assertEquals(Boolean.TRUE, lock.get());
        Future<Boolean> lock2 = execThread(() -> Boolean.valueOf(file2.lock(SqlJetLockType.SHARED)));
        Assert.assertEquals(Boolean.TRUE, lock2.get());
    }

	@Test
    public void testLockReservedThreads() throws Exception {
        Future<Boolean> lock = execThread(
        		() -> Boolean.valueOf(file.lock(SqlJetLockType.SHARED) && file.lock(SqlJetLockType.RESERVED))
            );
        Assert.assertEquals(Boolean.TRUE, lock.get());
        Future<Boolean> lock2 = execThread(
        		() -> Boolean.valueOf(file2.lock(SqlJetLockType.SHARED) && file2.lock(SqlJetLockType.RESERVED))
            );
        Assert.assertEquals(Boolean.FALSE, lock2.get());
    }

	@Test
    public void testLockExclusiveThreads() throws Exception {
        Future<Boolean> lock = execThread(
        		() -> Boolean.valueOf(file.lock(SqlJetLockType.SHARED))
        	);
        Assert.assertEquals(Boolean.TRUE, lock.get());
        final Future<Boolean> lock2 = execThread(
        		() -> Boolean.valueOf(file2.lock(SqlJetLockType.SHARED) && file2.lock(SqlJetLockType.RESERVED)
                        && file2.lock(SqlJetLockType.EXCLUSIVE))
            );
        Assert.assertEquals(Boolean.FALSE, lock2.get());
    }

	@Test
    public void testLockCheckReservedThreads() throws Exception {
        Future<Boolean> lock = execThread(
        		() -> Boolean.valueOf(file.lock(SqlJetLockType.SHARED) && file.lock(SqlJetLockType.RESERVED))
            );
        Assert.assertEquals(Boolean.TRUE, lock.get());
        Future<Boolean> lock2 = execThread(
        		() -> Boolean.valueOf(file2.checkReservedLock())
            );
        Assert.assertEquals(Boolean.TRUE, lock2.get());
    }

	@Test
    public void testLockSharedThreads2() throws Exception {
        Future<Boolean> lock = execThread(
        		() -> Boolean.valueOf(file.lock(SqlJetLockType.SHARED))
            );
        Assert.assertEquals(Boolean.TRUE, lock.get());
        Future<Boolean> lock2 = execThread(
        		() -> Boolean.valueOf(file.lock(SqlJetLockType.SHARED))
            );
        Assert.assertEquals(Boolean.TRUE, lock2.get());
    }

	@Test
    public void testLockReservedThreads2() throws Exception {
        Future<Boolean> lock = execThread(
        		() -> Boolean.valueOf(file.lock(SqlJetLockType.SHARED) && file.lock(SqlJetLockType.RESERVED))
            );
        Assert.assertEquals(Boolean.TRUE, lock.get());
        Future<Boolean> lock2 = execThread(
        		() -> Boolean.valueOf(file.lock(SqlJetLockType.SHARED) && file2.lock(SqlJetLockType.RESERVED))
            );
        Assert.assertEquals(Boolean.FALSE, lock2.get());
    }

	@Test
    public void testLockExclusiveThreads2() throws Exception {
        Future<Boolean> lock = execThread(() -> Boolean.valueOf(file.lock(SqlJetLockType.SHARED)));
        Assert.assertEquals(Boolean.TRUE, lock.get());
        Future<Boolean> lock2 = execThread(
        		() -> Boolean.valueOf(file2.lock(SqlJetLockType.SHARED) && file2.lock(SqlJetLockType.RESERVED)
                        && file.lock(SqlJetLockType.EXCLUSIVE))
            );
        Assert.assertEquals(Boolean.FALSE, lock2.get());
    }

	@Test
    public void testLockCheckReservedThreads2() throws Exception {
        Future<Boolean> lock = execThread(
        		() -> Boolean.valueOf(file.lock(SqlJetLockType.SHARED) && file.lock(SqlJetLockType.RESERVED))
            );
        Assert.assertEquals(Boolean.TRUE, lock.get());
        Future<Boolean> lock2 = execThread(() -> Boolean.valueOf(file.checkReservedLock()));
        Assert.assertEquals(Boolean.TRUE, lock2.get());
    }

	@Test
    public void testLockClose() throws Exception {
        Assert.assertTrue(file.lock(SqlJetLockType.SHARED));
        Assert.assertTrue(file.lock(SqlJetLockType.RESERVED));
        Assert.assertTrue(file.lock(SqlJetLockType.EXCLUSIVE));
        file2.close();
        Assert.assertFalse(file3.lock(SqlJetLockType.SHARED));
    }

	@Test
    public void testUnlock() throws Exception {
        Assert.assertTrue(file.unlock(SqlJetLockType.SHARED));
        Assert.assertTrue(file.unlock(SqlJetLockType.NONE));
    }

}
