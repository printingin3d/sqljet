/**
 * Memory.java
 * Copyright (C) 2009-2013 TMate Software Ltd
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

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.tmatesoft.sqljet.core.internal.memory.SqlJetMemoryManager;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
@RunWith(Parameterized.class)
public class MemoryBufferTest {
    private final ISqlJetMemoryManager memoryManager = new SqlJetMemoryManager();
    private final SqlJetMemoryBufferType bufferType;

    public MemoryBufferTest(SqlJetMemoryBufferType bufferType) {
		this.bufferType = bufferType;
	}
    
    @Parameters
    public static Collection<Object[]> data() {
    	return Stream.of(SqlJetMemoryBufferType.values()).map(bt -> new Object[] { bt }).collect(Collectors.toList());
    }

	@Test
    public void testByteArray() {
        ISqlJetMemoryBuffer b = memoryManager.allocate(ISqlJetMemoryManager.BYTE_SIZE, bufferType);
        b.putByte(0, (byte) 1);
        Assert.assertEquals(1, b.getByte(0));
        b.putByte(0, (byte) -1);
        Assert.assertEquals(-1, b.getByte(0));
        b.putByte(0, Byte.MAX_VALUE);
        Assert.assertEquals(Byte.MAX_VALUE, b.getByte(0));
        b.putByte(0, Byte.MIN_VALUE);
        Assert.assertEquals(Byte.MIN_VALUE, b.getByte(0));
    }

    @Test
    public void testUnsignedByteArray() {
        ISqlJetMemoryBuffer b = memoryManager.allocate(ISqlJetMemoryManager.BYTE_SIZE, bufferType);
        b.putByteUnsigned(0, 0xFF);
        Assert.assertEquals(0xFF, b.getByteUnsigned(0));
        b.putByteUnsigned(0, (byte) 1);
        Assert.assertEquals(1, b.getByteUnsigned(0));
        Assert.assertEquals(1, b.getByte(0));
        b.putByte(0, (byte) 1);
        Assert.assertEquals(1, b.getByteUnsigned(0));
        Assert.assertEquals(1, b.getByte(0));
    }

    @Test
    public void testShortArray() {
        ISqlJetMemoryBuffer b = memoryManager.allocate(ISqlJetMemoryManager.SHORT_SIZE, bufferType);
        b.putShort(0, (short) 1);
        Assert.assertEquals(1, b.getShort(0));
        b.putShort(0, (short) -1);
        Assert.assertEquals(-1, b.getShort(0));
        b.putShort(0, Short.MAX_VALUE);
        Assert.assertEquals(Short.MAX_VALUE, b.getShort(0));
        b.putShort(0, Short.MIN_VALUE);
        Assert.assertEquals(Short.MIN_VALUE, b.getShort(0));
    }

    @Test
    public void testUnsignedShortArray() {
        ISqlJetMemoryBuffer b = memoryManager.allocate(ISqlJetMemoryManager.SHORT_SIZE, bufferType);
        b.putShortUnsigned(0, 0xFFFF);
        Assert.assertEquals(0xFFFF, b.getShortUnsigned(0));
        b.putShortUnsigned(0, (short) 1);
        Assert.assertEquals(1, b.getShortUnsigned(0));
        Assert.assertEquals(1, b.getShort(0));
        b.putShort(0, (short) 1);
        Assert.assertEquals(1, b.getShortUnsigned(0));
        Assert.assertEquals(1, b.getShort(0));
    }

    @Test
    public void testIntArray() {
        ISqlJetMemoryBuffer b = memoryManager.allocate(ISqlJetMemoryManager.INT_SIZE, bufferType);
        b.putInt(0, 1);
        Assert.assertEquals(1, b.getInt(0));
        b.putInt(0, -1);
        Assert.assertEquals(-1, b.getInt(0));
        b.putInt(0, Integer.MAX_VALUE);
        Assert.assertEquals(Integer.MAX_VALUE, b.getInt(0));
        b.putInt(0, Integer.MIN_VALUE);
        Assert.assertEquals(Integer.MIN_VALUE, b.getInt(0));
    }

    @Test
    public void testUnsignedIntArray() {
        ISqlJetMemoryBuffer b = memoryManager.allocate(ISqlJetMemoryManager.INT_SIZE, bufferType);
        b.putIntUnsigned(0, 0xFFFFFFFFL);
        Assert.assertEquals(0xFFFFFFFFL, b.getIntUnsigned(0));
        b.putIntUnsigned(0, 1);
        Assert.assertEquals(1, b.getIntUnsigned(0));
        Assert.assertEquals(1, b.getInt(0));
        b.putInt(0, 1);
        Assert.assertEquals(1, b.getIntUnsigned(0));
        Assert.assertEquals(1, b.getInt(0));
    }
}
