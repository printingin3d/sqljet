/**
 * SqlJetMemoryManager.java
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
package org.tmatesoft.sqljet.core.internal.memory;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryBuffer;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryManager;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.SqlJetMemoryBufferType;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetMemoryManager implements ISqlJetMemoryManager {

    private final SqlJetMemoryBufferType defaultBufferType = SqlJetUtility.getEnumSysProp(
            "SqlJetMemoryManager.defaultBufferType", SqlJetMemoryBufferType.ARRAY);

    @Override
	public @Nonnull ISqlJetMemoryPointer allocatePtr(int size) {
        return allocate(size).getPointer(0);
    }

    @Override
	public @Nonnull ISqlJetMemoryPointer allocatePtr(int size, SqlJetMemoryBufferType bufferType) {
        return allocate(size, bufferType).getPointer(0);
    }

	@Override
	public @Nonnull ISqlJetMemoryPointer allocatePtr(@Nonnull byte[] bytes) {
		return allocate(bytes, defaultBufferType).getPointer(0);
	}

    @Override
	public @Nonnull ISqlJetMemoryBuffer allocate(final int size) {
        return allocate(size, defaultBufferType);
    }

    @Override
	public @Nonnull ISqlJetMemoryBuffer allocate(int size, SqlJetMemoryBufferType bufferType) {
        final ISqlJetMemoryBuffer buffer;
        switch (bufferType) {
        case ARRAY:
            buffer = new SqlJetByteArrayBuffer(size);
            break;
        case BUFFER:
            buffer = new SqlJetByteBuffer(size);
            break;
        case DIRECT:
            buffer = new SqlJetDirectByteBuffer(size);
            break;
        default:
            buffer = new SqlJetByteArrayBuffer(size);
        }
        return buffer;
    }
    
    @Override
    public @Nonnull ISqlJetMemoryBuffer allocate(@Nonnull byte[] bytes, SqlJetMemoryBufferType bufferType) {
		final ISqlJetMemoryBuffer buffer;
		switch (bufferType) {
		case ARRAY:
			buffer = new SqlJetByteArrayBuffer(bytes);
			break;
		case DIRECT:
		case BUFFER:
			buffer = new SqlJetByteBuffer(bytes);
			break;
		default:
			buffer = new SqlJetByteArrayBuffer(bytes);
		}
		return buffer;
    }

}
