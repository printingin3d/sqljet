/**
 * SqlJetEmptyMutex.java
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
package org.tmatesoft.sqljet.core.internal.mutex;

import org.tmatesoft.sqljet.core.ISqlJetMutex;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.table.ISqlJetConsumer;
import org.tmatesoft.sqljet.core.table.ISqlJetTransaction;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetEmptyMutex implements ISqlJetMutex {

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.internal.ISqlJetMutex#attempt()
     */
    @Override
	public boolean attempt() {
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.internal.ISqlJetMutex#enter()
     */
    @Override
	public void enter() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.internal.ISqlJetMutex#held()
     */
    @Override
	public boolean held() {
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.tmatesoft.sqljet.core.internal.ISqlJetMutex#leave()
     */
    @Override
	public void leave() {
    }

	@Override
	public <T> T run(ISqlJetTransaction<T, ISqlJetMutex> op) throws SqlJetException {
		return op.run(this);
	}

	@Override
	public void runVoid(ISqlJetConsumer<ISqlJetMutex> op) throws SqlJetException {
		op.run(this);		
	}

}
