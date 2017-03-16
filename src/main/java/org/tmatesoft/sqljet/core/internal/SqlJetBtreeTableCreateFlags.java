/**
 * SqlJetBtreeTableCreateFlags.java
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

import java.util.Set;

/**
 * The flags parameter to sqlite3BtreeCreateTable can be the bitwise OR of the
 * following flags:
 * 
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public enum SqlJetBtreeTableCreateFlags {

    /** Table has only 64-bit signed integer keys */
    INTKEY(1),

    /** Table has keys only - no data */
    ZERODATA(2),

    /** Data stored in leaves only. Implies INTKEY */
    LEAFDATA(4);

    private final int value;

    private SqlJetBtreeTableCreateFlags(int value) {
        this.value = value;
    }

    public static byte toByte(Set<SqlJetBtreeTableCreateFlags> flags) {
        byte v = 0;
        for (SqlJetBtreeTableCreateFlags flag : flags) {
            v |= flag.value;
        }
        return v;
    }

    /**
     * @return the value
     */
    public int getValue() {
        return value;
    }

    public boolean hasFlag(int flags) {
        return (flags & value) > 0;
    }

}
