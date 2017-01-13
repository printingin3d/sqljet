/**
 * SqlJetTypeAffinity.java
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
 */
package org.tmatesoft.sqljet.core.schema;

/**
 * Type affinity.
 * 
 * @author TMate Software Ltd.
 * @author Dmitry Stadnik (dtrace@seznam.cz)
 */
public enum SqlJetTypeAffinity {

    TEXT, NUMERIC, INTEGER, REAL, NONE;

    /**
     * Follows algorithm defined in SQLite documentation to infer type affinity
     * from column type.
     */
    public static SqlJetTypeAffinity decode(String s) {
        s = (s == null) ? "" : s.trim().toUpperCase();

        // 1. If the datatype contains the string "INT" then it is assigned
        // INTEGER affinity.
        if (s.contains("INT")) {
            return INTEGER;
        }

        // 2. If the datatype of the column contains any of the strings "CHAR",
        // "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the
        // type VARCHAR contains the string "CHAR" and is thus assigned TEXT
        // affinity.
        if (s.contains("CHAR") || s.contains("CLOB") || s.contains("TEXT")) {
            return TEXT;
        }

        // 3. If the datatype for a column contains the string "BLOB" or if no
        // datatype is specified then the column has affinity NONE.
        if (s.contains("BLOB") || s.isEmpty()) {
            return NONE;
        }

        // 4. If the datatype for a column contains any of the strings "REAL",
        // "FLOA", or "DOUB" then the column has REAL affinity.
        if (s.contains("REAL") || s.contains("FLOA") || s.contains("DOUB")) {
            return REAL;
        }

        // 5. Otherwise, the affinity is NUMERIC.
        return NUMERIC;
    }
}
