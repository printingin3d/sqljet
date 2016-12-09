/**
 * SqlJetSerialType.java
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
package org.tmatesoft.sqljet.core.internal.vdbe;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetVdbeSerialType {

    private static final byte aSize[] = { 0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0 };

    /**
     ** Return the length of the data corresponding to the supplied serial-type.
     */
    public static int serialTypeLen(int serialType) {
        if (serialType >= 12) {
            return (serialType - 12) / 2;
        } else {
            return aSize[serialType];
        }
    }

}
