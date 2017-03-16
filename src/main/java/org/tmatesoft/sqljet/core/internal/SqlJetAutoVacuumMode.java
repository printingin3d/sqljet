/**
 * SqlJetAutoVacuum.java
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

import javax.annotation.Nonnull;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public enum SqlJetAutoVacuumMode {

    /** Do not do auto-vacuum */
    NONE,

    /** Do full auto-vacuum */
    FULL,

    /** Incremental vacuum */
    INCR;

    public boolean isAutoVacuum() {
        return this != NONE;
    }

    public boolean isIncrVacuum() {
        return this == INCR;
    }

    public static @Nonnull SqlJetAutoVacuumMode selectVacuumMode(boolean autovacuum, boolean incr) {
        return autovacuum ? incr ? INCR : FULL : NONE;
    }

    public @Nonnull SqlJetAutoVacuumMode changeVacuumMode(boolean autovacuum) {
        return autovacuum ? this : NONE;
    }

    public @Nonnull SqlJetAutoVacuumMode changeIncrMode(boolean incr) {
        return isAutoVacuum() ? incr ? INCR : FULL : NONE;
    }
}
