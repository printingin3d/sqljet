/**
 * SqlJetEncoding.java
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
package org.tmatesoft.sqljet.core;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.annotation.Nonnull;

/**
 * These constant define integer codes that represent the various text encodings
 * supported by SQLite.
 * 
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public enum SqlJetEncoding {

    /**
     * UTF-8 encoding.
     */
    UTF8(StandardCharsets.UTF_8, 1),

    /**
     * UTF-16 little-endian.
     */
    UTF16LE(StandardCharsets.UTF_16LE, 2),

    /**
     * UTF-16 big-endian.
     */
    UTF16BE(StandardCharsets.UTF_16BE, 3),

    /** Use native byte order */
    UTF16(StandardCharsets.UTF_16, 4);

    @Nonnull
    private final Charset charset;
    private final int value;

    private SqlJetEncoding(@Nonnull Charset charset, int value) {
        this.charset = charset;
        this.value = value;
    }

    public @Nonnull Charset getCharset() {
        return charset;
    }

    /**
     * Get encoding coded to integer.
     * 
     * @return code
     */
    public int getValue() {
        return value;
    }

    public boolean isSupported() {
        return this == UTF8 || this == UTF16LE || this == UTF16BE;
    }

    /**
     * Get charset constant from string with charset name.
     * 
     * @param s
     *            string with charset name
     * @return decoded charset constant or null if sring doesn't contains known
     *         charser name
     */
    public static SqlJetEncoding decode(String s) {
        for (SqlJetEncoding e : values()) {
            if (e.getCharset().name().equalsIgnoreCase(s)) {
                return e;
            }
        }
        return null;
    }

    /**
     * Get charset constant from integer coded value.
     * 
     * @param value
     *            integer code of an encoding
     * @return decoded charset constant or null if value is unknown
     */
    public static SqlJetEncoding decodeInt(int value) {
        for (SqlJetEncoding e : values()) {
            if (e.value == value) {
                return e;
            }
        }
        return null;
    }
}
