/**
 * SqlJetFileUtil.java
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
package org.tmatesoft.sqljet.core.internal.fs.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.internal.SqlJetUtility;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetFileUtil {

    public static final int ATTEMPTS_COUNT = SqlJetUtility.getIntSysProp("sqljet.fs.win32_retry_count", 100);
    public static final SqlJetOsType OS = new SqlJetOsType();

    public static boolean deleteFile(@Nonnull File file) {
        return deleteFile(file, false);
    }

    public static boolean deleteFile(@Nonnull File file, boolean sync) {
        if (OS.isWindows() && !sync) {
        	file.deleteOnExit();
        }
        if (!sync || file.isDirectory() || !file.exists()) {
            return file.delete();
        }
        long sleep = 1;
        for (int i = 0; i < ATTEMPTS_COUNT; i++) {
            if (file.delete() && !file.exists()) {
                return true;
            }
            if (!file.exists()) {
                return true;
            }
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                return false;
            }
            if (sleep < 128) {
                sleep = sleep * 2;
            }
        }
        return false;
    }

    public static RandomAccessFile openFile(@Nonnull File file, String mode) throws FileNotFoundException {
        if (file.getParentFile() != null && !file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        if (OS.isWindows()) {
            long sleep = 1;
            for (int i = 0; i < ATTEMPTS_COUNT; i++) {
                try {
                    return new RandomAccessFile(file, mode);
                } catch (FileNotFoundException e) {
                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException e1) {
                        return null;
                    }
                }
                if (sleep < 128) {
                    sleep = sleep * 2;
                }
            }
        }
        return new RandomAccessFile(file, mode);
    }

    private static class SqlJetOsType {
        private final boolean windows;

        public SqlJetOsType() {
            final String osName = System.getProperty("os.name");
            if (osName==null) {
                this.windows = false;
            } else {
	            final String osNameLC = osName.toLowerCase();
	
	            this.windows = osNameLC.indexOf("windows") >= 0 || osNameLC.indexOf("os/2") >= 0;
            }
        }

        public boolean isWindows() {
            return windows;
        }
    }

}
