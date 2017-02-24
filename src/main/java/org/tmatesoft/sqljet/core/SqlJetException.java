/**
 * SqlJetException.java
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
package org.tmatesoft.sqljet.core;

import javax.annotation.Nonnull;

/**
 * SqlJet exception wraps error code {@link SqlJetErrorCode}
 * 
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetException extends Exception {

    private static final long serialVersionUID = -7132771040442635370L;

    private final @Nonnull SqlJetErrorCode errorCode;

    /**
     * Get error code.
     * 
     * @return the errorCode
     */
    public @Nonnull SqlJetErrorCode getErrorCode() {
        return errorCode;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Throwable#toString()
     */
    @Override
    public String toString() {
        return super.toString() + ": error code is " + errorCode.name();
    }

    /**
     * Create SqlJet exception with given error code.
     * 
     * @param errorCode
     *            the error code.
     * 
     */
    public SqlJetException(@Nonnull SqlJetErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    /**
     * Create SqlJet exception with given error code and message.
     * 
     * @param errorCode
     *            the error code.
     * @param message
     *            the message string.
     */
    public SqlJetException(@Nonnull SqlJetErrorCode errorCode, final String message) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * Create SqlJet exception with given error code and reason.
     * 
     * @param errorCode
     *            the error code.
     * @param cause
     *            the reason.
     */
    public SqlJetException(@Nonnull SqlJetErrorCode errorCode, final Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    /**
     * Create SqlJet exception with error code {@link SqlJetErrorCode#MISUSE}
     * and given message.
     * 
     * @param message
     *            the message string.
     */
    public SqlJetException(final String message) {
        this(SqlJetErrorCode.MISUSE, message);
    }

    /**
     * Create SqlJet exception with error code {@link SqlJetErrorCode#MISUSE}
     * and given reason.
     * 
     * @param cause
     *            the reason.
     */
    public SqlJetException(final Throwable cause) {
        this(SqlJetErrorCode.MISUSE, cause);
    }

    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (message == null) {
            message = this.errorCode.name();
        }
        return message;
    }

}
