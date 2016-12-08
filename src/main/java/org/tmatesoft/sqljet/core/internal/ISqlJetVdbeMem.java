/**
 * ISqlJetVdbeMem.java
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

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetValueType;
import org.tmatesoft.sqljet.core.schema.SqlJetTypeAffinity;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public interface ISqlJetVdbeMem extends ISqlJetReleasable {

    /**
     * This function is only available internally, it is not part of the
     * external API. It works in a similar way to sqlite3_value_text(), except
     * the data returned is in the encoding specified by the second parameter,
     * which must be one of SQLITE_UTF16BE, SQLITE_UTF16LE or SQLITE_UTF8.
     * 
     * (2006-02-16:) The enc value can be or-ed with SQLITE_UTF16_ALIGNED. If
     * that is the case, then the result must be aligned on an even byte
     * boundary.
     * 
     * @return
     * @throws SqlJetException
     */
    String valueString() throws SqlJetException;

    /**
     * Move data out of a btree key or data field and into a Mem structure. The
     * data or key is taken from the entry that pCur is currently pointing to.
     * offset and amt determine what portion of the data or key to retrieve. key
     * is true to get the key or false to get data. The result is written into
     * the pMem element.
     * 
     * The pMem structure is assumed to be uninitialized. Any prior content is
     * overwritten without being freed.
     * 
     * If this routine fails for any reason (malloc returns NULL or unable to
     * read from the disk) then the pMem is left in an inconsistent state.
     * 
     * @param pCur
     * @param offset
     *            Offset from the start of data to return bytes from.
     * @param amt
     *            Number of bytes to return.
     * @param key
     *            If true, retrieve from the btree key, not data.
     * @return
     * @throws SqlJetException
     */
    void fromBtree(ISqlJetBtreeCursor pCur, int offset, int amt, boolean key) throws SqlJetException;

    /**
     * Return some kind of integer value which is the best we can do at
     * representing the value that *pMem describes as an integer. If pMem is an
     * integer, then the value is exact. If pMem is a floating-point then the
     * value returned is the integer part. If pMem is a string or blob, then we
     * make an attempt to convert it into a integer and return that. If pMem is
     * NULL, return 0.
     * 
     * If pMem is a string, its encoding might be changed.
     */
    long intValue();

    /**
     * Return the best representation of pMem that we can get into a double. If
     * pMem is already a double or an integer, return its value. If it is a
     * string or blob, try to convert it to a double. If it is a NULL, return
     * 0.0.
     */
    double realValue();

    /**
     * @return
     */
    boolean isNull();
    boolean isInt();
    boolean isReal();
    boolean isNumber();
    boolean isString();
    boolean isBlob();

    /**
     * @return
     */
    SqlJetValueType getType();

    /**
     * Converts the object V into a BLOB and then returns a pointer to the
     * converted value.
     * 
     * @return
     * 
     * @throws SqlJetException 
     */
    ISqlJetMemoryPointer valueBlob() throws SqlJetException;

    /**
     * @param affinity
     * @param enc
     * @throws SqlJetException
     */
    void applyAffinity(SqlJetTypeAffinity affinity, SqlJetEncoding enc) throws SqlJetException;
 
    /**
     * Return the serial-type for the value stored in pMem.
     */
    int serialType(int file_format);
    
    /**
     * Write the serialized data blob for the value stored in pMem into buf. It
     * is assumed that the caller has allocated sufficient space. Return the
     * number of bytes written.
     * 
     * nBuf is the amount of space left in buf[]. nBuf must always be large
     * enough to hold the entire field. Except, if the field is a blob with a
     * zero-filled tail, then buf[] might be just the right size to hold
     * everything except for the zero-filled tail. If buf[] is only big enough
     * to hold the non-zero prefix, then only write that prefix into buf[]. But
     * if buf[] is large enough to hold both the prefix and the tail then write
     * the prefix and set the tail to all zeros.
     * 
     * Return the number of bytes actually written into buf[]. The number of
     * bytes in the zero-filled tail is included in the return value only if
     * those bytes were zeroed in buf[].
     */
    int serialPut(ISqlJetMemoryPointer buf, int nBuf, int file_format);
 
    int compare(ISqlJetVdbeMem that) throws SqlJetException;
}