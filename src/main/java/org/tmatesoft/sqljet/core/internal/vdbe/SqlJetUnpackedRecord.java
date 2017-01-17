/**
 * SqlJetUnpackedRecord.java
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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetUnpackedRecord;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetResultWithOffset;
import org.tmatesoft.sqljet.core.internal.SqlJetUnpackedRecordFlags;
import org.tmatesoft.sqljet.core.internal.memory.SqlJetVarintResult32;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetUnpackedRecord implements ISqlJetUnpackedRecord {

    /* Collation and sort-order information */
    private final SqlJetKeyInfo pKeyInfo;

    /* Boolean settings. UNPACKED_... below */
    private final Set<SqlJetUnpackedRecordFlags> flags = EnumSet.noneOf(SqlJetUnpackedRecordFlags.class);

    /* Values */
    private final List<ISqlJetVdbeMem> aMem;

	public SqlJetUnpackedRecord(SqlJetKeyInfo pKeyInfo, List<ISqlJetVdbeMem> aMem) {
		this.pKeyInfo = pKeyInfo;
		this.aMem = aMem;
	}

    @Override
	public int recordCompare(int nKey1, ISqlJetMemoryPointer pKey1) throws SqlJetException {
        int i = 0;
        int rc = 0;
        
        SqlJetVarintResult32 res = pKey1.getVarint32();
        int szHdr1 = res.getValue(); /* Number of bytes in header */
        int idx1 = res.getOffset(); /* Offset into aKey[] of next header element */
        int d1 = szHdr1;                   /* Offset into aKey[] of next data element */
        if (this.flags.contains(SqlJetUnpackedRecordFlags.IGNORE_ROWID)) {
            szHdr1--;
        }
        for (ISqlJetVdbeMem mem : aMem) {
			if (idx1 < szHdr1) {
	            /* Read the serial types for the next element in each key. */
	            SqlJetVarintResult32 res2 = pKey1.getVarint32(idx1);
	            idx1 += res2.getOffset();
	            if (d1 >= nKey1 && SqlJetVdbeSerialType.serialTypeLen(res2.getValue()) > 0) {
					break;
				}
	
	            /*
	             * Extract the values to be compared.
	             */
	            SqlJetResultWithOffset<ISqlJetVdbeMem> result = SqlJetVdbeMemFactory.serialGet(pKey1, d1, res2.getValue(), pKeyInfo.getEnc());
	            d1 += result.getOffset();
	
	            /*
	             * Do the comparison
	             */
	            rc = result.getValue().compareTo(mem);
	            if (rc != 0) {
	                break;
	            }
	            i++;
	        }
		}

        if (rc == 0) {
            /*
             * rc==0 here means that one of the keys ran out of fields and* all
             * the fields up to that point were equal. If the UNPACKED_INCRKEY*
             * flag is set, then break the tie by treating key2 as larger.* If
             * the UPACKED_PREFIX_MATCH flag is set, then keys with common
             * prefixes* are considered to be equal. Otherwise, the longer key
             * is the* larger. As it happens, the pPKey2 will always be the
             * longer* if there is a difference.
             */
            if (this.flags.contains(SqlJetUnpackedRecordFlags.INCRKEY)) {
                rc = -1;
            } else if (this.flags.contains(SqlJetUnpackedRecordFlags.PREFIX_MATCH)) {
                /* Leave rc==0 */
            } else if (idx1 < szHdr1) {
                rc = 1;
            }
        } else if (pKeyInfo.getSortOrder(i)) {
            rc = -rc;
        }

        return rc;
    }

    /**
     * @return the flags
     */
    public Set<SqlJetUnpackedRecordFlags> getFlags() {
        return flags;
    }
}
