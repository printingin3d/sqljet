/**
 * SqlJetKeyInfo.java
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

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetCollSeq;
import org.tmatesoft.sqljet.core.internal.ISqlJetKeyInfo;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.SqlJetUnpackedRecordFlags;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetKeyInfo implements ISqlJetKeyInfo {

    /* Text encoding - one of the TEXT_Utf* values */
    private SqlJetEncoding enc;

    /* Number of entries in aColl[] */
    private int nField;

    /* If defined an aSortOrder[i] is true, sort DESC */
    private boolean[] aSortOrder;

    /* Collating sequence for each term of the key */
    private ISqlJetCollSeq[] aColl;

    @Override
	public SqlJetUnpackedRecord recordUnpack(int nKey, ISqlJetMemoryPointer pKey) {
        SqlJetKeyInfo pKeyInfo = this;
        int[] szHdr = new int[1];
        SqlJetVdbeMem[] pMem;

        SqlJetUnpackedRecord p = new SqlJetUnpackedRecord();

        p.flags = SqlJetUtility.of(SqlJetUnpackedRecordFlags.NEED_DESTROY);
        p.pKeyInfo = pKeyInfo;
        p.nField = pKeyInfo.nField + 1;
        p.aMem = pMem = new SqlJetVdbeMem[p.nField];
        int idx = pKey.getVarint32(szHdr);
        int d = szHdr[0];
        int u = 0;

        while (idx < szHdr[0] && u < p.nField) {
            int[] serial_type = new int[1];

            idx += pKey.pointer(idx).getVarint32(serial_type);
            if (d >= nKey && SqlJetVdbeSerialType.serialTypeLen(serial_type[0]) > 0) {
				break;
			}
            pMem[u] = SqlJetVdbeMem.obtainInstance();
            pMem[u].enc = pKeyInfo.enc;
            pMem[u].db = null;
            pMem[u].flags = EnumSet.noneOf(SqlJetVdbeMemFlags.class);
            pMem[u].zMalloc = null;
            d += SqlJetVdbeSerialType.serialGet(pKey.pointer(d), serial_type[0], pMem[u]);
            u++;
        }
        assert (u <= pKeyInfo.nField + 1);
        p.nField = u;
        return p;
    }

    /**
     * @return the nField
     */
    public int getNField() {
        return nField;
    }

    /**
     * @param field the nField to set
     */
    public void setNField(int field) {
        nField = field;
        aSortOrder = new boolean[nField];
        aColl = new ISqlJetCollSeq[nField];
    }

    /**
     * @return the enc
     */
    public SqlJetEncoding getEnc() {
        return enc;
    }

    /**
     * @param enc the enc to set
     */
    public void setEnc(SqlJetEncoding enc) {
        this.enc = enc;
    }
    
    public void setSortOrder(int i, boolean desc) throws SqlJetException {
        if(i>=nField) {
			throw new SqlJetException(SqlJetErrorCode.ERROR);
		}
        this.aSortOrder[i]=desc;
    }
    
    public boolean getSortOrder(int i) throws SqlJetException {
        if(i>=nField) {
			throw new SqlJetException(SqlJetErrorCode.ERROR);
		}
        return this.aSortOrder[i];
    }

    public void setCollating(int i, ISqlJetCollSeq coll) throws SqlJetException {
        if(i>=nField) {
			throw new SqlJetException(SqlJetErrorCode.ERROR);
		}
        this.aColl[i]=coll;
    }
    
    public ISqlJetCollSeq getCollating(int i) throws SqlJetException {
        if(i>=nField) {
			throw new SqlJetException(SqlJetErrorCode.ERROR);
		}
        return this.aColl[i];
    }
    
}
