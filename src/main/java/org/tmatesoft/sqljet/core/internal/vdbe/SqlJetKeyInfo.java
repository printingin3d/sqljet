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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetKeyInfo;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetResultWithOffset;
import org.tmatesoft.sqljet.core.internal.memory.SqlJetVarintResult32;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 * 
 */
public class SqlJetKeyInfo implements ISqlJetKeyInfo {
    /* Text encoding - one of the TEXT_Utf values */
    private final @Nonnull SqlJetEncoding enc;

    /* If defined an aSortOrder[i] is true, sort DESC */
    private boolean[] aSortOrder = new boolean[0];

    public SqlJetKeyInfo(@Nonnull SqlJetEncoding enc) {
        this.enc = enc;
    }

    @Override
    public SqlJetUnpackedRecord recordUnpack(int nKey, ISqlJetMemoryPointer pKey) {
        List<ISqlJetVdbeMem> pMem = new ArrayList<>(this.aSortOrder.length + 1);
        SqlJetVarintResult32 res = pKey.getVarint32();
        int idx = res.getOffset();
        int d = res.getValue();
        int u = 0;

        while (idx < res.getValue() && u < this.aSortOrder.length + 1) {
            SqlJetVarintResult32 res2 = pKey.pointer(idx).getVarint32();
            idx += res2.getOffset();
            if (d >= nKey && SqlJetVdbeSerialType.serialTypeLen(res2.getValue()) > 0) {
                break;
            }
            SqlJetResultWithOffset<ISqlJetVdbeMem> result = SqlJetVdbeMemFactory.serialGet(pKey, d, res2.getValue(),
                    this.enc);
            d += result.getOffset();
            pMem.add(result.getValue());
            u++;
        }
        return new SqlJetUnpackedRecord(this, pMem);
    }

    /**
     * @return the nField
     */
    public int getNField() {
        return aSortOrder.length;
    }

    /**
     * @param field
     *            the nField to set
     */
    public void setNField(int field) {
        aSortOrder = new boolean[field];
    }

    /**
     * @return the enc
     */
    public @Nonnull SqlJetEncoding getEnc() {
        return enc;
    }

    public void setSortOrder(int i, boolean desc) throws SqlJetException {
        if (i >= aSortOrder.length) {
            throw new SqlJetException(SqlJetErrorCode.ERROR);
        }
        this.aSortOrder[i] = desc;
    }

    public boolean getSortOrder(int i) throws SqlJetException {
        if (i >= aSortOrder.length) {
            return false;
        }
        return this.aSortOrder[i];
    }

}
