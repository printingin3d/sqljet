package org.tmatesoft.sqljet.core.internal.btree;

import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;

public enum SqlJetPtrMapType {
    PTRMAP_ROOTPAGE(1),
    PTRMAP_FREEPAGE(2),
    PTRMAP_OVERFLOW1(3),
    PTRMAP_OVERFLOW2(4),
    PTRMAP_BTREE(5);
    
    private final int value;

	private SqlJetPtrMapType(int value) {
		this.value = value;
	}

	public int getValue() {
		return value;
	}
	
	public static SqlJetPtrMapType fromValue(int value) throws SqlJetException {
		for (SqlJetPtrMapType v : values()) {
			if (v.value == value) return v;
		}
		throw new SqlJetException(SqlJetErrorCode.CORRUPT, "Unknown value for map type: "+value);
	}
}
