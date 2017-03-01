package org.tmatesoft.sqljet.core.simpleschema;

import org.tmatesoft.sqljet.core.internal.schema.SqlJetColumnNotNull;
import org.tmatesoft.sqljet.core.internal.schema.SqlJetColumnPrimaryKey;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnConstraint;
import org.tmatesoft.sqljet.core.schema.ISqlJetColumnDef;

public enum SqlJetSimpleColumnContraint {
	NOT_NULL,
	PRIMARY_KEY,
	AUTOINCREMENTED_PRIMARY_KEY;
	
	public ISqlJetColumnConstraint toColumnConstraint(ISqlJetColumnDef col) {
		switch (this) {
		case NOT_NULL:
			return new SqlJetColumnNotNull(col, null, null);
		case PRIMARY_KEY:
			return new SqlJetColumnPrimaryKey(col, null, null, false, null);
		case AUTOINCREMENTED_PRIMARY_KEY:
			return new SqlJetColumnPrimaryKey(col, null, null, true, null);
		}
		return null;
	}
}
