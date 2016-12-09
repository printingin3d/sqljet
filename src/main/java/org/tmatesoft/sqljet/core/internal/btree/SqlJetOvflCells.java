package org.tmatesoft.sqljet.core.internal.btree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SqlJetOvflCells implements Iterable<SqlJetOvflCell> {
    private final List<SqlJetOvflCell> ovfl;

	private SqlJetOvflCells(List<SqlJetOvflCell> aOvfl) {
		this.ovfl = aOvfl;
	}
	
	public SqlJetOvflCells() {
		this(new ArrayList<>());
	}

	public void clear() {
		ovfl.clear();
	}
	
	public boolean isEmpty() {
		return ovfl.isEmpty();
	}
	
	public void add(SqlJetOvflCell value) {
		ovfl.add(0, value);
	}
	
	public int size() {
		return ovfl.size();
	}
	
	public SqlJetOvflCell get(int i) {
		return ovfl.get(i);
	}

	@Override
	public Iterator<SqlJetOvflCell> iterator() {
		return ovfl.iterator();
	}
	
	@Override
	public SqlJetOvflCells clone() {
		return new SqlJetOvflCells(new ArrayList<>(ovfl));
	}
}
