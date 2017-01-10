package org.tmatesoft.sqljet.core.internal.btree;

public class SqlJetIndexedMemPage {
	private final SqlJetMemPage page;
	private int index;
	
	public SqlJetIndexedMemPage(SqlJetMemPage page) {
		this.page = page;
		this.index = 0;
	}
	
	public int getIndex() {
		return index;
	}
	
	public int incrIndex() {
		return ++index;
	}
	
	public int decrIndex() {
		return --index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	public SqlJetMemPage getPage() {
		return page;
	}
}