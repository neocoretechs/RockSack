package com.neocoretechs.rocksack;

import java.io.Serializable;

public class Alias implements Serializable {
	private static final long serialVersionUID = -4900917167930271807L;
	private String alias;
	public Alias() {}
	public Alias(String alias) {
		this.alias = alias;
	}
	public Alias(byte[] alias) {
		this.alias = new String(alias);
	}
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	public void setAlias(byte[] alias) {
		this.alias = new String(alias);
	}
}
