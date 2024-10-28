package com.neocoretechs.rocksack;

import java.io.Serializable;
import java.util.Objects;

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
	@Override
	public String toString() {
		return alias;
	}
	@Override
	public int hashCode() {
		return Objects.hash(alias);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof Alias)) {
			return false;
		}
		Alias other = (Alias) obj;
		return Objects.equals(alias, other.alias);
	}

}
