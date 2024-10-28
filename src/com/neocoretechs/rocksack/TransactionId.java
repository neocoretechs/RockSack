package com.neocoretechs.rocksack;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public class TransactionId implements Serializable {
	private static final long serialVersionUID = -4900917167930271807L;
	private String transactionId;
	public TransactionId() {}
	public TransactionId(String transactionId) {
		this.transactionId = transactionId;
	}
	public TransactionId(byte[] transactionId) {
		this.transactionId = new String(transactionId);
	}
	public String getTransactionId() {
		return transactionId;
	}
	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}
	public void setTransactionId(byte[] transactionId) {
		this.transactionId = new String(transactionId);
	}
	public static TransactionId generate() {
		return new TransactionId(UUID.randomUUID().toString());
	}
	@Override
	public int hashCode() {
		return Objects.hash(transactionId);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof TransactionId)) {
			return false;
		}
		TransactionId other = (TransactionId) obj;
		return Objects.equals(transactionId, other.transactionId);
	}
	@Override
	public String toString() {
		return transactionId;
	}
	
}
