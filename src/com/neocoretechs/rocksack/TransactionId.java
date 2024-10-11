package com.neocoretechs.rocksack;

import java.io.Serializable;
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
}
