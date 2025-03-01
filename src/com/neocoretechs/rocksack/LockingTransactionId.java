package com.neocoretechs.rocksack;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
/**
* Encapsulates a transaction id guaranteed to be unique across all usage via UUID.
* Typically represented in string form and generated via LockingTransactionId.generate() static factory method.<p>
* Also contains transactionLockTimeout. If positive, specifies the default wait timeout in milliseconds 
* when a transaction attempts to lock a key if not specified by TransactionOptions.setLockTimeout(long) 
* If 0, no waiting is done if a lock cannot instantly be acquired. 
* If negative, there is no timeout. Not using a timeout is not recommended 
* as it can lead to deadlocks. Currently, there is no deadlock-detection to recover from a deadlock.
* Default: 1000 <p>
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2025
*/
public class LockingTransactionId extends TransactionId implements Serializable {
	private static final long serialVersionUID = -2057406013741087366L;
	private long transactionLockTimeout = 1000L;
	public LockingTransactionId() {}

	public LockingTransactionId(String transactionId, long transactionLockTimeout) {
		super(transactionId);
		this.transactionLockTimeout = transactionLockTimeout;
	}

	public LockingTransactionId(byte[] transactionId, long transactionLockTimeout) {
		super(transactionId);
		this.transactionLockTimeout = transactionLockTimeout;
	}
	
	public void setLockTimeout(long timeout) {
		this.transactionLockTimeout = timeout;
	}
	
	public long getLockTimeout() {
		return this.transactionLockTimeout;
	}
	
	/**
	 * Factory method to generate a locking id with default timeout of 1000 millis.
	 * @return the new instance
	 */
	public static LockingTransactionId generate() {
		return new LockingTransactionId(UUID.randomUUID().toString(), 1000L);
	}
	/**
	 * Factory method to generate new locking id with given timeout in millis
	 * @param timeout the timeout in milliseconds
	 * @return the new instance
	 */
	public static LockingTransactionId generate(long timeout) {
		return new LockingTransactionId(UUID.randomUUID().toString(), timeout);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(transactionLockTimeout);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (!(obj instanceof LockingTransactionId)) {
			return false;
		}
		LockingTransactionId other = (LockingTransactionId) obj;
		return transactionLockTimeout == other.transactionLockTimeout;
	}

	@Override
	public String toString() {
		return super.toString() + " timeout="+transactionLockTimeout;
	}


}
