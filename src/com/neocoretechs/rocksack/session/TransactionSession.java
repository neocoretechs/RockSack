package com.neocoretechs.rocksack.session;

import org.rocksdb.Options;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.WriteOptions;
/**
 * Extends the {@link Session} class to include transaction semantics. In RocksDb a TransactionDB
 * instance contains the transaction classes and methods to provide atomicity.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2023
 *
 */
public class TransactionSession extends Session implements TransactionInterface {
	private static boolean DEBUG = false;
	protected TransactionSession(TransactionDB kvStore, Options options) {
		super(kvStore, options);
	}
	
	@Override
	public TransactionDB getKVStore() {
		return (TransactionDB) kvStore;
	}
		
	@Override
	public Transaction BeginTransaction() {
		return getKVStore().beginTransaction(new WriteOptions());
	}
	
	
	@Override
	public String toString() {
		return super.toString();
	}

}
