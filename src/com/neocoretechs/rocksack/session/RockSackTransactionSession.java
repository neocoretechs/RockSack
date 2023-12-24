package com.neocoretechs.rocksack.session;

import java.io.IOException;

import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.WriteOptions;

public class RockSackTransactionSession extends RockSackSession implements TransactionInterface {
	private static boolean DEBUG = true;
	protected RockSackTransactionSession(TransactionDB kvStore, Options options) {
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
