package com.neocoretechs.rocksack.session;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.TransactionDB;

public class RockSackTransactionSession extends RockSackSession implements TransactionInterface {

	protected RockSackTransactionSession(TransactionDB kvStore, Options options, int uid, int gid) {
		super(kvStore, options, uid, gid);
	}
	
	@Override
	public TransactionDB getKVStore() {
		return (TransactionDB) kvStore;
	}
	
	@Override
	public void BeginTransaction() {
		
	}

}
