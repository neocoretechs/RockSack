package com.neocoretechs.rocksack.session;

import java.io.IOException;

import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.WriteOptions;

public class RockSackTransactionSession extends RockSackSession implements TransactionInterface {
	
	protected RockSackTransactionSession(TransactionDB kvStore, Options options, int uid, int gid) {
		super(kvStore, options, uid, gid);
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
	/**
	* @exception IOException for low level failure
	*/
	public void Rollback(Transaction txn) throws IOException {
		try {
			txn.rollback();
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}
	@Override
	public void Commit(Transaction txn) throws IOException {
		try {
			txn.commit();
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}
	/**
	 * Checkpoint the current transaction
	 * @return 
	 * @throws IOException 
	 * @throws IllegalAccessException 
	 */
	public Snapshot Checkpoint(Transaction txn) throws IllegalAccessException, IOException {
		return getKVStore().getSnapshot();
	}
	/**
	* Close this transaction.
	* @param txn Transaction
	* @param rollback true to roll back, false to commit
	* @exception IOException For low level failure
	*/
	public void Close(Transaction txn, boolean rollback) throws IOException {
		rollupSession(txn, rollback);
	}
	
	/**
	* Generic session roll up.  Data is committed based on rollback param.
	* We deallocate the outstanding block
	* We iterate the tablespaces for each db removing obsolete log files.
	* Remove the WORKER threads from KeyValueMain, then remove this session from the SessionManager
	* @param rollback true to roll back, false to commit
	* @exception IOException For low level failure
	*/
	public void rollupSession(Transaction txn, boolean rollback) throws IOException {
		if (rollback) {
			Rollback(txn);
		} else {
			Commit(txn);
		}
		txn.close();
	}
	
	@Override
	public String toString() {
		return super.toString();
	}

}
