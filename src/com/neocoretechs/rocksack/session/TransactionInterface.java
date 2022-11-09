package com.neocoretechs.rocksack.session;

import java.io.IOException;

import org.rocksdb.Transaction;

interface TransactionInterface {

	boolean COMMIT = false;
	boolean ROLLBACK = true;
	
	Transaction BeginTransaction();
	/**
	* Close this session.
	* @param rollback true to roll back, false to commit
	* @exception IOException For low level failure
	*/
	void Close(Transaction txn, boolean rollback) throws IOException;

	/**
	* @exception IOException for low level failure
	*/
	void Rollback(Transaction txn) throws IOException;
	
	/**
	* Commit the blocks.
	* @exception IOException For low level failure
	*/
	void Commit(Transaction txn) throws IOException;
	/**
	 * Checkpoint the current transaction
	 * @throws IOException 
	 * @throws IllegalAccessException 
	 */
	void Checkpoint(Transaction txn) throws IllegalAccessException, IOException;
	/**
	* Generic session roll up.  Data is committed based on rollback param.
	* We deallocate the outstanding block
	* We iterate the tablespaces for each db removing obsolete log files.
	* Remove the WORKER threads from KeyValueMain, then remove this session from the SessionManager
	* @param rollback true to roll back, false to commit
	* @exception IOException For low level failure
	*/
	void rollupSession(Transaction txn, boolean rollback) throws IOException;
	
}