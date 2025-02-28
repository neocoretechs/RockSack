package com.neocoretechs.rocksack.session;


import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
/**
 * Basic interface to begin transaction
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2024
 *
 */
interface TransactionInterface {
	/**
	 * Initiate a transaction.
	 * @param transactionName the name of the transaction, in our context a UUID
	 * @return The RocksDB Transaction instance
	 * @throws RocksDBException
	 */
	Transaction BeginTransaction(String transactionName) throws RocksDBException;
	
	/**
	 * Initiate a transaction.
	 * @return The RocksDB transaction instance.
	 */
	Transaction BeginTransaction();	
}