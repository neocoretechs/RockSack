package com.neocoretechs.rocksack.session;

import org.rocksdb.Transaction;
/**
 * Basic interface to begin transaction
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2024
 *
 */
interface TransactionInterface {
	
	/**
	 * Initiate a transaction.
	 * @return The RocksDB transaction instance.
	 */
	Transaction BeginTransaction();	
}