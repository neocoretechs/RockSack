package com.neocoretechs.rocksack.session;


import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;

interface TransactionInterface {

	Transaction BeginTransaction(String transactionName) throws RocksDBException;
	Transaction BeginTransaction();
	
}