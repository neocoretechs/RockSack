package com.neocoretechs.rocksack.session;


import org.rocksdb.Transaction;

interface TransactionInterface {

	Transaction BeginTransaction();
	
}