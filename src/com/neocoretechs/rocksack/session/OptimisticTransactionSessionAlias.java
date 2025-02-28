package com.neocoretechs.rocksack.session;

import java.util.ArrayList;
import java.util.List;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.Options;
//import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import com.neocoretechs.rocksack.Alias;
import com.neocoretechs.rocksack.SerializedComparator;

public class OptimisticTransactionSessionAlias extends TransactionSessionAlias {
	private static boolean DEBUG = false;
	OptimisticTransactionOptions optoOpts;

	public OptimisticTransactionSessionAlias(OptimisticTransactionDB kvStore, Options options, ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptor, List<ColumnFamilyHandle> columnFamilyHandles, Alias alias) {
		super(kvStore, options, columnFamilyDescriptor, columnFamilyHandles, alias);
	}

	//@Override
	//public synchronized RocksDB getKVStore() {
	//	return ((OptimisticTransactionDB)kvStore).getBaseDB();
	//}
	
	@Override
	public synchronized Transaction BeginTransaction(String transactionName) throws RocksDBException {
		optoOpts = new OptimisticTransactionOptions();
		optoOpts.setComparator(new SerializedComparator());
		optoOpts.setSetSnapshot(true);
		Transaction t = ((OptimisticTransactionDB) getKVStore()).beginTransaction(new WriteOptions(), optoOpts);
		if(DEBUG)
			System.out.printf("%s.BeginTransaction transaction name %s%n",this.getClass().getName(),transactionName);
		t.setName(transactionName);
		return t;
	}
	
	@Override
	public synchronized Transaction BeginTransaction() {
		if(DEBUG)
			System.out.printf("%s.BeginTransaction transaction name undefined%n",this.getClass().getName());
		optoOpts = new OptimisticTransactionOptions();
		optoOpts.setComparator(new SerializedComparator());
		optoOpts.setSetSnapshot(true);
		return ((OptimisticTransactionDB) getKVStore()).beginTransaction(new WriteOptions(), optoOpts);
	}
}
