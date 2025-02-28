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
/**
 * Extends the {@link TransactionSessionAlias} class to include Alias for optimistic transactions. In RocksDb a TransactionDB
 * instance contains the transaction classes and methods to provide atomicity.
 * Transactions are linked to an OptimisticTransactionDb, a subclass of RocksDB. Each transaction may be named,
 * and the name must be unique. To enforce uniqueness considering these constraints, the name
 * formed will be a concatenation of Transaction Id, which is a UUID, the class name, which is also
 * a column family or the default column family, and the Alias, or none, which is the default database path.<p>
 * From the {@link TransactionManager} we link the transaction Id's to an instance of this, and associated transaction.
 * This class handles the aliased instances of OptimisticTransactionSessions.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2023,2024
 *
 */
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
