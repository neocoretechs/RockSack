package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDB.KeyLockInfo;
import org.rocksdb.WriteOptions;

import com.neocoretechs.rocksack.TransactionId;
import com.neocoretechs.rocksack.session.TransactionManager.SessionAndTransaction;
/**
 * Extends the {@link Session} class to include transaction semantics. In RocksDb a TransactionDB
 * instance contains the transaction classes and methods to provide atomicity.
 * Transactions are linked to a TransactionDb, a subclass of RocksDB. Each transaction may be named,
 * and the name must be unique. To enforce uniqueness considering these constraints, the name
 * formed will be a concatenation of Transaction Id, which is a UUID, the class name, which is also
 * a column family or the default column family, and the Alias, or none, which is the default database path.<p>
 * From the {@link TransactionManager} we link the transaction Id's to an instance of this and associated transaction.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2023,2024
 */
public class TransactionSession extends Session implements TransactionInterface {
	private static boolean DEBUG = false;
	ReadOptions ro;
	WriteOptions wo;

	protected TransactionSession(TransactionDB kvStore, Options options, ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptor, List<ColumnFamilyHandle> columnFamilyHandles) {
		super(kvStore, options, columnFamilyDescriptor, columnFamilyHandles);
		ro = new ReadOptions();
		wo = new WriteOptions();
	}
	
	public TransactionSession(OptimisticTransactionDB kvStore, Options options, ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptor, List<ColumnFamilyHandle> columnFamilyHandles) {
		super(kvStore, options, columnFamilyDescriptor, columnFamilyHandles);
		ro = new ReadOptions();
		wo = new WriteOptions();
	}

	@Override
	public synchronized Transaction BeginTransaction(String transactionName) throws RocksDBException {
		Transaction t = ((TransactionDB) getKVStore()).beginTransaction(new WriteOptions());
		if(DEBUG)
			System.out.printf("%s.BeginTransaction transaction name %s%n",this.getClass().getName(),transactionName);
		t.setName(transactionName);
		return t;
	}
	
	@Override
	public synchronized Transaction BeginTransaction() {
		if(DEBUG)
			System.out.printf("%s.BeginTransaction transaction name undefined%n",this.getClass().getName());
		return ((TransactionDB) getKVStore()).beginTransaction(new WriteOptions());
	}
	/**
	 * Get the Transaction object formed from id and class. If the transaction
	 * id is in use for another class in this, the default db, use the existing transaction.
	 * @param transactionId
	 * @param clazz
	 * @param create true to create if not existing
	 * @return The RocksDb Transaction object or null if not found and create was false
	 */
	public synchronized Transaction getTransaction(TransactionId transactionId, String clazz, boolean create) {
		try {
			String name = transactionId.getTransactionId()+clazz;
			if(DEBUG)
				System.out.printf("%s.getTransaction Enter Transaction id:%s Class:%s create:%b from name:%s%n",this.getClass().getName(),transactionId,clazz,create,name);
			Transaction transaction = null;
			boolean exists = false;
			SessionAndTransaction transLink = null;
			// check exact match
			ConcurrentHashMap<String, SessionAndTransaction> transSession = TransactionManager.getTransactionSession(transactionId);
			if(transSession == null) {
				transSession = TransactionManager.setTransaction(transactionId);
			} else {
				transLink = transSession.get(name);
				if(transLink == null) {
					// no match, is id in use for another class? if so, use that transaction
					Collection<SessionAndTransaction> all = transSession.values();
					if(all != null && !all.isEmpty()) {
						for(SessionAndTransaction alle : all) {
							//if(DEBUG)
							//	System.out.printf("%s.getTransaction Transaction id:%s Transaction name:%s%n",this.getClass().getName(),alle.getKey(),alle.getValue().getName());
							if(alle.getTransaction().getName().startsWith(transactionId.getTransactionId())) {
								transaction = alle.getTransaction();
								exists = true;
								break;
							}
						}
					}
				} else {
					transaction = transLink.getTransaction();
					exists = true;
				}
			}
			if(!exists && create) {
				if(DEBUG)
					System.out.printf("%s.getTransaction Creating Transaction id:%s Transaction name:%s%n",this.getClass().getName(),transactionId,name);
				transaction = BeginTransaction(name);
				transLink = new SessionAndTransaction(this, transaction);
				transSession.put(name, transLink);
			}
			if(DEBUG)
				System.out.printf("%s.getTransaction returning Transaction name:%s%n",this.getClass().getName(),transaction.getName());
			return transaction;
		} catch (RocksDBException e) {
				throw new RuntimeException(e);
		}
	}

	/**
	 * Generate a mangled name identifier of transaction id, classname, and optionally the alias,
	 * to identify this entry in the mapping of mangled name to SessionAndTransaction
	 * instances. Create a new SessionAndTransaction instance, populate it with session from TransactionalMap
	 * and a new transaction, then insert them into the passed map.
	 * @param xid the TransactionId
	 * @param tm the TransactionalMap we want to use for classname and sesssion
	 * @param tLink The map entry from TransactionManager idToNameToSessionAndTransaction from key xid
	 * @return true if passed map contains mangled name
	 * @throws IOException
	 */
	public synchronized boolean linkSessionAndTransaction(TransactionId xid, TransactionalMap tm, ConcurrentHashMap<String, SessionAndTransaction> tLink) throws IOException {
		if(DEBUG)
			System.out.printf("%s.linkSessionAndTransaction %s%n",this.getClass().getName(), tm);
		String name = xid.getTransactionId()+tm.getClassName();
		if(tLink.containsKey(name))
			return true;
		SessionAndTransaction sLink;
		try {
			sLink = new SessionAndTransaction(tm.getSession(), BeginTransaction(name));
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
		tLink.put(name, sLink);
		return false;
	}

	/**
	 * Check the mangled name identifier of transaction id, classname, and optionally the alias,
	 * to identify this entry in the mapping of mangled name to SessionAndTransaction
	 * instances from the passed map.
	 * @param xid the TransactionId
	 * @param tm the TransactionalMap we want to use for classname and sesssion
	 * @param tLink The map entry from TransactionManager idToNameToSessionAndTransaction from key xid
	 * @return true if passed map contains mangled name
	 * @throws IOException
	 */
	public synchronized boolean isTransactionLinked(TransactionId xid, TransactionalMap tm, ConcurrentHashMap<String, SessionAndTransaction> tLink) throws IOException {
		if(DEBUG)
			System.out.printf("%s.isTransactionLinked %s %s%n",this.getClass().getName(), tm, tLink);
		if(tLink == null)
			return false;
		String name = xid.getTransactionId()+tm.getClassName();
		if(DEBUG)
			System.out.printf("%s.isTransactionLinked %s returning %b%n",this.getClass().getName(), name, tLink.containsKey(name));
		if(tLink.containsKey(name))
			return true;
		return false;
	}
	
	/**
	 * Get the map of lock status from TransactionDb
	 * @return
	 */
	public Map<Long, KeyLockInfo> getLockStatusData() {
		return ((TransactionDB) getKVStore()).getLockStatusData();
	}
	
	/**
	 * Get all prepared transactions
	 * @return
	 */
	public List<Transaction> getAllPreparedTransactions() {
		return ((TransactionDB) getKVStore()).getAllPreparedTransactions();
	}
	
	@Override
	public String toString() {
		return super.toString();
	}

}
