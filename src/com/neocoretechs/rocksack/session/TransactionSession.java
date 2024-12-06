package com.neocoretechs.rocksack.session;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.WriteOptions;

import com.neocoretechs.rocksack.Alias;
import com.neocoretechs.rocksack.TransactionId;
/**
 * Extends the {@link Session} class to include transaction semantics. In RocksDb a TransactionDB
 * instance contains the transaction classes and methods to provide atomicity.
 * Transactions are linked to a TransactionDb, a subclass of RocksDB. Each transaction may be named,
 * and the name must be unique. To enforce uniqueness considering these constraints, the name
 * formed will be a concatenation of Transaction Id, which is a UUID, the class name, which is also
 * a column family or the default column family, and the Alias, or none, which is the default database path.  
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2023,2024
 *
 */
public class TransactionSession extends Session implements TransactionInterface {
	private static boolean DEBUG = false;
	ReadOptions ro;
	private WriteOptions wo;
	private static ConcurrentHashMap<String, Transaction> nameToTransaction = new ConcurrentHashMap<String,Transaction>();
	
	protected TransactionSession(TransactionDB kvStore, Options options, ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptor, List<ColumnFamilyHandle> columnFamilyHandles) {
		super(kvStore, options, columnFamilyDescriptor, columnFamilyHandles);
		ro = new ReadOptions();
		wo = new WriteOptions();
	}
	
	@Override
	public TransactionDB getKVStore() {
		return (TransactionDB) kvStore;
	}
	
	@Override
	public Transaction BeginTransaction() {
		return getKVStore().beginTransaction(new WriteOptions());
	}
	/**
	 * Get the Transaction object formed from id and class. If the transaction
	 * id is in use for another class in this, the default db, use the existing transaction.
	 * @param transactionId
	 * @param clazz
	 * @param create true to create if not existing
	 * @return The RocksDb Transaction object or null if not found and create was false
	 */
	public Transaction getTransaction(TransactionId transactionId, String clazz, boolean create) {
		try {
			String name = transactionId.getTransactionId()+clazz;
			Transaction transaction = null;
			boolean exists = false;
			// check exact match
			if((transaction = nameToTransaction.get(name)) == null) {
				// no match, is id in use for another class? if so, use that transaction
				Set<Entry<String, Transaction>> all = getTransactions();
				for(Entry<String, Transaction> alle : all) {
					if(alle.getKey().startsWith(transactionId.getTransactionId())) {
						transaction = alle.getValue();
						exists = true;
						break;
					}
				}
			}
			if(!exists && create) {
				transaction = getKVStore().beginTransaction(wo);
				transaction.setName(name);
				nameToTransaction.put(name, transaction);
			}
			return transaction;
		} catch (RocksDBException e) {
				throw new RuntimeException(e);
		}
	}
	/**
	 * Get the Transaction object formed from id and class and alias. If the transaction
	 * id is in use for another class in the alias db, use the existing transaction.
	 * We are checking that the transaction name 'startswith' the id and 'endswith' the alias.
	 * @param alias
	 * @param transactionId
	 * @param clazz
	 * @param create
	 * @return The RocksDb Transaction object or null if not found and create was false
	 */
	public Transaction getTransaction(Alias alias, TransactionId transactionId, String clazz, boolean create) {
		try {
			String name = transactionId.getTransactionId()+clazz+alias.getAlias();
			Transaction transaction = null;
			boolean exists = false;
			// check exact match
			if((transaction = nameToTransaction.get(name)) == null) {
				// no match, is id in use for another class? if so, use that transaction
				Set<Entry<String, Transaction>> all = getTransactions();
				for(Entry<String, Transaction> alle : all) {
					if(alle.getKey().startsWith(transactionId.getTransactionId()) &&
					   alle.getKey().endsWith(alias.getAlias())) {
						transaction = alle.getValue();
						exists = true;
						break;
					}
				}
			}
			if(!exists && create) {
				transaction = getKVStore().beginTransaction(wo);
				transaction.setName(name);
				nameToTransaction.put(name, transaction);
			}
			return transaction;
		} catch (RocksDBException e) {
				throw new RuntimeException(e);
		}
	}
	
	public Set<Entry<String, Transaction>> getTransactions() {
		return nameToTransaction.entrySet();
	}
	
	/**
	 * Get the Transaction objects formed from id. 
	 * @param transactionId
	 * @return Set of names and RocksDb Transaction objects or null if none found
	 */
	public Set<Entry<String, Transaction>> getTransactions(TransactionId transactionId) {
		Set<Entry<String, Transaction>> all = getTransactions();
		ConcurrentHashMap<String, Transaction> names = new ConcurrentHashMap<String,Transaction>();
		for(Entry<String, Transaction> alle : all) {
			if(alle.getKey().startsWith(transactionId.getTransactionId())) {
				names.put(alle.getKey(), alle.getValue());
			}
		}
		return names.entrySet();
	}
	/**
	 * Get the Transaction objects formed from id and class name. 
	 * @param transactionId
	 * @param clazz
	 * @return Set of names and RocksDb Transaction objects or null if none found
	 */
	public Set<Entry<String, Transaction>> getTransactions(TransactionId transactionId, String clazz) {
		Set<Entry<String, Transaction>> all = getTransactions();
		ConcurrentHashMap<String, Transaction> names = new ConcurrentHashMap<String,Transaction>();
		for(Entry<String, Transaction> alle : all) {
			if(alle.getKey().startsWith(transactionId.getTransactionId()+clazz)) {
				names.put(alle.getKey(), alle.getValue());
			}
		}
		return names.entrySet();
	}
	
	/**
	 * Get the Transaction objects formed from id and alias name. 
	 * @param transactionId
	 * @param clazz
	 * @return Set of names and RocksDb Transaction objects or null if none found
	 */
	public Set<Entry<String, Transaction>> getTransactions(TransactionId transactionId, Alias alias) {
		Set<Entry<String, Transaction>> all = getTransactions();
		ConcurrentHashMap<String, Transaction> names = new ConcurrentHashMap<String,Transaction>();
		for(Entry<String, Transaction> alle : all) {
			if(alle.getKey().startsWith(transactionId.getTransactionId()) &&
				alle.getKey().endsWith(alias.getAlias())) {
					names.put(alle.getKey(), alle.getValue());
			}
		}
		return names.entrySet();
	}
	
	/**
	 * Get the Transaction objects containing alias name. 
	 * @param alias
	 * @return Set of names and RocksDb Transaction objects or null if none found
	 */
	public Set<Entry<String, Transaction>> getTransactions(Alias alias) {
		Set<Entry<String, Transaction>> all = getTransactions();
		ConcurrentHashMap<String, Transaction> names = new ConcurrentHashMap<String,Transaction>();
		for(Entry<String, Transaction> alle : all) {
			if(alle.getKey().endsWith(alias.getAlias())) {
				names.put(alle.getKey(), alle.getValue());
			}
		}
		return names.entrySet();
	}
	
	/**
	 * Get the Transaction objects formed from class name. 
	 * @param clazz
	 * @return Set of names and RocksDb Transaction objects or null if none found
	 */
	public Set<Entry<String, Transaction>> getTransactions(String clazz) {
		Set<Entry<String, Transaction>> all = getTransactions();
		ConcurrentHashMap<String, Transaction> names = new ConcurrentHashMap<String,Transaction>();
		for(Entry<String, Transaction> alle : all) {
			if(alle.getKey().substring(36).startsWith(clazz)) {
				names.put(alle.getKey(), alle.getValue());
			}
		}
		return names.entrySet();
	}
	
	/**
	 * Get the Transaction objects formed from class name and alias. 
	 * @param clazz
	 * @param alias
	 * @return Set of names and RocksDb Transaction objects or null if none found
	 */
	public Set<Entry<String, Transaction>> getTransactions(String clazz, Alias alias) {
		Set<Entry<String, Transaction>> all = getTransactions();
		ConcurrentHashMap<String, Transaction> names = new ConcurrentHashMap<String,Transaction>();
		for(Entry<String, Transaction> alle : all) {
			if(alle.getKey().substring(36).startsWith(clazz) &&
				alle.getKey().substring(36).endsWith(alias.getAlias())	) {
				names.put(alle.getKey(), alle.getValue());
			}
		}
		return names.entrySet();
	}
	
	public Transaction getTransaction(String transactionName) {
		return nameToTransaction.get(transactionName);
	}
	
	public Transaction removeTransaction(String transactionName) {
		return nameToTransaction.remove(transactionName);
	}
	
	public void removeTransactions(Set<Entry<String, Transaction>> transactions) {
		transactions.forEach(e->{
			if(DEBUG)
				System.out.println("Removing transaction "+e.getKey());
			removeTransaction(e.getKey());
		});
	}
	
	public void commit(String transactionName) throws RocksDBException {
		getTransaction(transactionName).commit();
	}
	
	public void commit(Set<Entry<String, Transaction>> transactions) {
		transactions.forEach(e->{
			if(DEBUG)
				System.out.println("Committing transaction "+e.getKey());
			try {
				e.getValue().commit();
			} catch (RocksDBException e1) {
				throw new RuntimeException(e1);
			}
		});
	}
	
	public void rollback(String transactionName) throws RocksDBException {
		getTransaction(transactionName).rollback();
	}
	
	public void rollback(Set<Entry<String, Transaction>> transactions) {
		transactions.forEach(e->{
			if(DEBUG)
				System.out.println("Rollback transaction "+e.getKey());
			try {
				e.getValue().rollback();
			} catch (RocksDBException e1) {
				throw new RuntimeException(e1);
			}
		});
	}
	
	public void checkpoint(String transactionName) throws RocksDBException {
		getTransaction(transactionName).setSavePoint();
	}
	
	public void checkpoint(Set<Entry<String, Transaction>> transactions) {
		transactions.forEach(e->{
			if(DEBUG)
				System.out.println("Checkpoint transaction "+e.getKey());
			try {
				e.getValue().setSavePoint();
			} catch (RocksDBException e1) {
				throw new RuntimeException(e1);
			}
		});
	}
	
	public void rollbackToCheckpoint(String transactionName) throws RocksDBException {
		getTransaction(transactionName).rollbackToSavePoint();
	}
	
	public void rollbackToCheckpoint(Set<Entry<String, Transaction>> transactions) {
		transactions.forEach(e->{
			if(DEBUG)
				System.out.println("RollbackToCheckpoint transaction "+e.getKey());
			try {
				e.getValue().rollbackToSavePoint();
			} catch (RocksDBException e1) {
				throw new RuntimeException(e1);
			}
		});
	}
	
	@Override
	public String toString() {
		return super.toString();
	}

}
