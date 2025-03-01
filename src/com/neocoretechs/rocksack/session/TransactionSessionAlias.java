package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.WriteOptions;

import com.neocoretechs.rocksack.Alias;
import com.neocoretechs.rocksack.LockingTransactionId;
import com.neocoretechs.rocksack.TransactionId;
import com.neocoretechs.rocksack.session.TransactionManager.SessionAndTransaction;
/**
 * Extends the {@link TransactionSession} class to include Alias. In RocksDb a TransactionDB
 * instance contains the transaction classes and methods to provide atomicity.
 * Transactions are linked to a TransactionDb, a subclass of RocksDB. Each transaction may be named,
 * and the name must be unique. To enforce uniqueness considering these constraints, the name
 * formed will be a concatenation of Transaction Id, which is a UUID, the class name, which is also
 * a column family or the default column family, and the Alias, or none, which is the default database path.<p>
 * From the {@link TransactionManager} we link the transaction Id's to an instance of this, and associated transaction.
 * This class handles the aliased instances of TransactionSessions.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2023,2024
 *
 */
public class TransactionSessionAlias extends TransactionSession {
	private static boolean DEBUG = false;
	private Alias alias;
	/**
	 * 
	 * @param kvStore the RocksDb TransactionDB encapsulated by this instance
	 * @param options the options for opening the transaction database
	 * @param columnFamilyDescriptor the list of column family descriptors for the class instances stored in this session and database
	 * @param columnFamilyHandles the list of column family handles for the class instances stored associated with the the column family descriptors
	 * @param alias the alias associated with this database
	 */
	protected TransactionSessionAlias(TransactionDB kvStore, Options options, ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptor, List<ColumnFamilyHandle> columnFamilyHandles, Alias alias) {
		super(kvStore, options, columnFamilyDescriptor, columnFamilyHandles);
		this.alias = alias;
		ro = new ReadOptions();
		wo = new WriteOptions();
	}
	
	public TransactionSessionAlias(OptimisticTransactionDB kvStore, Options options, ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptor, List<ColumnFamilyHandle> columnFamilyHandles, Alias alias) {
		super(kvStore, options, columnFamilyDescriptor, columnFamilyHandles);
		this.alias = alias;
		ro = new ReadOptions();
		wo = new WriteOptions();
	}

	/**
	 * @return the {@link Alias} associated with this transaction and session.
	 */
	public synchronized Alias getAlias() {
		return alias;
	}
	
	/**
	 * Called from associateSession and setTransaction to link a new mangled name to a new SessionAndTransaction instance
	 * to populate the passed tLink map. A new transaction is initiated as the map of {@link SessionAndTransaction} is populated.
	 * @param xid the transaction Id
	 * @param tm the {@link TransactionalMap} instance containing the {@link Session} to link
	 * @param tLink the map of mangled name to {@link SessionAndTransaction} instance populated by the method
	 * @return true if the tLink instance already contained the linked mangled name, false if a new name is created and a new transaction is initiated.
	 * @throws IOException if database exception occurs beginning the transaction.
	 * 
	 */
	@Override
	public synchronized boolean linkSessionAndTransaction(TransactionId xid, TransactionalMap tm, ConcurrentHashMap<String, SessionAndTransaction> tLink) throws IOException {
		if(DEBUG)
			System.out.printf("%s.linkSessionAndTransaction %s%n",this.getClass().getName(), tm);
		String name = xid.getTransactionId()+tm.getClassName()+alias.getAlias();
		if(tLink.containsKey(name))
			return true;
		SessionAndTransaction sLink;
		if(xid instanceof LockingTransactionId)
			sLink = new SessionAndTransaction(tm.getSession(), BeginTransaction(((LockingTransactionId)xid).getLockTimeout()), xid);
		else
			sLink = new SessionAndTransaction(tm.getSession(), BeginTransaction(), xid);
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
	@Override
	public synchronized boolean isTransactionLinked(TransactionId xid, TransactionalMap tm, ConcurrentHashMap<String, SessionAndTransaction> tLink) throws IOException {
		if(DEBUG)
			System.out.printf("%s.isTransactionLinked %s %s%n",this.getClass().getName(), tm, tLink);
		if(tLink == null)
			return false;
		String name = xid.getTransactionId()+tm.getClassName()+alias.getAlias();
		if(DEBUG)
			System.out.printf("%s.isTransactionLinked %s returning %b%n",this.getClass().getName(), name, tLink.containsKey(name));
		if(tLink.containsKey(name))
			return true;
		return false;
	}
	/**
	 * Get the Transaction object formed from id and class. If the transaction
	 * id is in use for another class in this, the aliased db, use the existing transaction.
	 * We assume can associate with another transaction in the same DB if necessary since
	 * subclasses may need to be automagically accounted for, but we cant physically span databases, though logically we can
	 * simulate it.
	 * @param transactionId
	 * @param clazz
	 * @param create true to create if not existing
	 * @return The RocksDb Transaction object or null if not found and create was false
	 */
	@Override
	public synchronized Transaction getTransaction(TransactionId transactionId, String clazz, boolean create) {
		String name = transactionId.getTransactionId()+clazz+alias.getAlias();
		if(DEBUG)
			System.out.printf("%s.getTransaction Enter Alias:%s Transaction id:%s Class:%s create:%b from name:%s%n",this.getClass().getName(),alias,transactionId,clazz,create,name);
		Transaction transaction = null;
		boolean exists = false;
		SessionAndTransaction transLink = null;
		// check exact match
		ConcurrentHashMap<String, SessionAndTransaction> transSession = TransactionManager.getTransactionSession(transactionId);
		if(transSession == null) {
			if(DEBUG)
				System.out.printf("%s.getTransaction transSession null Alias:%s Transaction id:%s Class:%s create:%b from name:%s%n",this.getClass().getName(),alias,transactionId,clazz,create,name);
			transSession = TransactionManager.setTransaction(transactionId);
		} else {
			transLink = transSession.get(name);
			if(transLink == null) {
				if(DEBUG)
					System.out.printf("%s.getTransaction transLink null Alias:%s Transaction id:%s Class:%s create:%b from name:%s%n",this.getClass().getName(),alias,transactionId,clazz,create,name);
				// no match, is id in use for another class? if so, use that transaction
				// as long as its in the same alias. RocksDb transactions cant span databases
				// but conceptually and virtually we can.
				Collection<SessionAndTransaction> all = transSession.values();
				if(all != null && !all.isEmpty()) {
					if(DEBUG)
						System.out.printf("%s.getTransaction transSession collection null or empty Alias:%s Transaction id:%s Class:%s create:%b from name:%s%n",this.getClass().getName(),alias,transactionId,clazz,create,name);
					for(SessionAndTransaction alle : all) {
						if(alle.getTransactionId().getTransactionId().startsWith(transactionId.getTransactionId())) {
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
			if(transactionId instanceof LockingTransactionId)
				transaction = BeginTransaction(((LockingTransactionId)transactionId).getLockTimeout());
			else
				transaction = BeginTransaction();
			transLink = new SessionAndTransaction(this, transaction, transactionId);
			transSession.put(name, transLink);
		}
		if(DEBUG)
			System.out.printf("%s.getTransaction returning Transaction name:%s%n",this.getClass().getName(),transaction.getName());
		return transaction;
	}

	@Override
	public String toString() {
		return "Alias:"+alias+" "+super.toString();
	}

}
