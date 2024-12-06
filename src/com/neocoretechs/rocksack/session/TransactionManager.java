package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;

import com.neocoretechs.rocksack.TransactionId;
import com.neocoretechs.rocksack.session.VolumeManager.Volume;

public class TransactionManager {
	private static ConcurrentHashMap<TransactionId, TransactionSession> idToSession = new ConcurrentHashMap<TransactionId,TransactionSession>();
	// Multithreaded double check Singleton setups:
	// 1.) privatized constructor; no other class can call
	private TransactionManager() {
	}
	// 2.) volatile instance
	private static volatile TransactionManager instance = null;
	private static boolean DEBUG;
	// 3.) lock class, assign instance if null
	public static TransactionManager getInstance() {
		synchronized(TransactionManager.class) {
			if(instance == null) {
				instance = new TransactionManager();
			}
		}
		return instance;
	}
	
	static Collection<TransactionSession> getTransactionSessions() {
		return idToSession.values();
	}
	
	static TransactionSession getTransactionSession(TransactionId xid) {
		return idToSession.get(xid);
	}
	
	static void setTransaction(TransactionId xid, TransactionSession ts) {
		idToSession.put(xid,  ts);
	}
	/**
	 * Return a list of the state of all transactions with id's mapped to transactions
	 * in the set of active volumes. According to docs states are:
	 * AWAITING_COMMIT AWAITING_PREPARE AWAITING_ROLLBACK COMMITED COMMITTED (?)
	 * LOCKS_STOLEN PREPARED ROLLEDBACK STARTED. In practice, it seems to mainly vary between
	 * STARTED and COMMITTED. The 'COMMITED' state doesnt seem to manifest.
	 * @return
	 */
	static List<String> getOutstandingTransactionState() {
		ArrayList<String> retState = new ArrayList<String>();
		for(Transaction t: getOutstandingTransactions()) {
			retState.add("Transaction:"+t.getName()+" State:"+ t.getState().name());
		}
		return retState;
	}
	/**
	 * Return a list all RocksDB transactions with id's mapped to transactions
	 * in the set of active volumes, regardless of state.
	 * @return raw RocksDB transactions, use with caution.
	 */
	static List<Transaction> getOutstandingTransactions() {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		for(Entry<TransactionId, TransactionSession> transSessions : idToSession.entrySet()) {
			// Get all the TransactionSessions
			for(Entry<String, Transaction> transMaps: transSessions.getValue().getTransactions()) {
				// Get all the transactions active for each TransactionSession
					//if(!transact.getState().equals(TransactionState.COMMITED) &&
					//	!transact.getState().equals(TransactionState.COMMITTED) &&
					//	!transact.getState().equals(TransactionState.ROLLEDBACK))
				if(!retXactn.contains(transMaps.getValue()))
					retXactn.add(transMaps.getValue());
			}
		}
		return retXactn;
	}
	/**
	 * Return a list all RocksDB transactions with id's mapped to transactions
	 * in the set of active volumes for a particular path regardless of state.
	 * @param path the path in question
	 * @return the List of RocksDB Transactions.
	 * @throws IOException 
	 */
	static List<Transaction> getOutstandingTransactions(String path) throws IOException {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		Volume v = VolumeManager.get(path);
		// Get all the TransactionalMaps for the volume
		for(TransactionSetInterface transMaps: v.classToIsoTransaction.values()) {
			// Get all the transactions active for each TransactionalMap
			Collection<Entry<String, Transaction>> transactions = ((TransactionalMap)transMaps).getSession().getTransactions();
			for(Entry<String, Transaction> transact: transactions) {
				//if(!transact.getState().equals(TransactionState.COMMITED) &&
				//	!transact.getState().equals(TransactionState.COMMITTED) &&
				//	!transact.getState().equals(TransactionState.ROLLEDBACK))
				if(!retXactn.contains(transact.getValue()))
					retXactn.add(transact.getValue());
			}
		}
		return retXactn;
	}
	/**
	 * Return a list all RocksDB transactions with id's mapped to transactions
	 * in the set of active volumes for a particular alias to a path regardless of state
	 * @param alias the path alias
	 * @return the List of RocksDB Transactions.
	 * @throws IOException 
	 */
	static List<Transaction> getOutstandingTransactionsAlias(String alias) throws IOException {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		Volume v = VolumeManager.getByAlias(alias);
		// Get all the TransactionalMaps for the volume
		// Get all the TransactionalMaps for the volume
		for(TransactionSetInterface transMaps: v.classToIsoTransaction.values()) {
			// Get all the transactions active for each TransactionalMap
			Collection<Entry<String, Transaction>> transactions = ((TransactionalMap)transMaps).getSession().getTransactions();
			for(Entry<String, Transaction> transact: transactions) {
				//if(!transact.getState().equals(TransactionState.COMMITED) &&
				//	!transact.getState().equals(TransactionState.COMMITTED) &&
				//	!transact.getState().equals(TransactionState.ROLLEDBACK))
				if(!retXactn.contains(transact.getValue()))
					retXactn.add(transact.getValue());
			}
		}
		return retXactn;
	}
	
	/**
	 * Get a list of transactions with the given id to verify whether it appears in multiple volumes.
	 * @param uid
	 * @return
	 */
	static List<Transaction> getOutstandingTransactionsById(String uid) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		for(Entry<TransactionId, TransactionSession> sessions : idToSession.entrySet()) {
			// Get all the TransactionalMaps for the session
			if(sessions.getKey().getTransactionId().equals(uid)) {
				for(Entry<String, Transaction> transMaps: sessions.getValue().getTransactions()) {
					// Get all the transactions active for each TransactionalMap
					//if(!transact.getState().equals(TransactionState.COMMITED) &&
					//	!transact.getState().equals(TransactionState.COMMITTED) &&
					//	!transact.getState().equals(TransactionState.ROLLEDBACK))
					if(!retXactn.contains(transMaps.getValue()))
						retXactn.add(transMaps.getValue());
				}
			}
		}
		return retXactn;
	}
	
	/**
	 * Get a list of transactions with the given id which may appear in multiple aliases and volumes
	 * possibly signaling a user discrepancy.
	 * @param alias
	 * @param uid
	 * @return
	 */
	static List<Transaction> getOutstandingTransactionsByAliasAndId(String alias, String uid) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		for(Entry<TransactionId, TransactionSession> sessions : idToSession.entrySet()) {
			// Get all the TransactionalMaps for the session
			if(sessions.getKey().getTransactionId().equals(uid)) {
				for(Entry<String, Transaction> transMaps: sessions.getValue().getTransactions(alias)) {
					// Get all the transactions active for each TransactionalMap
					//if(!transact.getState().equals(TransactionState.COMMITED) &&
					//	!transact.getState().equals(TransactionState.COMMITTED) &&
					//	!transact.getState().equals(TransactionState.ROLLEDBACK))
					if(!retXactn.contains(transMaps.getValue()))
						retXactn.add(transMaps.getValue());
				}
			}
		}
		return retXactn;
	}
	
	/**
	 * Get the list of outstanding transactions based on tablespace path and transaction id, regardless of state.
	 * @param path
	 * @param uid
	 * @return The list of Transaction
	 * @throws IOException 
	 */
	static List<Transaction> getOutstandingTransactionsByPathAndId(String path, String uid) throws IOException {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		Volume v = VolumeManager.get(path);
		if(DEBUG)
			System.out.println("VolumeManager.getOutstandingTransactionsByPathAndId for path:"+path+" id:"+uid+" got volume "+v);
		// Get all the TransactionalMaps for the volume
		for(TransactionSetInterface transMaps: v.classToIsoTransaction.values()) {
			// Get all the transactions active for each TransactionalMap
			Set<Entry<String, Transaction>> transactions = ((TransactionalMap)transMaps).getSession().getTransactions();
			for(Entry<String, Transaction> transact: transactions) {
				if(DEBUG)
					System.out.println("VolumeManager.getOutstandingTransactionsByPathAndId trying transaction:"+transact.getKey());
				//if(!transact.getState().equals(TransactionState.COMMITED) &&
				//	!transact.getState().equals(TransactionState.COMMITTED) &&
				//	!transact.getState().equals(TransactionState.ROLLEDBACK))
				if(transact.getKey().substring(0,36).equals(uid)) {
					if(DEBUG)
						System.out.println("VolumeManager.getOutstandingTransactionsByPathAndId adding:"+transact.getKey());
					if(!retXactn.contains(transact.getValue()))
						retXactn.add(transact.getValue());
				}
			}
		}
		if(DEBUG)
			System.out.println("VolumeManager.getOutstandingTransactionsByPathAndId returning:"+retXactn.size());
		return retXactn;
	}
	/**
	 * Clear all the outstanding transactions. Roll back all in-progress transactions,
	 * the clear the id to transaction table in the set of volumes.
	 * Needless to say, use with caution.
	 */
	static void clearAllOutstandingTransactions() {
		for(Transaction t: getOutstandingTransactions()) {
			try {
				t.rollback();
			} catch (RocksDBException e) {}
		}
	}
	/**
	 * Roll back all transactions associated with the given transaction uid
	 * @param uid
	 * @throws IOException 
	 */
	static void clearOutstandingTransaction(String uid) throws IOException {
		for(Transaction t: getOutstandingTransactions(uid)) {
			try {
				t.rollback();
				removeTransaction(uid);
			} catch (RocksDBException e) {}
		}
	}
	/**
	 * Remove the transaction from all volumes that had an idToTransaction table entry with this transaction id.
	 * @param uid The target transaction id
	 * @return The unique list of volumes that had an entry for this id. Typically only 1, or empty list.
	 * @throws IOException If transaction state was not STARTED, COMMITTED, or ROLLEDBACK
	 */
	static void /*List<Volume>*/ removeTransaction(String uid) throws IOException {
		//ArrayList<Volume> rv = new ArrayList<Volume>();
		TransactionId tid = new TransactionId(uid);
		TransactionSession tis = idToSession.get(tid);
		Set<Entry<String, Transaction>> s = tis.getTransactions();
		tis.removeTransactions(s);
		//return rv;
	}

}
