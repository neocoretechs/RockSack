package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;

import com.neocoretechs.rocksack.Alias;
import com.neocoretechs.rocksack.TransactionId;
import com.neocoretechs.rocksack.session.VolumeManager.Volume;
/**
 * Singleton transaction manager which maintains the map of {@link TransactionId}s to
 * {@link TransactionSession}s. We can access and manipulate this map in order to get us to the eventual
 * map of mangled transaction names and RocksDb TransactionDb Transaction instances in TransactionSession.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2024
 *
 */
public final class TransactionManager {
	private static boolean DEBUG = false;
	private static ConcurrentHashMap<TransactionId, ConcurrentHashMap<String, SessionAndTransaction>> idToNameToSessionAndTransaction = new ConcurrentHashMap<TransactionId,ConcurrentHashMap<String, SessionAndTransaction>>();
	// Multithreaded double check Singleton setups:
	// 1.) privatized constructor; no other class can call
	private TransactionManager() {
	}
	// 2.) volatile instance
	private static volatile TransactionManager instance = null;
	// 3.) lock class, assign instance if null
	public static TransactionManager getInstance() {
		synchronized(TransactionManager.class) {
			if(instance == null) {
				instance = new TransactionManager();
			}
		}
		return instance;
	}
	/**
	 * Class to hold instances that link session and transaction to mangled transaction name
	 *
	 */
	static class SessionAndTransaction {
		TransactionSession transactionSession;
		Transaction transaction;
		public SessionAndTransaction(TransactionSession session, Transaction transaction) {
			this.transactionSession = session;
			this.transaction = transaction;
		}
		/**
		 * @return the transactionSession
		 */
		public TransactionSession getTransactionSession() {
			return transactionSession;
		}
		/**
		 * @param transactionSession the transactionSession to set
		 */
		public void setTransactionSession(TransactionSession transactionSession) {
			this.transactionSession = transactionSession;
		}
		/**
		 * @return the transaction
		 */
		public Transaction getTransaction() {
			return transaction;
		}
		/**
		 * @param transaction the transaction to set
		 */
		public void setTransaction(Transaction transaction) {
			this.transaction = transaction;
		}
		@Override
		public int hashCode() {
			return Objects.hash(transaction, transactionSession);
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (!(obj instanceof SessionAndTransaction)) {
				return false;
			}
			SessionAndTransaction other = (SessionAndTransaction) obj;
			return Objects.equals(transaction, other.transaction)
					&& Objects.equals(transactionSession, other.transactionSession);
		}
		@Override
		public String toString() {
			return "SessionAndTransaction [transactionSession=" + transactionSession + ", transaction=" + transaction+ "]";
		}

	}
	
	static Collection<ConcurrentHashMap<String, SessionAndTransaction>> getTransactionSessions() {
		return idToNameToSessionAndTransaction.values();
	}
	/**
	 * Get the submap of mangled name to {@link SessionAndTransaction} instances
	 * @param xid
	 * @return
	 */
	static ConcurrentHashMap<String, SessionAndTransaction> getTransactionSession(TransactionId xid) {
		//if(DEBUG) {
		//	System.out.printf("TransactionManager.getTransactionSession %s%n", xid);
		//	System.out.printf("TransactionManager.getTransactionSession size:%s%n",idToNameToSessionAndTransaction.size());
		//	System.out.printf("TransactionManager.getTransactionSession will return:%s%n",idToNameToSessionAndTransaction.get(xid));
		//}
		return idToNameToSessionAndTransaction.get(xid);
	}
	/**
	 * Associate a TransactionId with a NameToSessionAndTransaction instance. this is
	 * called from associateSession where we want to link an existing TransactionalMap with a TransactionId.<p/>
	 * Calls linkSessionAndTransaction with new map of mangled name to SessionAndTransaction and passed
	 * xid and TransactionalMap tm. Newly formed map is put into idToNameToSessionaAndTransaction with key xid.
	 * @param xid
	 * @param tm
	 * @throws IOException
	 */
	static void setTransaction(TransactionId xid, TransactionalMap tm) throws IOException {
		if(idToNameToSessionAndTransaction.contains(xid))
			throw new IOException("Transaction id table already contains "+xid);
		ConcurrentHashMap<String, SessionAndTransaction> newLink = new ConcurrentHashMap<String, SessionAndTransaction>();
		tm.getSession().linkSessionAndTransaction(xid, tm, newLink);
		idToNameToSessionAndTransaction.put(xid, newLink);
	}
	/**
	 * Create a new empty map of mangled name to SessionAndTransaction, put it in idToNameToSessionAndTransaction
	 * return the new linkage for further population with mangled name and session subclass.
	 * @param xid
	 * @return
	 */
	static ConcurrentHashMap<String, SessionAndTransaction> setTransaction(TransactionId xid) {
		ConcurrentHashMap<String, SessionAndTransaction> newLink = new ConcurrentHashMap<String, SessionAndTransaction>();
		idToNameToSessionAndTransaction.put(xid, newLink);
		return newLink;
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
	 * Extract the list of unique transactions from a subset of the mapping of mangled names to SessionAndTransaction instances
	 * @param tLink
	 * @return
	 */
	static List<Transaction> getTransactions(ConcurrentHashMap<String, SessionAndTransaction> tLink) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		// Get all the SessionAndTransactions instances
		for(Entry<String, SessionAndTransaction> transMaps: tLink.entrySet()) {
			SessionAndTransaction sLink = transMaps.getValue();
			// Get all the transactions active for each TransactionSession
			//if(!transact.getState().equals(TransactionState.COMMITED) &&
			//	!transact.getState().equals(TransactionState.COMMITTED) &&
			//	!transact.getState().equals(TransactionState.ROLLEDBACK))
			if(!retXactn.contains(sLink.getTransaction()))
				retXactn.add(sLink.getTransaction());
		}
		return retXactn;
	}
	/**
	 * Return a list all RocksDB transactions with id's mapped to transactions
	 * in the set of active volumes, regardless of state.
	 * @return raw RocksDB transactions, use with caution.
	 */
	static List<Transaction> getOutstandingTransactions() {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		for(Entry<TransactionId, ConcurrentHashMap<String, SessionAndTransaction>> transSessions : idToNameToSessionAndTransaction.entrySet()) {
			ConcurrentHashMap<String,SessionAndTransaction> sessAndTrans = transSessions.getValue();
			// Get all the SessionAndTransactions instances which contain the Session and its Transaction
			for(Entry<String, SessionAndTransaction> transMaps: sessAndTrans.entrySet()) {
				SessionAndTransaction sLink = transMaps.getValue();
				// Get all the transactions active for each TransactionSession
					//if(!transact.getState().equals(TransactionState.COMMITED) &&
					//	!transact.getState().equals(TransactionState.COMMITTED) &&
					//	!transact.getState().equals(TransactionState.ROLLEDBACK))
				if(!retXactn.contains(sLink.getTransaction()))
					retXactn.add(sLink.getTransaction());
			}
		}
		return retXactn;
	}
	/**
	 * Return a list all RocksDB transactions with id's mapped to transactions
	 * in the set of active volumes for a particular path regardless of state.
	 * Build a list of Session instances from the VolumeManager path linkages
	 * then find those sessions in the map of Transaction Id to SessionAndTransaction
	 * @param path the path in question
	 * @return the List of RocksDB Transactions.
	 * @throws IOException 
	 */
	static List<Transaction> getOutstandingTransactions(String path) throws IOException {
		ArrayList<Session> sessions = new ArrayList<Session>();
		Volume v = VolumeManager.get(path);
		// Get all the TransactionalMaps for the volume
		for(TransactionSetInterface transMaps: v.classToIsoTransaction.values()) {
			// Get all the transactions active for each TransactionalMap
			sessions.add(((TransactionalMap)transMaps).getSession());
		}
		return getTransactionsForSessionList(sessions);
	}
	/**
	 * Return a list of transactions associated with a list of sessions
	 * @param sessions
	 * @return
	 */
	private static List<Transaction> getTransactionsForSessionList(List<Session> sessions) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		for(Entry<TransactionId, ConcurrentHashMap<String, SessionAndTransaction>> transSessions : idToNameToSessionAndTransaction.entrySet()) {
			ConcurrentHashMap<String,SessionAndTransaction> sessAndTrans = transSessions.getValue();
			// Get all the SessionAndTransactions instances which contain the Session and its Transaction
			for(Entry<String, SessionAndTransaction> transMaps: sessAndTrans.entrySet()) {
				SessionAndTransaction sLink = transMaps.getValue();
				if(!sessions.contains(sLink.getTransactionSession()))
					continue;
				// Get all the transactions active for each TransactionSession
					//if(!transact.getState().equals(TransactionState.COMMITED) &&
					//	!transact.getState().equals(TransactionState.COMMITTED) &&
					//	!transact.getState().equals(TransactionState.ROLLEDBACK))
				if(!retXactn.contains(sLink.getTransaction()))
					retXactn.add(sLink.getTransaction());
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
	static List<Transaction> getOutstandingTransactionsAlias(String alias) throws NoSuchElementException, IOException {
		ArrayList<Session> sessions = new ArrayList<Session>();
		Alias al = new Alias(alias);
		Volume v = VolumeManager.getByAlias(alias);
		// Get all the TransactionalMaps for the volume
		for(TransactionSetInterface transMaps: v.classToIsoTransaction.values()) {
			// Get all the transactions active for each TransactionalMap
			if(transMaps.getSession() instanceof TransactionSessionAlias &&
				((TransactionSessionAlias)transMaps.getSession()).getAlias().equals(al)) {
				sessions.add(transMaps.getSession());
			}
		}
		return getTransactionsForSessionList(sessions);
	}
	
	/**
	 * Get a list of transactions with the given id to verify whether it appears in multiple volumes.
	 * @param uid
	 * @return
	 */
	static List<Transaction> getOutstandingTransactionsById(String uid) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		for(Entry<TransactionId, ConcurrentHashMap<String, SessionAndTransaction>> sessions : idToNameToSessionAndTransaction.entrySet()) {
			// Get all the TransactionalMaps for the session
			if(sessions.getKey().getTransactionId().equals(uid)) {
				ConcurrentHashMap<String,SessionAndTransaction> sessAndTrans = sessions.getValue();
				// Get all the SessionAndTransactions instances which contain the Session and its Transaction
				for(Entry<String, SessionAndTransaction> transMaps: sessAndTrans.entrySet()) {
					SessionAndTransaction sLink = transMaps.getValue();
					// Get all the transactions active for each TransactionSession
					//if(!transact.getState().equals(TransactionState.COMMITED) &&
					//	!transact.getState().equals(TransactionState.COMMITTED) &&
					//	!transact.getState().equals(TransactionState.ROLLEDBACK))
					if(!retXactn.contains(sLink.getTransaction()))
						retXactn.add(sLink.getTransaction());
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
		Alias al = new Alias(alias);
		ConcurrentHashMap<String, SessionAndTransaction> sessions = idToNameToSessionAndTransaction.get(new TransactionId(uid));
		if(sessions != null) {
			for(SessionAndTransaction sLink : sessions.values()) {
				if(sLink.getTransactionSession() instanceof TransactionSessionAlias &&
					((TransactionSessionAlias)sLink.getTransactionSession()).getAlias().equals(al)) {
					// Get all the transactions active for each TransactionSession
					//if(!transact.getState().equals(TransactionState.COMMITED) &&
					//	!transact.getState().equals(TransactionState.COMMITTED) &&
					//	!transact.getState().equals(TransactionState.ROLLEDBACK))
					if(!retXactn.contains(sLink.getTransaction()))
						retXactn.add(sLink.getTransaction());
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
		ConcurrentHashMap<String, SessionAndTransaction> sessions = idToNameToSessionAndTransaction.get(new TransactionId(uid));
		if(DEBUG)
			System.out.println("TransactionManager.getOutstandingTransactionsByPathAndId for path:"+path+" id:"+uid+" got volume "+v);
		// Get all the TransactionalMaps for the volume, compare sessions
		if(sessions != null) {
			Collection<SessionAndTransaction> sLink = sessions.values();
			for(TransactionSetInterface transMaps: v.classToIsoTransaction.values()) {
				// see if TransactionMap has a session that matches the session in the collection attached to this transaction id
				for(SessionAndTransaction session : sLink) {
					if(session.getTransactionSession().equals(transMaps.getSession())) {
						if(DEBUG)
							System.out.println("TransactionManager.getOutstandingTransactionsByPathAndId adding:"+session);
						if(!retXactn.contains(session.getTransaction())) {
							retXactn.add(session.getTransaction());
						}
					}
				}
			}
		}
		if(DEBUG)
			System.out.println("TransactionManager.getOutstandingTransactionsByPathAndId returning:"+retXactn.size());
		return retXactn;
	}
	/**
	 * Get the Transaction objects formed from id. 
	 * @param transactionId
	 * @return Set of names and RocksDb Transaction objects or null if none found
	 */
	public static ConcurrentHashMap<String, SessionAndTransaction> getTransactions(TransactionId transactionId) {
		return idToNameToSessionAndTransaction.get(transactionId);
	}
	/**
	 * Get the Transaction objects formed from id and class name. 
	 * @param transactionId
	 * @param clazz
	 * @return Set of names and RocksDb Transaction objects or null if none found
	 */
	public static List<Transaction> getTransactions(TransactionId transactionId, String clazz) {
		ConcurrentHashMap<String, SessionAndTransaction> all = getTransactions(transactionId);
		ArrayList<Transaction> names = new ArrayList<Transaction>();
		if(all != null) {
			for(Entry<String, SessionAndTransaction> alle : all.entrySet()) {
				if(alle.getKey().startsWith(transactionId.getTransactionId()+clazz)) {
					names.add(alle.getValue().getTransaction());
				}
			}
		}
		return names;
	}
	
	/**
	 * Get the Transaction objects formed from class name. 
	 * @param clazz
	 * @return Set of names and RocksDb Transaction objects or null if none found
	 */
	public static ArrayList<Transaction> getTransactions(String clazz) {
		List<Transaction> all = getOutstandingTransactions();
		ArrayList<Transaction> names = new ArrayList<Transaction>();
		for(Transaction alle : all) {
			if(alle.getName().substring(36).startsWith(clazz)) {
				names.add(alle);
			}
		}
		return names;
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
	 * @throws RocksDBException 
	 */
	static void clearOutstandingTransaction(String uid) throws RocksDBException, IOException {
		for(Transaction t: getOutstandingTransactions()) {
				if(t.getName().startsWith(uid)) {
					t.rollback();
					removeTransaction(uid);
				}
		}
	}
	
	public static void commit(String uid) throws RocksDBException {
		for(Transaction t: getOutstandingTransactions()) {
			if(t.getName().startsWith(uid)) {			
					t.commit();
			}
		}
	}
	
	public static void rollback(String uid) throws RocksDBException {
		for(Transaction t: getOutstandingTransactions()) {
			if(t.getName().startsWith(uid)) {			
					t.rollback();
			}
		}
	}	
	
	public static void checkpoint(String uid) throws RocksDBException {
		for(Transaction t: getOutstandingTransactions()) {
			if(t.getName().startsWith(uid)) {			
					t.setSavePoint();
			}
		}
	}
	
	public static void rollbackToCheckpoint(String uid) throws RocksDBException {
		for(Transaction t: getOutstandingTransactions()) {
			if(t.getName().startsWith(uid)) {			
					t.rollbackToSavePoint();
			}
		}
	}
	
	/**
	 * Remove the transaction from all volumes that had an idToTransaction table entry with this transaction id.
	 * @param uid The target transaction id
	 * @throws IOException If transaction state was not STARTED, COMMITTED, or ROLLEDBACK
	 */
	static void removeTransaction(String uid) throws IOException {
		TransactionId tid = new TransactionId(uid);
		ConcurrentHashMap<String, SessionAndTransaction> tis = idToNameToSessionAndTransaction.get(tid);
		List<String> ts = new ArrayList<String>();
		for(Entry<String, SessionAndTransaction> s : tis.entrySet()) {
			if(DEBUG) {
				System.out.printf("TransactionManager.removeTransaction xid:%s name:%s%n", uid, s.getValue());
			}
			// sanity check
			if(!s.getKey().startsWith(uid))
				throw new IOException("Encountered corrupt idToNameToSessionAndTransaction entry");
			ts.add(s.getKey());
		}
		for(String s : ts) {
			tis.remove(s);
		}
		if(tis.isEmpty()) {
			if(DEBUG) {
				System.out.printf("TransactionManager.removeTransaction removing empty idToNameToSessionAndTransaction map entry for xid:%s%n", tid);
			}
			idToNameToSessionAndTransaction.remove(tid);
		}
	}

}
