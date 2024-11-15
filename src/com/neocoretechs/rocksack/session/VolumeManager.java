package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.Transaction.TransactionState;

import com.neocoretechs.rocksack.Alias;
import com.neocoretechs.rocksack.TransactionId;
/**
 * A volume is a set of RocksDb directories and files indexed by a volume name, which is a directory path PLUS database prefix. The
 * last component of the volume refers to the prefix of a database name within the parent path. When forming filesets for RocksDb, the
 * volume, concatenated with the class name, form a RockDB database. For instance:
 * /Users/db/test would create a database test in the /Users/db path and resultant RocksDB database files for instances of the
 * String class would appear in the directory /Users/db/testjava.lang.String <p/>
 * A subsequent call to RockSackAdapter.setTableSpaceDir("/Users/db/test") would set the default tablespace and directory
 * to the test database in /Users/db and method calls lacking an alias would reference this default tablespace.<p/>
 * In the case of an alias, one may use the calls to createAlias("alias","/Users/db/test") to establish an alias to that tablespace
 * for subsequent calls to methods that use an alias when working with multiple databases. One an alias has been established, the
 * tablespace CANNOT be assigned to another alias without first issuing calls to explicitly remove the alias first. 
 * An IllegalArgumentException will be thrown.<br/>
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2023
 *
 */
public class VolumeManager {
	private static boolean DEBUG = true;
	private static ConcurrentHashMap<String, Volume> pathToVolume = new ConcurrentHashMap<String,Volume>();
	private static ConcurrentHashMap<Alias, String> aliasToPath = new ConcurrentHashMap<Alias,String>();
	/**
	 * Index by tablespaceDir
	 */
	static class Volume {
		public ConcurrentHashMap<String, SetInterface> classToIso = new ConcurrentHashMap<String,SetInterface>();
		// these are active in a transaction context
		public ConcurrentHashMap<String, TransactionSetInterface> classToIsoTransaction = new ConcurrentHashMap<String, TransactionSetInterface>();
	}
	/**
	 * Get the volume for the given tablespace path. If the volume does not exist, it will be created
	 * @param path
	 * @return
	 */
	static Volume get(String path) {
		if(DEBUG)
			System.out.println("VolumeManager.get attempt for path:"+path);
		Volume v = pathToVolume.get(path);
		if(v == null) {
			if(DEBUG)
				System.out.println("VolumeManager.get creating new volume for path:"+path);
			v = new Volume();
			pathToVolume.put(path, v);
		}
		return v;
	}
	/**
	 * Get the tablespace path for the given alias
	 * @param alias
	 * @return The path for this alias or null if none
	 */
	static String getAliasToPath(String alias) {
		if(DEBUG)
			System.out.println("VolumeManager.getAliasToPath attempt for alias:"+alias+" will return:"+aliasToPath.get(new Alias(alias)));
		return aliasToPath.get(new Alias(alias));
	}
	/**
	 * Get the tablespace path for the given alias
	 * @param alias
	 * @return The path for this alias or null if none
	 */
	static String getAliasToPath(Alias alias) {
		if(DEBUG)
			System.out.println("VolumeManager.getAliasToPath attempt for alias:"+alias+" will return:"+aliasToPath.get(alias));
		return aliasToPath.get(alias);
	}
	/**
	 * Get the volume for the given alias. If the alias does not exist, the volume will NOT be created.
	 * An explicit createAlias call is needed.
	 * @param alias
	 * @return The Volume for the alias
	 * @throws NoSuchElementException If the alias was not found
	 */
	static Volume getByAlias(String alias) throws NoSuchElementException {
		String path = aliasToPath.get(new Alias(alias));
		if(path == null)
			throw new NoSuchElementException("The alias "+alias+" was not found.");
		if(DEBUG)
			System.out.println("VolumeManager.getByAlias attempt for alias:"+alias+" got path:"+path);
		return get(path);
	}
	/**
	 * Get the volume for the given alias. If the alias does not exist, the volume will NOT be created.
	 * An explicit createAlias call is needed.
	 * @param alias
	 * @return The Volume for the alias
	 * @throws NoSuchElementException If the alias was not found
	 */
	static Volume getByAlias(Alias alias) throws NoSuchElementException {
		String path = aliasToPath.get(alias);
		if(path == null)
			throw new NoSuchElementException("The alias "+alias+" was not found.");
		if(DEBUG)
			System.out.println("VolumeManager.getByAlias attempt for alias:"+alias+" got path:"+path);
		return get(path);
	}	
	/**
	 * @return The aliases and their paths as 2d array. 1st dim is 0 if none.
	 */
	static String[][] getAliases() {
		String[][] array = new String[aliasToPath.size()][2];
		int count = 0;
		for(Map.Entry<Alias,String> entry : aliasToPath.entrySet()){
		    array[count][0] = entry.getKey().getAlias();
		    array[count][1] = entry.getValue();
		    count++;
		}
		return array;
	}
	/**
	 * Create an alias for the given tablespace path
	 * @param alias
	 * @param path
	 */
	static Volume createAlias(String alias, String path) throws IllegalArgumentException {
		if(DEBUG)
			System.out.println("VolumeManager.createAlias for alias:"+alias+" and path:"+path);
		String prevAlias = aliasToPath.get(new Alias(alias));
		if(prevAlias != null)
			throw new IllegalArgumentException("Alias "+alias+" already assigned. Must be explicitly removed before reassignment.");
		aliasToPath.put(new Alias(alias), path);
		return get(path);
	}
	/**
	 * Create an alias for the given tablespace path
	 * @param alias
	 * @param path
	 */
	static Volume createAlias(Alias alias, String path) throws IllegalArgumentException {
		if(DEBUG)
			System.out.println("VolumeManager.createAlias for alias:"+alias+" and path:"+path);
		String prevAlias = aliasToPath.get(alias);
		if(prevAlias != null)
			throw new IllegalArgumentException("Alias "+alias+" already assigned. Must be explicitly removed before reassignment.");
		aliasToPath.put(alias, path);
		return get(path);
	}
	/**
	 * Remove the alias for the given tablespace path. The volume will not be affected.
	 * @param alias
	 */
	static void removeAlias(String alias) {
		if(DEBUG)
			System.out.println("VolumeManager.removeAlias for alias:"+alias);
		aliasToPath.remove(new Alias(alias));
	}
	/**
	 * Remove the alias for the given tablespace path. The volume will not be affected.
	 * @param alias
	 */
	static void removeAlias(Alias alias) {
		if(DEBUG)
			System.out.println("VolumeManager.removeAlias for alias:"+alias);
		aliasToPath.remove(alias);
	}
	/**
	 * Remove the Volume for the given tablespace path.
	 * @param path
	 * @return
	 */
	static Volume remove(String path) {
		if(DEBUG)
			System.out.println("VolumeManager.remove for path:"+path+" will return previous Volume:"+pathToVolume.remove(path));
		return pathToVolume.remove(path);
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
		for(Map.Entry<String, Volume> volumes : pathToVolume.entrySet()) {
			// Get all the TransactionalMaps for the volume
			for(TransactionSetInterface transMaps: volumes.getValue().classToIsoTransaction.values()) {
				// Get all the transactions active for each TransactionalMap
				Collection<Transaction> transactions = ((TransactionalMap)transMaps).getTransactions();
				for(Transaction transact: transactions) {
					//if(!transact.getState().equals(TransactionState.COMMITED) &&
					//	!transact.getState().equals(TransactionState.COMMITTED) &&
					//	!transact.getState().equals(TransactionState.ROLLEDBACK))
					retXactn.add(transact);
				}
			}
		}
		return retXactn;
	}
	/**
	 * Return a list all RocksDB transactions with id's mapped to transactions
	 * in the set of active volumes for a particular path regardless of state.
	 * @param path the path in question
	 * @return the List of RocksDB Transactions. be wary.
	 */
	static List<Transaction> getOutstandingTransactions(String path) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		Volume v = get(path);
		// Get all the TransactionalMaps for the volume
		for(TransactionSetInterface transMaps: v.classToIsoTransaction.values()) {
			// Get all the transactions active for each TransactionalMap
			Collection<Transaction> transactions = ((TransactionalMap)transMaps).getTransactions();
			for(Transaction transact: transactions) {
				//if(!transact.getState().equals(TransactionState.COMMITED) &&
				//	!transact.getState().equals(TransactionState.COMMITTED) &&
				//	!transact.getState().equals(TransactionState.ROLLEDBACK))
				retXactn.add(transact);
			}
		}
		return retXactn;
	}
	/**
	 * Return a list all RocksDB transactions with id's mapped to transactions
	 * in the set of active volumes for a particular alias to a path regardless of state
	 * @param alias the path alias
	 * @return the List of RocksDB Transactions.
	 */
	static List<Transaction> getOutstandingTransactionsAlias(String alias) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		Volume v = getByAlias(alias);
		// Get all the TransactionalMaps for the volume
		for(TransactionSetInterface transMaps: v.classToIsoTransaction.values()) {
			// Get all the transactions active for each TransactionalMap
			Collection<Transaction> transactions = ((TransactionalMap)transMaps).getTransactions();
			for(Transaction transact: transactions) {
				//if(!transact.getState().equals(TransactionState.COMMITED) &&
				//	!transact.getState().equals(TransactionState.COMMITTED) &&
				//	!transact.getState().equals(TransactionState.ROLLEDBACK))
				retXactn.add(transact);
			}
		}
		return retXactn;
	}
	
	/**
	 * Get a list of transactions with the given id to verify whether it appears in multiple volumes.
	 * This might signal a user discrepancy.
	 * @param uid
	 * @return
	 */
	static List<Transaction> getOutstandingTransactionsById(String uid) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		for(Map.Entry<String, Volume> volumes : pathToVolume.entrySet()) {
			// Get all the TransactionalMaps for the volume
			for(TransactionSetInterface transMaps: volumes.getValue().classToIsoTransaction.values()) {
				// Get all the transactions active for each TransactionalMap
				Collection<Transaction> transactions = ((TransactionalMap)transMaps).getTransactions();
				for(Transaction transact: transactions) {
					//if(!transact.getState().equals(TransactionState.COMMITED) &&
					//	!transact.getState().equals(TransactionState.COMMITTED) &&
					//	!transact.getState().equals(TransactionState.ROLLEDBACK))
					if(transact.getName().equals(uid))
						retXactn.add(transact);
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
		Volume v = getByAlias(alias);
		// Get all the TransactionalMaps for the volume
		for(TransactionSetInterface transMaps: v.classToIsoTransaction.values()) {
			// Get all the transactions active for each TransactionalMap
			Collection<Transaction> transactions = ((TransactionalMap)transMaps).getTransactions();
			for(Transaction transact: transactions) {
				//if(!transact.getState().equals(TransactionState.COMMITED) &&
				//	!transact.getState().equals(TransactionState.COMMITTED) &&
				//	!transact.getState().equals(TransactionState.ROLLEDBACK))
				if(transact.getName().equals(uid))
					retXactn.add(transact);
			}
		}
		return retXactn;
	}
	
	/**
	 * Get the list of outstanding transactions based on tablespace path and transaction id, regardless of state.
	 * @param path
	 * @param uid
	 * @return The list of Transaction
	 */
	static List<Transaction> getOutstandingTransactionsByPathAndId(String path, String uid) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		Volume v = get(path);
		if(DEBUG)
			System.out.println("VolumeManager.getOutstandingTransactionsByPathAndId for path:"+path+" id:"+uid+" got volume "+v);
		// Get all the TransactionalMaps for the volume
		for(TransactionSetInterface transMaps: v.classToIsoTransaction.values()) {
			// Get all the transactions active for each TransactionalMap
			Collection<Transaction> transactions = ((TransactionalMap)transMaps).getTransactions();
			for(Transaction transact: transactions) {
				if(DEBUG)
					System.out.println("VolumeManager.getOutstandingTransactionsByPathAndId trying transaction:"+transact.getName());
				//if(!transact.getState().equals(TransactionState.COMMITED) &&
				//	!transact.getState().equals(TransactionState.COMMITTED) &&
				//	!transact.getState().equals(TransactionState.ROLLEDBACK))
				if(transact.getName().equals(uid)) {
					if(DEBUG)
						System.out.println("VolumeManager.getOutstandingTransactionsByPathAndId adding:"+transact.getName());
					retXactn.add(transact);
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
		for(Map.Entry<String, Volume> volumes : pathToVolume.entrySet()) {
			
		}
	}
	/**
	 * Roll back all transactions associated with the given transaction uid
	 * @param uid
	 */
	static void clearOutstandingTransaction(String uid) {
		for(Map.Entry<String, Volume> volumes : pathToVolume.entrySet()) {
			// Get all the TransactionalMaps for the volume
			for(TransactionSetInterface transMaps: volumes.getValue().classToIsoTransaction.values()) {
				// Get all the transactions active for each TransactionalMap
				Collection<Transaction> transactions = ((TransactionalMap)transMaps).getTransactions();
				for(Transaction transact: transactions) {
					//if(!transact.getState().equals(TransactionState.COMMITED) &&
					//	!transact.getState().equals(TransactionState.COMMITTED) &&
					//	!transact.getState().equals(TransactionState.ROLLEDBACK))
					if(transact.getName().equals(uid)) {
						if(DEBUG)
							System.out.println("ClearOutstandingTransaction found uid "+uid+" in volume map.");
						try {
							transact.rollback();
						} catch (RocksDBException e) {
							if(DEBUG)
								System.out.println("ClearOutstandingTransaction found uid cant rollback "+uid+" exception "+e);
						}
						if(DEBUG)
							System.out.println("ClearOutstandingTransaction found uid "+uid+" in volume map. Removing "+uid+" key.");
						((TransactionalMap)transMaps).removeTransaction(new TransactionId(uid));
						// id can occur multiple places, so continue
					}
				}
			}
		}
	}
	/**
	 * Remove the transaction from all volumes that had an idToTransaction table entry with this transaction id.
	 * @param uid The target transaction id
	 * @return The unique list of volumes that had an entry for this id. Typically only 1, or empty list.
	 * @throws IOException If transaction state was not STARTED, COMMITTED, or ROLLEDBACK
	 */
	static List<Volume> removeTransaction(String uid) throws IOException {
		ArrayList<Volume> rv = new ArrayList<Volume>();
		for(Map.Entry<String, Volume> volumes : pathToVolume.entrySet()) {
			for(TransactionSetInterface transMaps: volumes.getValue().classToIsoTransaction.values()) {
				// Get all the transactions active for each TransactionalMap
				Collection<Transaction> transactions = ((TransactionalMap)transMaps).getTransactions();
				for(Transaction transact: transactions) {
					//if(!transact.getState().equals(TransactionState.COMMITED) &&
					//	!transact.getState().equals(TransactionState.COMMITTED) &&
					//	!transact.getState().equals(TransactionState.ROLLEDBACK))
					if(transact.getName().equals(uid)) {
						if(transact.getState().equals(TransactionState.COMMITTED) || transact.getState().equals(TransactionState.ROLLEDBACK) ||
								transact.getState().equals(TransactionState.STARTED)	) {
							((TransactionalMap)transMaps).removeTransaction(new TransactionId(uid));
							if(!rv.contains(volumes.getValue()))
								rv.add(volumes.getValue());
						} else
							throw new IOException("Transaction "+uid+" is in state "+transact.getState().name()+" must be COMMITTED or ROLLEDBACK or STARTED for removal");
						if(DEBUG) {
							System.out.println("VolumeManager removed uid "+uid+" for transaction "+transact);
						}
					}
				}
			}
		}
		return rv;
	}
}
