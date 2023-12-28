package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.Transaction.TransactionState;
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
	private static boolean DEBUG = false;
	private static ConcurrentHashMap<String, Volume> pathToVolume = new ConcurrentHashMap<String,Volume>();
	private static ConcurrentHashMap<String, String> aliasToPath = new ConcurrentHashMap<String,String>();
	/**
	 * Index by tablespaceDir
	 */
	static class Volume {
		public ConcurrentHashMap<String, SetInterface> classToIso = new ConcurrentHashMap<String,SetInterface>();
		// these are active in a transaction context
		public ConcurrentHashMap<String, ConcurrentHashMap<String,SetInterface>> classToIsoTransaction = new ConcurrentHashMap<String,ConcurrentHashMap<String,SetInterface>>();
		public ConcurrentHashMap<String, Transaction> idToTransaction = new ConcurrentHashMap<String, Transaction>();
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
		for(Map.Entry<String,String> entry : aliasToPath.entrySet()){
		    array[count][0] = entry.getKey();
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
	 * in the set of active volumes.
	 * @return raw RocksDB transactions, use with caution.
	 */
	static List<Transaction> getOutstandingTransactions() {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		for(Map.Entry<String, Volume> volumes : pathToVolume.entrySet()) {
			for(Map.Entry<String, Transaction> transactions: volumes.getValue().idToTransaction.entrySet()) {
				retXactn.add(transactions.getValue());
			}
		}
		return retXactn;
	}
	/**
	 * Return a list all RocksDB transactions with id's mapped to transactions
	 * in the set of active volumes for a particular path
	 * @param path the path in question
	 * @return the List of RocksDB Transactions. be wary.
	 */
	static List<Transaction> getOutstandingTransactions(String path) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		Volume v = get(path);
		for(Map.Entry<String, Transaction> transactions: v.idToTransaction.entrySet()) {
				retXactn.add(transactions.getValue());
		}
		return retXactn;
	}
	/**
	 * Return a list all RocksDB transactions with id's mapped to transactions
	 * in the set of active volumes for a particular alias to a path
	 * @param alias the path alias
	 * @return the List of RocksDB Transactions. Use with care.
	 */
	static List<Transaction> getOutstandingTransactionsAlias(String alias) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		Volume v = getByAlias(alias);
		for(Map.Entry<String, Transaction> transactions: v.idToTransaction.entrySet()) {
				retXactn.add(transactions.getValue());
		}
		return retXactn;
	}
	
	static List<Transaction> getOutstandingTransactionsById(String uid) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		for(Map.Entry<String, Volume> volumes : pathToVolume.entrySet()) {
			for(Map.Entry<String, Transaction> transactions: volumes.getValue().idToTransaction.entrySet()) {
				if(transactions.getKey().equals(uid))
					retXactn.add(transactions.getValue());
			}
		}
		return retXactn;
	}
	
	static List<Transaction> getOutstandingTransactionsByAliasAndId(String alias, String uid) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		Volume v = getByAlias(alias);
		for(Map.Entry<String, Transaction> transactions: v.idToTransaction.entrySet()) {
			if(transactions.getKey().equals(uid))
				retXactn.add(transactions.getValue());
		}
		return retXactn;
	}
	
	static List<Transaction> getOutstandingTransactionsByPathAndId(String path, String uid) {
		ArrayList<Transaction> retXactn = new ArrayList<Transaction>();
		Volume v = get(path);
		for(Map.Entry<String, Transaction> transactions: v.idToTransaction.entrySet()) {
			if(transactions.getKey().equals(uid))
				retXactn.add(transactions.getValue());
		}
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
			 volumes.getValue().idToTransaction.clear();
		}
	}
	/**
	 * Roll back all transactions associated with the given transaction uid
	 * @param uid
	 */
	static void clearOutstandingTransaction(String uid) {
				for(Map.Entry<String, Volume> volumes : pathToVolume.entrySet()) {
					 for(Entry<String, Transaction> id: volumes.getValue().idToTransaction.entrySet()) {
						 if(id.getKey().equals(uid)) {
							if(DEBUG)
								System.out.println("ClearOutstandingTransaction found uid "+uid+" in volume map.");
							try {
								id.getValue().rollback();
							} catch (RocksDBException e) {
								if(DEBUG)
									System.out.println("ClearOutstandingTransaction found uid cant rollback "+uid+" exception "+e);
							}
							if(DEBUG)
								System.out.println("ClearOutstandingTransaction found uid "+uid+" in volume map. Removing "+id+" key.");
							volumes.getValue().idToTransaction.remove(id.getKey());
							// id can occur multiple places, so continue
						 }
					 }
				}
	}
	
	static void removeTransaction(String uid) throws IOException {
		for(Map.Entry<String, Volume> volumes : pathToVolume.entrySet()) {
			Transaction removed = volumes.getValue().idToTransaction.get(uid);
			if(removed != null) {
				if(removed.getState().equals(TransactionState.COMMITTED) || removed.getState().equals(TransactionState.ROLLEDBACK) ||
					removed.getState().equals(TransactionState.STARTED)	)
					removed = volumes.getValue().idToTransaction.remove(uid);
				else
					throw new IOException("Transaction "+uid+" is in state "+removed.getState().name()+" must be COMMITTED or ROLLEDBACK or STARTED for removal");
				if(DEBUG) {
					System.out.println("VolumeManager removed uid "+uid+" for transaction "+removed);
				}
			}
		}
	}
}
