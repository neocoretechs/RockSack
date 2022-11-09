package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.neocoretechs.rocksack.DBPhysicalConstants;



/**
 * This factory class enforces a strong typing for the RockSack using the database naming convention linked to the
 * class name of the class stored there.<p/>
 * In almost all cases, this is the main entry point to obtain a BufferedMap or a TransactionalMap.<p/>
 * 
 * The main function of this adapter is to ensure that the appropriate map is instantiated.<br/>
 * A map can be obtained by instance of Comparable to impart ordering.<br/>
 * A Buffered map has atomic transactions bounded automatically with each insert/delete.<br/>
 * A transactional map requires commit/rollback and can be checkpointed.
 * In either case recovery is in effect to preserve integrity.
 * The database name is the full path of the top level tablespace and log directory, i.e.
 * /home/db/test would create a 'test' database in the /home/db directory. If we are using this strong
 * typing adapter, and were to store a String, the database name would translate to: /home/db/testjava.lang.String.
 * The class name is translated into the appropriate file name via a simple translation table to give us a
 * database/class/tablespace identifier for each file used.
 * BufferedMap returns one instance of the class for each call to get the map. Transactional maps create a new instance with a new
 * transaction context using the originally opened database, and so must be maintained in another context for each transaction.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2014,2015,2021,2022
 *
 */
public class RockSackAdapter {
	private static boolean DEBUG = true;
	private static String tableSpaceDir = "/";
	private static final char[] ILLEGAL_CHARS = { '[', ']', '!', '+', '=', '|', ';', '?', '*', '\\', '<', '>', '|', '\"', ':' };
	private static final char[] OK_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E' };
	
	static {
		RocksDB.loadLibrary();
	}
	private static ConcurrentHashMap<String, SetInterface> classToIso = new ConcurrentHashMap<String,SetInterface>();
	private static ConcurrentHashMap<String, ConcurrentHashMap<String,SetInterface>> classToIsoTransaction = new ConcurrentHashMap<String,ConcurrentHashMap<String,SetInterface>>();
	
	public static String getTableSpaceDir() {
		return tableSpaceDir;
	}
	public static void setTableSpaceDir(String tableSpaceDir) {
		RockSackAdapter.tableSpaceDir = tableSpaceDir;
	}

	public static String getDatabaseName(Class clazz) {
		String xClass = translateClass(clazz.getName());
		return tableSpaceDir+xClass;
	}
	public static String getDatabaseName(String clazz) {
		return tableSpaceDir+clazz;
	}

	
	/**
	 * Get a Map via Comparable instance.
	 * @param clazz The Comparable object that the java class name is extracted from
	 * @return A BufferedTreeMap for the clazz instances.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedMap getRockSackMap(Comparable clazz) throws IllegalAccessException, IOException {
		return getRockSackMap(clazz.getClass());
	}
	/**
	 * Get a TreeMap via Java Class type.
	 * @param clazz The Java Class of the intended database
	 * @return The BufferedTreeMap for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedMap getRockSackMap(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		BufferedMap ret = (BufferedMap) classToIso.get(xClass);
		if(DEBUG)
			System.out.println("RockSackAdapter.getRockSackTreeMap About to return designator: "+tableSpaceDir+xClass+" formed from "+clazz.getClass().getName());
		if( ret == null ) {
			ret =  new BufferedMap(tableSpaceDir+xClass,DBPhysicalConstants.DATABASE, DBPhysicalConstants.BACKINGSTORE);
			classToIso.put(xClass, ret);
		}
		return ret;
	}

	/**
	 * Get a TransactionalTreeMap via Comparable instance. Retrieve an existing transaction
	 * @param clazz The Comparable object that the java class name is extracted from.
	 * @return A TransactionalTreeMap for the clazz instances.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static TransactionalMap getRockSackTransactionalMap(Comparable clazz, String xaction) throws IllegalAccessException, IOException {
		return getRockSackTransactionalMap(clazz.getClass(), xaction);
	}
	/**
	 * Get a TransactionalTreeMap via Comparable instance. Create the initial transaction.
	 * @param clazz The Comparable object that the java class name is extracted from.
	 * @return A TransactionalTreeMap for the clazz instances.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static TransactionalMap getRockSackTransactionalMap(Comparable clazz) throws IllegalAccessException, IOException {
		return getRockSackTransactionalMap(clazz.getClass());
	}
	/**
	 * Get a TransactionalTreeMap via Java Class type. Create the initial transaction.
	 * We connect a new transaction to the first {@link RockSackTransactionSession} 
	 * in the collection value of the database key at classToIsoTransaction.
	 * @param clazz The Java Class of the intended database.
	 * @return The TransactionalTreeMap for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static TransactionalMap getRockSackTransactionalMap(Class clazz) throws IllegalAccessException, IOException {
		TransactionalMap ret = null;
		String xClass = translateClass(clazz.getName());
		ConcurrentHashMap<String, SetInterface> xactions = classToIsoTransaction.get(xClass);
		// If we find no collection of transactions, the DB has not been opened for even one
		if(xactions == null) {
			ret = new TransactionalMap(tableSpaceDir+xClass, DBPhysicalConstants.DATABASE, DBPhysicalConstants.BACKINGSTORE);
			xactions = new ConcurrentHashMap<String, SetInterface>();
			try {
				ret.txn.setName(UUID.randomUUID().toString());
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
			xactions.put(ret.txn.getName(), ret);
			classToIsoTransaction.put(xClass, xactions);
			if(DEBUG)
				System.out.println("RockSackAdapter.getRockSackMapTransaction About to return first initial designator: "+tableSpaceDir+xClass+
						" transaction:"+ret.txn.getName()+" formed from "+clazz.getClass().getName());
			return ret;
		}
		// connect it to the same session as existing transactions. session contains database info
		// session is database level, not user level...
		RockSackTransactionSession ti = ((TransactionalMap)xactions.values().toArray()[0]).session;
		ret = new TransactionalMap(ti);
		try {
			ret.txn.setName(UUID.randomUUID().toString());
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
		xactions.put(ret.txn.getName(), ret);
		if(DEBUG)
			System.out.println("RockSackAdapter.getRockSackMapTransaction About to return additional initial designator: "+tableSpaceDir+xClass+
						" transaction:"+ret.txn.getName()+" formed from "+clazz.getClass().getName());
		return ret;
	}
	/**
	 * Get a TransactionalTreeMap via Java Class type. Retrieve an existing transaction.
	 * @param clazz The Java Class of the intended database.
	 * @param xaction The name of the transaction
	 * @return The TransactionalTreeMap for the clazz type. null if not found.
	 * @throws IllegalAccessException
	 * @throws IOException If no transactions are active for the given class/db
	 */
	public static TransactionalMap getRockSackTransactionalMap(Class clazz, String xaction) throws IllegalAccessException, IOException {
		SetInterface ret = null;
		String xClass = translateClass(clazz.getName());
		ConcurrentHashMap<String, SetInterface> xactions = classToIsoTransaction.get(xClass);
		if(xactions == null) {
			throw new IOException("No transactions active for class "+xClass);
		}
		ret = xactions.get(xaction);
		if(ret == null)
			return null;
		return (TransactionalMap) ret;
	}
	/**
	 * Remove the given TransactionalMap from active DB/transaction collection
	 * @param tmap
	 */
	public static void removeRockSackTransactionalMap(SetInterface tmap) {
		classToIsoTransaction.forEach((k,v) -> {
			if(v.contains(tmap)) {
				TransactionalMap verify = (TransactionalMap) v.remove(((TransactionalMap)tmap).txn.getName());
				if(DEBUG)
					System.out.println("RockSackAdapter.removeRockSackTransactionalMap removing xaction "+((TransactionalMap)tmap).txn.getName()+" for DB "+k+" which should match "+verify.txn.getName());
				return;
			}
		});
	}
	/**
	 * Translate a class name into a legitimate file name with some aesthetics.
	 * @param clazz
	 * @return
	 */
	public static String translateClass(String clazz) {
		//boolean hasReplaced = false; // debug
		StringBuffer sb = new StringBuffer();
		for(int i = 0; i < clazz.length(); i++) {
			char chr = clazz.charAt(i);
			for(int j = 0; j < ILLEGAL_CHARS.length; j++) {
				if( chr == ILLEGAL_CHARS[j] ) {
					chr = OK_CHARS[j];
					//hasReplaced = true;
					break;
				}
			}
			sb.append(chr);
		}
		//if( hasReplaced )
		//	System.out.println("Class name translated from "+clazz+" to "+sb.toString());
		return sb.toString();
	}

}
