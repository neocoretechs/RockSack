package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.RocksDB;

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
	private static boolean DEBUG = false;
	private static String tableSpaceDir = "/";
	private static final char[] ILLEGAL_CHARS = { '[', ']', '!', '+', '=', '|', ';', '?', '*', '\\', '<', '>', '|', '\"', ':' };
	private static final char[] OK_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E' };
	
	static {
		RocksDB.loadLibrary();
	}
	private static ConcurrentHashMap<String, SetInterface> classToIso = new ConcurrentHashMap<String,SetInterface>();
	
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
	 * Get a TreeMap via Comparable instance.
	 * @param clazz The Comparable object that the java class name is extracted from
	 * @return A BufferedTreeMap for the clazz instances.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedMap getRockSackTreeMap(Comparable clazz) throws IllegalAccessException, IOException {
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
	 * Get a TransactionalTreeMap via Comparable instance.
	 * @param clazz The Comparable object that the java class name is extracted from.
	 * @return A TransactionalTreeMap for the clazz instances.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static TransactionalMap getRockSackTransactionalMap(Comparable clazz) throws IllegalAccessException, IOException {
		return getRockSackTransactionalMap(clazz.getClass());
	}
	/**
	 * Get a TransactionalTreeMap via Java Class type.
	 * @param clazz The Java Class of the intended database.
	 * @return The TransactionalTreeMap for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static TransactionalMap getRockSackTransactionalMap(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		TransactionalMap ret = (TransactionalMap) classToIso.get(xClass);
		if(DEBUG)
			System.out.println("RockSackAdapter.getRockSackMapTransaction About to return designator: "+tableSpaceDir+xClass+" formed from "+clazz.getClass().getName());
		if( ret == null ) {
			ret =  new TransactionalMap(tableSpaceDir+xClass, DBPhysicalConstants.DATABASE, DBPhysicalConstants.BACKINGSTORE);
			classToIso.put(xClass, ret);
			return ret;
		}
		return new TransactionalMap(ret.session);
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
