package com.neocoretechs.rocksack.session;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.Transaction;
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
	public static class Volume {
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
	public static Volume get(String path) {
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
	 * @return
	 */
	public static String getAliasToPath(String alias) {
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
	public static Volume getByAlias(String alias) throws NoSuchElementException {
		String path = aliasToPath.get(alias);
		if(path == null)
			throw new NoSuchElementException("The alias "+alias+" was not found.");
		if(DEBUG)
			System.out.println("VolumeManager.getByAlias attempt for alias:"+alias+" got path:"+path);
		return get(path);
	}
	/**
	 * Create an alias for the given tablespace path
	 * @param alias
	 * @param path
	 */
	public static Volume createAlias(String alias, String path) throws IllegalArgumentException {
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
	public static void removeAlias(String alias) {
		if(DEBUG)
			System.out.println("VolumeManager.removeAlias for alias:"+alias);
		aliasToPath.remove(alias);
	}
	/**
	 * Remove the Volume for the given tablespace path.
	 * @param path
	 * @return
	 */
	public static Volume remove(String path) {
		if(DEBUG)
			System.out.println("VolumeManager.remove for path:"+path+" will return previous Volume:"+pathToVolume.remove(path));
		return pathToVolume.remove(path);
	}
}
