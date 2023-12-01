package com.neocoretechs.rocksack.session;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.Transaction;
/**
 * A volume is a set of RocksDb directories and files indexed by a volume name, which is a directory path PLUS database prefix. The
 * last component of the volume refers to the prefix of a database name within the parent path. When forming filesets for RocksDb, the
 * volume, concatenated with the class name, form a RockDB database. For instance:
 * \Users\db\test would create a database test in the \Users\db path and resultant RocksDB database files for instances of the
 * String class would appear in the directory testjava.lang.String under \Users\db <p/>
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2023
 *
 */
public class VolumeManager {
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
		Volume v = pathToVolume.get(path);
		if(v == null) {
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
		return aliasToPath.get(alias);
	}
	/**
	 * Get the volume for the given alias. If the alias does not exist, the volume will NOT be created.
	 * An explicit createAlias call is needed.
	 * @param alias
	 * @return
	 * @throws NoSuchElementException
	 */
	public static Volume getByAlias(String alias) throws NoSuchElementException {
		String path = aliasToPath.get(alias);
		if(path == null)
			throw new NoSuchElementException("The alias "+alias+" was not found.");
		return pathToVolume.get(path);
	}
	/**
	 * Create an alias for the given tablespace path
	 * @param alias
	 * @param path
	 */
	public static void createAlias(String alias, String path) {
		aliasToPath.put(alias, path);
	}
	/**
	 * Remove the alias for the given tablespace path. The volume will not be affected.
	 * @param alias
	 */
	public static void removeAlias(String alias) {
		aliasToPath.remove(alias);
	}
	/**
	 * Remove the Volume for the given tablespace path.
	 * @param path
	 * @return
	 */
	public static Volume remove(String path) {
		return pathToVolume.remove(path);
	}
}
