package com.neocoretechs.rocksack.session;

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
	/**
	 * Index by tablespaceDir
	 */
	public static class Volume {
		public ConcurrentHashMap<String, SetInterface> classToIso = new ConcurrentHashMap<String,SetInterface>();
		// these are active in a transaction context
		public ConcurrentHashMap<String, ConcurrentHashMap<String,SetInterface>> classToIsoTransaction = new ConcurrentHashMap<String,ConcurrentHashMap<String,SetInterface>>();
		public ConcurrentHashMap<String, Transaction> idToTransaction = new ConcurrentHashMap<String, Transaction>();
	}
	
	public static Volume get(String path) {
		Volume v = pathToVolume.get(path);
		if(v == null) {
			v = new Volume();
			pathToVolume.put(path, v);
		}
		return v;
	}
	
	public static Volume remove(String path) {
		return pathToVolume.remove(path);
	}
}
