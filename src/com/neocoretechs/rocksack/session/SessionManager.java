package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;

import com.neocoretechs.rocksack.Alias;

/*
* Copyright (c) 2003, NeoCoreTechs
* All rights reserved.
* Redistribution and use in source and binary forms, with or without modification, 
* are permitted provided that the following conditions are met:
*
* Redistributions of source code must retain the above copyright notice, this list of
* conditions and the following disclaimer. 
* Redistributions in binary form must reproduce the above copyright notice, 
* this list of conditions and the following disclaimer in the documentation and/or
* other materials provided with the distribution. 
* Neither the name of NeoCoreTechs nor the names of its contributors may be 
* used to endorse or promote products derived from this software without specific prior written permission. 
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
* WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
* PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR 
* ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
* TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED 
* OF THE POSSIBILITY OF SUCH DAMAGE.
*
*/
/**
* SessionManager class is a singleton 
* that accepts connections and returns a {@link Session} subclass instance. A table of one to one sessions and
* tables is maintained. Primary means by which {@link DatabaseManager} receives various {@link TransactionSession},
* {@link OptimisticTransactionSession}, etc. and from there obtains the {@link BufferedMap}, {@link TransactionalMap}, etc.
* maps by which most of the work gets done. The session typically provides the overarching set of methods from which the various
* maps encapsulate the functionality of the specific type of map.
* @author Jonathan Groff (c) NeoCoreTechs 2003, 2017, 2021, 2024, 2025
*/
public final class SessionManager {
	private static boolean DEBUG = false;
	private static ConcurrentHashMap<String, Session> SessionTable = new ConcurrentHashMap<String, Session>();
	@SuppressWarnings("rawtypes")
	private static ConcurrentHashMap<?, ?> AdminSessionTable = new ConcurrentHashMap();
	private static Vector<String> OfflineDBs = new Vector<String>();

	//
	// Sets the maximum number users
	@SuppressWarnings("unused")
	private static final int MAX_USERS = -1;
	//
	// Multithreaded double check Singleton setups:
	// 1.) privatized constructor; no other class can call
	private SessionManager() {
	}
	// 2.) volatile instance
	private static volatile SessionManager instance = null;
	// 3.) lock class, assign instance if null
	public static SessionManager getInstance() {
		synchronized(SessionManager.class) {
			if(instance == null) {
				instance = new SessionManager();
			}
		}
		return instance;
	}
	// Global transaction timestamps
	private static long lastStartTime = 0L;
	private static long lastCommitTime = 0L;

	/**
	* Get begin transaction timestamp
	*/
	protected static void getBeginStamp() {
		long bts = System.currentTimeMillis();
		// if < or =, it started at same time (< cause we might have bumped it already)
		if (bts <= lastStartTime)
			++lastStartTime;
		else
			lastStartTime = bts;
		//
	}
	/**
	* @return Current, or end, transaction timestamp
	*/
	protected static long getEndStamp() {
		long ets = System.currentTimeMillis();
		// if < or =, it started at same time (< cause we might have bumped it already)
		if (ets <= lastCommitTime)
			++lastCommitTime;
		else
			lastCommitTime = ets;
		//
		return lastCommitTime;
	}

	/**
	 * Open the database and extract the ColumnFamily that represents the derivedClassName
	 * @param dbname
	 * @param options RocksDb.listColumnFamilies options
	 * @param derivedClassName
	 * @return The {@link Session} that contains the methods to be invoked with ColumnFamilyHandle once we extract it from DB params
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized Session Connect(String dbname, Options options, String derivedClassName) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to column family database:%s with options:%s derived class:%s%n", dbname, options, derivedClassName);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		Session hps = (Session) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamily(dbname,options,derivedClassName);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}
	/**
	 * Open the database and extract the ColumnFamily that represents the default column family for main class
	 * @param dbname
	 * @param options RocksDb.listColumnFamilies options
	 * @return The {@link Session} that contains the methods to be invoked with ColumnFamilyHandle once we extract it from DB params
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized Session Connect(String dbname, Options options) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to column family database:%s with options:%s%n", dbname, options);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		Session hps = (Session) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamily(dbname,options);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}

	/**
	 * Connect to a transaction database column family for a default ColumnFamily class being stored in that database.
	 * @param dbname the path to the database
	 * @param options the RocksDb options RocksDb.listColumnFamilies options
	 * @return the {@link TransactionSession} that contains methods we call using ColumnFamilyHandle
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized TransactionSession ConnectTransaction(String dbname, Options options) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to transaction database:%s with options:%s%n", dbname, options);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		TransactionSession hps = (TransactionSession) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamilyTransaction(dbname,options);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}
	/**
	 * Connect to a transaction database column family for a default ColumnFamily class being stored in that database with associated transaction timeout.
	 * @param dbname the path to the database
	 * @param options the RocksDb options RocksDb.listColumnFamilies options
	 * @param timeout the transaction timeout
	 * @return the {@link TransactionSession} that contains methods we call using ColumnFamilyHandle
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized TransactionSession ConnectTransaction(String dbname, Options options, long timeout) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to transaction database:%s with options:%s%n", dbname, options);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		TransactionSession hps = (TransactionSession) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamilyTransaction(dbname,options,timeout);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}
	/**
	 * Connect to a transaction database column family for a default ColumnFamily class being stored in that database.
	 * @param alias the database alias
	 * @param dbname the path to the database
	 * @param options the RocksDb options RocksDb.listColumnFamilies options
	 * @return the {@link TransactionSession} that contains methods we call using ColumnFamilyHandle
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized TransactionSession ConnectTransaction(Alias alias, String dbname, Options options) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to transaction database:%s with options:%s%n", dbname, options);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		TransactionSession hps = (TransactionSession) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamilyTransaction(alias,dbname,options);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}
	/**
	 * Connect to a transaction database column family for a default ColumnFamily class being stored in that database with associated transaction timeout.
	 * @param alias the database alias
	 * @param dbname the path to the database
	 * @param options the RocksDb options RocksDb.listColumnFamilies options
	 * @param timeout the transaction timeout
	 * @return the {@link TransactionSession} that contains methods we call using ColumnFamilyHandle
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized TransactionSession ConnectTransaction(Alias alias, String dbname, Options options, long timeout) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to transaction database:%s with options:%s%n", dbname, options);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		TransactionSession hps = (TransactionSession) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamilyTransaction(alias, dbname, options, timeout);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}
	/**
	 * Connect to a transaction database column family for a derived class being stored in that database.
	 * @param alias the database alias
	 * @param dbname the path to the database
	 * @param options the RocksDb options RocksDb.listColumnFamilies options
	 * @param derivedClassName the derived class that will contain the ColumnFamily of the same name
	 * @return the {@link TransactionSession} that contains methods we call using ColumnFamilyHandle
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized TransactionSession ConnectTransaction(Alias alias, String dbname, Options options, String derivedClassName) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to transaction database:%s with options:%s%n", dbname, options);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		TransactionSession hps = (TransactionSession) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamilyTransaction(alias,dbname,options,derivedClassName);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}
	/**
	 * Connect to a transaction database column family for a derived class being stored in that database with associated transaction timeout.
	 * @param alias the database alias
	 * @param dbname the path to the database
	 * @param options the RocksDb options RocksDb.listColumnFamilies options
	 * @param derivedClassName the derived class that will contain the ColumnFamily of the same name
	 * @param timeout transaction timeout
	 * @return the {@link TransactionSession} that contains methods we call using ColumnFamilyHandle
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized TransactionSession ConnectTransaction(Alias alias, String dbname, Options options, String derivedClassName, long timeout) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to transaction database:%s with options:%s%n", dbname, options);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		TransactionSession hps = (TransactionSession) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamilyTransaction(alias, dbname, options, derivedClassName, timeout);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}
	/**
	 * Connect to a transaction database column family for a derived class being stored in that database.
	 * @param dbname the path to the database
	 * @param options the RocksDb options RocksDb.listColumnFamilies options
	 * @param derivedClassName the derived class that will contain the ColumnFamily of the same name
	 * @return the {@link TransactionSession} that contains methods we call using ColumnFamilyHandle
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized TransactionSession ConnectTransaction(String dbname, Options options, String derivedClassName) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to transaction database:%s with options:%s%n", dbname, options);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		TransactionSession hps = (TransactionSession) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamilyTransaction(dbname,options,derivedClassName);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}
	/**
	 * Connect to a transaction database column family for a derived class being stored in that database with associated transaction timeout.
	 * @param dbname the path to the database
	 * @param options the RocksDb options RocksDb.listColumnFamilies options
	 * @param derivedClassName the derived class that will contain the ColumnFamily of the same name
	 * @param timeout the transaction timeout
	 * @return the {@link TransactionSession} that contains methods we call using ColumnFamilyHandle
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized TransactionSession ConnectTransaction(String dbname, Options options, String derivedClassName, long timeout) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to transaction database:%s with options:%s%n", dbname, options);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		TransactionSession hps = (TransactionSession) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamilyTransaction(dbname, options, derivedClassName, timeout);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}
	
	/**
	 * Connect to an optimistic transaction database column family for a default ColumnFamily class being stored in that database.
	 * @param dbname the path to the database
	 * @param options the RocksDb options RocksDb.listColumnFamilies options
	 * @return the {@link TransactionSession} that contains methods we call using ColumnFamilyHandle
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized TransactionSession ConnectOptimisticTransaction(String dbname, Options options) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to optimistic transaction database:%s with options:%s%n", dbname, options);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		TransactionSession hps = (TransactionSession) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamilyOptimisticTransaction(dbname,options);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New optimistic session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}
	/**
	 * Connect to an optimistic transaction database column family for a default ColumnFamily class being stored in that database.
	 * @param dbname the path to the database
	 * @param options the RocksDb options RocksDb.listColumnFamilies options
	 * @return the {@link TransactionSession} that contains methods we call using ColumnFamilyHandle
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized TransactionSession ConnectOptimisticTransaction(Alias alias, String dbname, Options options) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to optimistic transaction database:%s with options:%s%n", dbname, options);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		TransactionSession hps = (TransactionSession) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamilyOptimisticTransaction(alias,dbname,options);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New optimistic session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}
	/**
	 * Connect to an optimistic transaction database column family for a derived class being stored in that database.
	 * @param dbname the path to the database
	 * @param options the RocksDb options RocksDb.listColumnFamilies options
	 * @param derivedClassName the derived class that will contain the ColumnFamily of the same name
	 * @return the {@link TransactionSession} that contains methods we call using ColumnFamilyHandle
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized TransactionSession ConnectOptimisticTransaction(Alias alias, String dbname, Options options, String derivedClassName) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to optimistic transaction database:%s with options:%s%n", dbname, options);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		TransactionSession hps = (TransactionSession) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamilyOptimisticTransaction(alias,dbname,options,derivedClassName);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New optimistic session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}
	/**
	 * Connect to an optimistic transaction database column family for a derived class being stored in that database.
	 * @param dbname the path to the database
	 * @param options the RocksDb options RocksDb.listColumnFamilies options
	 * @param derivedClassName the derived class that will contain the ColumnFamily of the same name
	 * @return the {@link TransactionSession} that contains methods we call using ColumnFamilyHandle
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized TransactionSession ConnectOptimisticTransaction(String dbname, Options options, String derivedClassName) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to optimistic transaction database:%s with options:%s%n", dbname, options);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		TransactionSession hps = (TransactionSession) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew
			hps = OpenDBColumnFamilyOptimisticTransaction(dbname,options,derivedClassName);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New optimistic session for db:%s session:%s kvmain:%s %n",dbname,hps,dbname);
		}
		return hps;
	}
	/**
	 * Start the DB with no logging for debugging purposes
	 * or to run read only without logging for some reason
	 * @param dbname the path to the database (path+dbname)
	 * @param options db options RocksDb.listColumnFamilies options
	 * @return The {@link Session}
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized Session ConnectNoRecovery(String dbname, Options options) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.println("Connecting WITHOUT RECOVERY to "+dbname);
		}
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		Session hps = (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew, throws IllegalAccessException if no go
			// Global IO and main KeyValue implementation
			if( DEBUG )
				System.out.println("SessionManager.ConectNoRecovery bringing up IO");
			hps = OpenDBColumnFamily(dbname, options);
			if( DEBUG )
				System.out.println("SessionManager.ConectNoRecovery bringing up session");
			if( DEBUG )
				System.out.println("SessionManager.ConectNoRecovery logging session");
			SessionTable.put(dbname, hps);
		} 
		//
		return hps;
	}
	/**
	 * Call the RocksDB open for the given path and options
	 * @param dbPath
	 * @param options
	 * @return
	 */
	private static RocksDB OpenDB(String dbPath, Options options) {
		RocksDB db = null;
		  try {	  
			  db = RocksDB.open(options, dbPath);
			  if(DEBUG) {
				  final String str = db.getProperty("rocksdb.stats");
				  System.out.println(str);
			  }
		  } catch (final RocksDBException e) {
			    System.out.format("[ERROR] caught the unexpected exception -- %s\n", e);
			    assert (false);
		  }
		  return db;
	}
	/**
	 * Open the database for a given path and options and extract the ColumnFamily of the main classe stored there
	 * @param dbPath
	 * @param options RocksDb.listColumnFamilies options
	 * @return the {@link Session} that contains the method calls to RocksDb
	 */
	private static Session OpenDBColumnFamily(String dbPath, Options options) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDefaultColumnFamilyDescriptors(dbPath, options);
	    RocksDB db;
		try {
			db = RocksDB.open(DatabaseManager.getDefaultDBOptions(), dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		if(DEBUG)
			System.out.printf("SessionManager.OpenDBColumnFamily Session return for db:%s default columnfamily%n",dbPath);
	    return new Session(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles);
	}
	/**
	 * Open the database for a given path and options and extract the ColumnFamily of the derived classes stored there
	 * @param dbPath
	 * @param options RocksDb.listColumnFamilies options
	 * @param derivedClassName
	 * @return the {@link Session} that contains the method calls to RocksDb
	 */
	private static Session OpenDBColumnFamily(String dbPath, Options options, String derivedClassName) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDerivedColumnFamilyDescriptors(dbPath, options, derivedClassName);
	    RocksDB db;
		try {
			db = RocksDB.open(DatabaseManager.getDefaultDBOptions(), dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		if(DEBUG)
			System.out.printf("SessionManager.OpenDBColumnFamily Session return with derived for db:%s derivedClass:%s%n",dbPath,derivedClassName);
	    return new Session(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles);
	}
	/**
	 * Open the transaction database for a given path and options and extract the default ColumnFamily of the classes stored there
	 * @param dbPath
	 * @param options RocksDb.listColumnFamilies options
	 * @return the {@link TransactionSession} that contains the method calls to TransactionDb
	 */
	private static TransactionSession OpenDBColumnFamilyTransaction(String dbPath, Options options) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDefaultColumnFamilyDescriptors(dbPath, options);
	    TransactionDB db;
	    TransactionDBOptions tDbo = new TransactionDBOptions();
		try {
			db = (TransactionDB) TransactionDB.open(DatabaseManager.getDefaultDBOptions(), tDbo, dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	    return new TransactionSession(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles);
	}
	/**
	 * Open the transaction database for a given path and options and extract the default ColumnFamily of the classes stored there
	 * @param dbPath
	 * @param options RocksDb.listColumnFamilies options
	 * @param timeout the lock timeout
	 * @return the {@link TransactionSession} that contains the method calls to TransactionDb
	 */
	private static TransactionSession OpenDBColumnFamilyTransaction(String dbPath, Options options, long timeout) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDefaultColumnFamilyDescriptors(dbPath, options);
	    TransactionDB db;
	    TransactionDBOptions tDbo = new TransactionDBOptions();
	    tDbo.setTransactionLockTimeout(timeout);
		try {
			db = (TransactionDB) TransactionDB.open(DatabaseManager.getDefaultDBOptions(), tDbo, dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	    return new TransactionSession(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles);
	}
	/**
	 * Open the transaction database for a given path and options and extract the ColumnFamily of the derived classes stored there
	 * @param dbPath
	 * @param options RocksDb.listColumnFamilies options
	 * @param derivedClassName
	 * @return the {@link TransactionSession} that contains the method calls to TransactionDb
	 */
	private static TransactionSession OpenDBColumnFamilyTransaction(String dbPath, Options options, String derivedClassName) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDerivedColumnFamilyDescriptors(dbPath, options, derivedClassName);
	    TransactionDB db;
	    TransactionDBOptions tDbo = new TransactionDBOptions();
		try {
			db = (TransactionDB) TransactionDB.open(DatabaseManager.getDefaultDBOptions(), tDbo, dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		if(DEBUG)
			System.out.printf("SessionManager.OpenDBColumnFamilyTransaction Session return with derived for db:%s derivedClass:%s%n",dbPath,derivedClassName);
	    return new TransactionSession(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles);
	}
	/**
	 * Open the transaction database for a given path and options and extract the ColumnFamily of the derived classes stored there
	 * with optional lock timeout differing from the default 1000ms
	 * @param dbPath
	 * @param options RocksDb.listColumnFamilies options
	 * @param derivedClassName
	 * @param the lock timeout in millis.
	 * @return the {@link TransactionSession} that contains the method calls to TransactionDb
	 */
	private static TransactionSession OpenDBColumnFamilyTransaction(String dbPath, Options options, String derivedClassName, long timeout) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDerivedColumnFamilyDescriptors(dbPath, options, derivedClassName);
	    TransactionDB db;
	    TransactionDBOptions tDbo = new TransactionDBOptions();
	    tDbo.setTransactionLockTimeout(timeout);
		try {
			db = (TransactionDB) TransactionDB.open(DatabaseManager.getDefaultDBOptions(), tDbo, dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		if(DEBUG)
			System.out.printf("SessionManager.OpenDBColumnFamilyTransaction Session return with derived for db:%s derivedClass:%s%n",dbPath,derivedClassName);
	    return new TransactionSession(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles);
	}

	/**
	 * Open the transaction database for a given path and options and extract the ColumnFamily of the derived classes stored there
	 * @param alias The database alias
	 * @param dbPath
	 * @param options RocksDb.listColumnFamilies options
	 * @param derivedClassName
	 * @return the {@link TransactionSessionAlias} that contains the method calls to TransactionDb
	 */
	private static TransactionSession OpenDBColumnFamilyTransaction(Alias alias, String dbPath, Options options, String derivedClassName) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDerivedColumnFamilyDescriptors(dbPath, options, derivedClassName);
	    TransactionDB db;
	    TransactionDBOptions tDbo = new TransactionDBOptions();
		try {
			db = (TransactionDB) TransactionDB.open(DatabaseManager.getDefaultDBOptions(), tDbo, dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		if(DEBUG)
			System.out.printf("SessionManager.OpenDBColumnFamilyTransaction Session return with derived for db:%s derivedClass:%s%n",dbPath,derivedClassName);
	    return new TransactionSessionAlias(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles, alias);
	}
	/**
	 * Open the transaction database for a given path and options and extract the ColumnFamily of the derived classes stored there
	 * with optional lock timeout differing from the default 1000ms
	 * @param alias The database alias
	 * @param dbPath
	 * @param options RocksDb.listColumnFamilies options
	 * @param derivedClassName
	 * @param timeout the transaction timeout in millis.
	 * @return the {@link TransactionSessionAlias} that contains the method calls to TransactionDb
	 */
	private static TransactionSession OpenDBColumnFamilyTransaction(Alias alias, String dbPath, Options options, String derivedClassName, long timeout) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDerivedColumnFamilyDescriptors(dbPath, options, derivedClassName);
	    TransactionDB db;
	    TransactionDBOptions tDbo = new TransactionDBOptions();
	    tDbo.setTransactionLockTimeout(timeout);
		try {
			db = (TransactionDB) TransactionDB.open(DatabaseManager.getDefaultDBOptions(), tDbo, dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		if(DEBUG)
			System.out.printf("SessionManager.OpenDBColumnFamilyTransaction Session return with derived for db:%s derivedClass:%s%n",dbPath,derivedClassName);
	    return new TransactionSessionAlias(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles, alias);
	}
	/**
	 * Open the transaction database for a given path and options and extract the default ColumnFamily of the classes stored there
	 * @param alias the database alias
	 * @param dbPath the tablespace path
	 * @param optionsthe build column family descriptor options RocksDb.listColumnFamilies options
	 * @return the {@link TransactionSessionAlias} that contains the method calls to TransactionDb
	 */
	private static TransactionSession OpenDBColumnFamilyTransaction(Alias alias, String dbPath, Options options) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDefaultColumnFamilyDescriptors(dbPath, options);
	    TransactionDB db;
	    TransactionDBOptions tDbo = new TransactionDBOptions();
		try {
			db = (TransactionDB) TransactionDB.open(DatabaseManager.getDefaultDBOptions(), tDbo, dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		if(DEBUG)
			System.out.printf("SessionManager.OpenDBColumnFamilyTransaction Session return with derived for db:%s%n",dbPath);
	    return new TransactionSessionAlias(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles, alias);
	}
	/**
	 * Open the transaction database for a given alias, tablespace path and options and extract the default ColumnFamily of the classes stored there
	 * with optional lock timeout differing from the default 1000ms.
	 * @param alias the database alias
	 * @param dbPath the tablespace path
	 * @param options the build column family descriptor options RocksDb.listColumnFamilies options
	 * @param the transaction timeout
	 * @return the {@link TransactionSessionAlias} that contains the method calls to TransactionDb
	 */
	private static TransactionSession OpenDBColumnFamilyTransaction(Alias alias, String dbPath, Options options, long timeout) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDefaultColumnFamilyDescriptors(dbPath, options);
	    TransactionDB db;
	    TransactionDBOptions tDbo = new TransactionDBOptions();
	    tDbo.setTransactionLockTimeout(timeout);
		try {
			db = (TransactionDB) TransactionDB.open(DatabaseManager.getDefaultDBOptions(), tDbo, dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		if(DEBUG)
			System.out.printf("SessionManager.OpenDBColumnFamilyTransaction Session return with derived for db:%s%n",dbPath);
	    return new TransactionSessionAlias(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles, alias);
	}
	/**
	 * Open the transaction database for a given path and options and extract the default ColumnFamily of the classes stored there
	 * @param dbPath
	 * @param options RocksDb.listColumnFamilies options
	 * @return the {@link OptimisticTransactionSession} that contains the method calls to TransactionDb
	 */
	private static OptimisticTransactionSession OpenDBColumnFamilyOptimisticTransaction(String dbPath, Options options) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDefaultColumnFamilyDescriptors(dbPath, options);
	    OptimisticTransactionDB db;
		try {
			db = OptimisticTransactionDB.open(DatabaseManager.getDefaultDBOptions(), dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	    return new OptimisticTransactionSession(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles);
	}
	/**
	 * Open the optimistic transaction database for a given path and options and extract the ColumnFamily of the derived classes stored there
	 * @param dbPath
	 * @param options RocksDb.listColumnFamilies options
	 * @param derivedClassName
	 * @return the {@link OptimisticTransactionSession} that contains the method calls to TransactionDb
	 */
	private static OptimisticTransactionSession OpenDBColumnFamilyOptimisticTransaction(String dbPath, Options options, String derivedClassName) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDerivedColumnFamilyDescriptors(dbPath, options, derivedClassName);
	    OptimisticTransactionDB db;
		try {
			db = OptimisticTransactionDB.open(DatabaseManager.getDefaultDBOptions(), dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		if(DEBUG)
			System.out.printf("SessionManager.OpenDBColumnFamilyOptimisticTransaction Session return with derived for db:%s derivedClass:%s%n",dbPath,derivedClassName);
	    return new OptimisticTransactionSession(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles);
	}

	/**
	 * Open the optimistic transaction database for a given path and options and extract the ColumnFamily of the derived classes stored there
	 * @param alias the database alias
	 * @param dbPath
	 * @param options RocksDb.listColumnFamilies options
	 * @param derivedClassName
	 * @return the {@link OptimisticTransactionSessionAlias} that contains the method calls to TransactionDb
	 */
	private static OptimisticTransactionSessionAlias OpenDBColumnFamilyOptimisticTransaction(Alias alias, String dbPath, Options options, String derivedClassName) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDerivedColumnFamilyDescriptors(dbPath, options, derivedClassName);
	    OptimisticTransactionDB db;
		try {
			db = OptimisticTransactionDB.open(DatabaseManager.getDefaultDBOptions(), dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		if(DEBUG)
			System.out.printf("SessionManager.OpenDBColumnFamilyOptimisticTransaction Session return with derived for db:%s derivedClass:%s%n",dbPath,derivedClassName);
	    return new OptimisticTransactionSessionAlias(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles, alias);
	}
	/**
	 * Open the optimistic transaction database for a given alias and path and options and extract the default ColumnFamily of the classes stored there
	 * @param alias the database alias
	 * @param dbPath database tablespace
	 * @param options RocksDb.listColumnFamilies options
	 * @return the {@link OptimisticTransactionSessionAlias} that contains the method calls to TransactionDb
	 */
	private static OptimisticTransactionSessionAlias OpenDBColumnFamilyOptimisticTransaction(Alias alias, String dbPath, Options options) {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
		List<ColumnFamilyDescriptor> columnFamilyDescriptor = buildDefaultColumnFamilyDescriptors(dbPath, options);
	    OptimisticTransactionDB db;
		try {
			db = OptimisticTransactionDB.open(DatabaseManager.getDefaultDBOptions(), dbPath, columnFamilyDescriptor, columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		if(DEBUG)
			System.out.printf("SessionManager.OpenDBColumnFamilyOptimisticTransaction Session return with derived for db:%s%n",dbPath);
	    return new OptimisticTransactionSessionAlias(db, options, (ArrayList<ColumnFamilyDescriptor>) columnFamilyDescriptor, columnFamilyHandles, alias);
	}
	
	/**
	 * From the provided dbpath and options, build the list of default columnFamilyDescriptors.<p>
	 * We do this by calling the RocksDB.listColumnFamilies method which gives us a list of byte arrays. 
	 * We then construct the ColumnFamilyDescriptor for each returned byte array. We have to search for the default column family
	 * and if not located, we must add it. Our eventual goal is to provide the list by which we can obtain the
	 * columnFamilyHandles using our open call for a particular database.
	 * @param dbPath The database path, which is tablespace plus database name and we append the particular class stored in default column family
	 * @param options The database options
	 * @return columnFamilyDescriptor ArrayList populated with discovered and constructed descriptors
	 */
	private static List<ColumnFamilyDescriptor> buildDefaultColumnFamilyDescriptors(String dbPath, Options options) {
		List<byte[]> allColumnFamilies;
		try {
			allColumnFamilies = RocksDB.listColumnFamilies(options, dbPath);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptor = new ArrayList<ColumnFamilyDescriptor>();
		boolean foundDefault = false;
		String defcn = new String(RocksDB.DEFAULT_COLUMN_FAMILY); //is this necessary?
		//this.session.columnFamilyDescriptor = Arrays.asList(
		//	new ColumnFamilyDescriptor(TransactionDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions), this.columnFamilyDescriptor);
		for(byte[] e : allColumnFamilies) {
			String cn = new String(e);
			if(DEBUG)
				System.out.printf("SessionManager.buildColumnFamilyDescriptors reading column family %s for db:%s%n",cn,dbPath);
			if(cn.equals(defcn)) {
				foundDefault = true;
			}
			ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(e, DatabaseManager.getDefaultColumnFamilyOptions());
			columnFamilyDescriptor.add(cfd);
		}
		if(!foundDefault) {
			if(DEBUG)
				System.out.printf("SessionManager.buildColumnFamilyDescriptors did NOT find %s for db:%s default columnfamily%n",new String(RocksDB.DEFAULT_COLUMN_FAMILY),dbPath);
			// options from main DB open?
			ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, DatabaseManager.getDefaultColumnFamilyOptions());
			columnFamilyDescriptor.add(cfd);
			//cfo.close();
		}
		return columnFamilyDescriptor;
	}
	/**
	 * From the provided dbpath and options, build the list of default and derived columnFamilyDescriptors.<p>
	 * We do this by calling the RocksDB.listColumnFamilies method which gives us a list of byte arrays. 
	 * We then construct the ColumnFamilyDescriptor for each returned byte array. We have to search for the default and derived column families
	 * and if not located, we must add them. Our eventual goal is to provide the list by which we can obtain the
	 * columnFamilyHandles using our open call for a particular database.
	 * @param dbPath The database path, which is tablespace plus database name and we append the particular class stored in default column family
	 * @param options The database options
	 * @return columnFamilyDescriptor ArrayList populated with discovered and constructed descriptors
	 */
	private static List<ColumnFamilyDescriptor> buildDerivedColumnFamilyDescriptors(String dbPath, Options options, String derivedClassName) {
		List<byte[]> allColumnFamilies;
		try {
			allColumnFamilies = RocksDB.listColumnFamilies(options, dbPath);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptor = new ArrayList<ColumnFamilyDescriptor>();
		boolean found = false;
		boolean foundDefault = false;
		String defcn = new String(TransactionDB.DEFAULT_COLUMN_FAMILY); //is this necessary?
		//this.session.columnFamilyDescriptor = Arrays.asList(
		//	new ColumnFamilyDescriptor(TransactionDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions), this.columnFamilyDescriptor);
		for(byte[] e : allColumnFamilies) {
			String cn = new String(e);
			if(DEBUG)
				System.out.printf("SessionManager.buildDerivedColumnFamilyDescriptors reading column family %s for db:%s derivedClass:%s%n",cn,dbPath,derivedClassName);
			if(cn.equals(derivedClassName)) {
					found = true;
			}
			if(cn.equals(defcn)) {
				foundDefault = true;
			}
			ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(e,  DatabaseManager.getDefaultColumnFamilyOptions());
			columnFamilyDescriptor.add(cfd);
		}
		if(!foundDefault) {
			if(DEBUG)
				System.out.printf("SessionManager.buildDerivedColumnFamilyDescriptors did NOT find %s for db:%s derivedClass:%s%n",TransactionDB.DEFAULT_COLUMN_FAMILY,dbPath,derivedClassName);
			// options from main DB open?
			ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(TransactionDB.DEFAULT_COLUMN_FAMILY, DatabaseManager.getDefaultColumnFamilyOptions());
			columnFamilyDescriptor.add(cfd);
		}
		return columnFamilyDescriptor;
	}
	/**
	* Set the database offline, kill all sessions using it
	* @param dbname The database to offline
	* @exception IOException if we can't force the close
	*/
	protected static synchronized void setDBOffline(String dbname) throws IOException {
		OfflineDBs.addElement(dbname);
		// look for session instance, then signal close
		Session hps = (SessionTable.get(dbname));
		if (hps != null) {
			hps.Close();
		}
	}
	protected static synchronized void setDBOnline(String dbname) {
		OfflineDBs.removeElement(dbname);
	}
	public static synchronized boolean isDBOffline(String dbname) {
		return OfflineDBs.contains(dbname);
	}
	protected static synchronized void releaseSession(TransactionInterface DS) {
		SessionTable.remove(DS);
	}

	public static ConcurrentHashMap<String, Session> getSessionTable() {
		return SessionTable;
	}
	
	/**
	 * For those that wish to maintain admin tables like {@code getSessionTable()}
	 * @return The Hashtable of Admin sessions - you define
	 */
	protected static ConcurrentHashMap<?, ?> getAdminSessionTable() {
		return AdminSessionTable;
	}
	/**
	 * Open the Rocks TransactionDB
	 * @param dbPath
	 * @param options
	 * @return
	 */
	private static TransactionDB OpenTransactionDB(String dbPath, Options options) {
	    final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
	    TransactionDB txnDb = null;
		try {
			txnDb = TransactionDB.open(options, txnDbOptions, dbPath);
		} catch (RocksDBException e) {
		    System.out.format("[ERROR] caught the unexpected exception -- %s\n", e);
		}
	    return txnDb;
	}
/**
 * Demonstrates "Read Committed" isolation
 *           try (final WriteOptions writeOptions = new WriteOptions();
 *	               final ReadOptions readOptions = new ReadOptions()) {
 *
 *	            ////////////////////////////////////////////////////////
 *	            //
 *	            // Simple Transaction Example ("Read Committed")
 *	            //
 *	            ////////////////////////////////////////////////////////
 *	            readCommitted(txnDb, writeOptions, readOptions);
 *			}
private static void readCommitted(final TransactionDB txnDb,
    final WriteOptions writeOptions, final ReadOptions readOptions)
    throws RocksDBException {
  final byte key1[] = "abc".getBytes();
  final byte value1[] = "def".getBytes();

  final byte key2[] = "xyz".getBytes();
  final byte value2[] = "zzz".getBytes();

  // Start a transaction
  try(final Transaction txn = txnDb.beginTransaction(writeOptions)) {
    // Read a key in this transaction
    byte[] value = txn.get(readOptions, key1);
    assert(value == null);

    // Write a key in this transaction
    txn.put(key1, value1);

    // Read a key OUTSIDE this transaction. Does not affect txn.
    value = txnDb.get(readOptions, key1);
    assert(value == null);

    // Write a key OUTSIDE of this transaction.
    // Does not affect txn since this is an unrelated key.
    // If we wrote key 'abc' here, the transaction would fail to commit.
    txnDb.put(writeOptions, key2, value2);

    // Commit transaction
    txn.commit();
  }
}

 * Demonstrates "Repeatable Read" (Snapshot Isolation) isolation
 *           try (final WriteOptions writeOptions = new WriteOptions();
 *	               final ReadOptions readOptions = new ReadOptions()) {
 * 	           ////////////////////////////////////////////////////////
 *	            //
 *	            // "Repeatable Read" (Snapshot Isolation) Example
 *	            //   -- Using a single Snapshot
 *	            //
 *	            ////////////////////////////////////////////////////////
 *	            repeatableRead(txnDb, writeOptions, readOptions);
 *			}
private static void repeatableRead(final TransactionDB txnDb,
    final WriteOptions writeOptions, final ReadOptions readOptions)
    throws RocksDBException {

  final byte key1[] = "ghi".getBytes();
  final byte value1[] = "jkl".getBytes();

  // Set a snapshot at start of transaction by setting setSnapshot(true)
  try(final TransactionOptions txnOptions = new TransactionOptions()
        .setSetSnapshot(true);
      final Transaction txn =
          txnDb.beginTransaction(writeOptions, txnOptions)) {

    final Snapshot snapshot = txn.getSnapshot();

    // Write a key OUTSIDE of transaction
    txnDb.put(writeOptions, key1, value1);

    // Attempt to read a key using the snapshot.  This will fail since
    // the previous write outside this txn conflicts with this read.
    readOptions.setSnapshot(snapshot);

    try {
      final byte[] value = txn.getForUpdate(readOptions, key1, true);
      throw new IllegalStateException();
    } catch(final RocksDBException e) {
      //assert(e.getStatus().getCode() == Status.Code.Busy);
    }

    txn.rollback();
  } finally {
    // Clear snapshot from read options since it is no longer valid
    readOptions.setSnapshot(null);
  }
}

 * Demonstrates "Read Committed" (Monotonic Atomic Views) isolation
 *           try (final WriteOptions writeOptions = new WriteOptions();
 *	               final ReadOptions readOptions = new ReadOptions()) {
 *             ////////////////////////////////////////////////////////
 *	            //
 *	            // "Read Committed" (Monotonic Atomic Views) Example
 *	            //   --Using multiple Snapshots
 *	            //
 *	            ////////////////////////////////////////////////////////
 *	            readCommitted_monotonicAtomicViews(txnDb, writeOptions, readOptions);
 *			}
 * In this example, we set the snapshot multiple times.  This is probably
 * only necessary if you have very strict isolation requirements to
 * implement.
 
private static void readCommitted_monotonicAtomicViews(
    final TransactionDB txnDb, final WriteOptions writeOptions,
    final ReadOptions readOptions) throws RocksDBException {

  final byte keyX[] = "x".getBytes();
  final byte valueX[] = "x".getBytes();

  final byte keyY[] = "y".getBytes();
  final byte valueY[] = "y".getBytes();

  try (final TransactionOptions txnOptions = new TransactionOptions()
      .setSetSnapshot(true);
       final Transaction txn =
           txnDb.beginTransaction(writeOptions, txnOptions)) {

    // Do some reads and writes to key "x"
    Snapshot snapshot = txnDb.getSnapshot();
    readOptions.setSnapshot(snapshot);
    byte[] value = txn.get(readOptions, keyX);
    txn.put(valueX, valueX);

    // Do a write outside of the transaction to key "y"
    txnDb.put(writeOptions, keyY, valueY);

    // Set a new snapshot in the transaction
    txn.setSnapshot();
    txn.setSavePoint();
    snapshot = txnDb.getSnapshot();
    readOptions.setSnapshot(snapshot);

    // Do some reads and writes to key "y"
    // Since the snapshot was advanced, the write done outside of the
    // transaction does not conflict.
    value = txn.getForUpdate(readOptions, keyY, true);
    txn.put(keyY, valueY);

    // Decide we want to revert the last write from this transaction.
    txn.rollbackToSavePoint();

    // Commit.
    txn.commit();
  } finally {
    // Clear snapshot from read options since it is no longer valid
    readOptions.setSnapshot(null);
  }
}
*/
}
