package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Filter;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.HashSkipListMemTableConfig;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.Snapshot;
import org.rocksdb.Statistics;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.TransactionOptions;
import org.rocksdb.VectorMemTableConfig;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

import com.neocoretechs.rocksack.SerializedComparator;

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
* that accepts connections and returns a RockSackSession object. A table of one to one sessions and
* tables is maintained. Typically the RockSackAdapter will create the proper instance the map based on
* desired transaction level or none, and the mep then issues a call here to establish a session.<p/>
* The session openes the database or passes an already opened database to a new transaction map which
* creates a new transaction context for that map.
* @author Jonathan Groff (c) NeoCoreTechs 2003, 2017, 2021
*/
public final class SessionManager {
	private static boolean DEBUG = false;
	private static ConcurrentHashMap<String, RockSackSession> SessionTable = new ConcurrentHashMap<String, RockSackSession>();
	@SuppressWarnings("rawtypes")
	private static ConcurrentHashMap<?, ?> AdminSessionTable = new ConcurrentHashMap();
	private static Vector<String> OfflineDBs = new Vector<String>();
	private static String backingStoreType;
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
	* Connect and return Session instance that is the session.
	* @param dbname The database name as full path
	* @param keystoreType "HMap", "BTree" etc.
	* @param backingstoreType The type of filesystem of memory map "File" "MMap" etc.
	* @return RockSackSession The session we use to control access
	* @exception IOException If low level IO problem
	* @exception IllegalAccessException If access to database is denied
	*/
	public static synchronized RockSackSession Connect(String dbname, String keystoreType, String backingstoreType) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to database:%s with key store:%s and backing store:%s%n", dbname, keystoreType, backingstoreType);
		}
		backingStoreType = backingstoreType;
		// translate user name to uid and group
		// we can restrict access at database level here possibly
		int uid = 0;
		int gid = 1;
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		RockSackSession hps = (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew, throws IllegalAccessException if no go.
			// Global IO and main Key/Value index
			Options o = new Options();
			setOptions(o);
			RocksDB db = OpenDB(dbname, o);
			hps = new RockSackSession(db, o, uid, gid);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New session for db:%s session:%s kvmain:%s %n",dbname,hps,db);
		}
		return hps;
	}
	
	/**
	* Connect and return Session instance that is the transaction session.
	* @param dbname The database name as full path
	* @param keystoreType "RocksDB", "BTree" etc.
	* @param backingstoreType The type of filesystem of memory map "File" "MMap" etc.
	* @return RockSackSession The session we use to control access
	* @exception IOException If low level IO problem
	* @exception IllegalAccessException If access to database is denied
	*/
	public static synchronized RockSackTransactionSession ConnectTransaction(String dbname, String keystoreType, String backingstoreType) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.printf("Connecting to transaction database:%s with key store:%s and backing store:%s%n", dbname, keystoreType, backingstoreType);
		}
		backingStoreType = backingstoreType;
		// translate user name to uid and group
		// we can restrict access at database level here possibly
		int uid = 0;
		int gid = 1;
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		RockSackTransactionSession hps = (RockSackTransactionSession) (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew, throws IllegalAccessException if no go.
			// Global IO and main Key/Value index
			Options o = new Options();
			setOptions(o);
			TransactionDB db = OpenTransactionDB(dbname,o);
			hps = new RockSackTransactionSession(db, o, uid, gid);
			SessionTable.put(dbname, hps);
			if( DEBUG )
				System.out.printf("New session for db:%s session:%s kvmain:%s %n",dbname,hps,db);
		}
		return hps;
	}
	private static void setOptions(Options options) {
		//RocksDB db = null;
		//final String db_path = dbpath;
		//final String db_path_not_found = db_path + "_not_found";

		final Filter bloomFilter = new BloomFilter(10);
		//final ReadOptions readOptions = new ReadOptions().setFillCache(false);
		final Statistics stats = new Statistics();
		final RateLimiter rateLimiter = new RateLimiter(10000000,10000, 10);
		options.setComparator(new SerializedComparator());
		try {
		    options.setCreateIfMissing(true)
		        .setStatistics(stats)
		        .setWriteBufferSize(8 * SizeUnit.KB)
		        .setMaxWriteBufferNumber(3)
		        .setMaxBackgroundJobs(10)
		        .setCompressionType(CompressionType.ZLIB_COMPRESSION)
		        .setCompactionStyle(CompactionStyle.UNIVERSAL);
		 } catch (final IllegalArgumentException e) {
		    assert (false);
		 }
		  assert (options.createIfMissing() == true);
		  assert (options.writeBufferSize() == 8 * SizeUnit.KB);
		  assert (options.maxWriteBufferNumber() == 3);
		  assert (options.maxBackgroundJobs() == 10);
		  assert (options.compressionType() == CompressionType.ZLIB_COMPRESSION);
		  assert (options.compactionStyle() == CompactionStyle.UNIVERSAL);

		  assert (options.memTableFactoryName().equals("SkipListFactory"));
		  options.setMemTableConfig(
		      new HashSkipListMemTableConfig()
		          .setHeight(4)
		          .setBranchingFactor(4)
		          .setBucketCount(2000000));
		  assert (options.memTableFactoryName().equals("HashSkipListRepFactory"));

		  options.setMemTableConfig(
		      new HashLinkedListMemTableConfig()
		          .setBucketCount(100000));
		  assert (options.memTableFactoryName().equals("HashLinkedListRepFactory"));

		  options.setMemTableConfig(
		      new VectorMemTableConfig().setReservedSize(10000));
		  assert (options.memTableFactoryName().equals("VectorRepFactory"));

		  options.setMemTableConfig(new SkipListMemTableConfig());
		  assert (options.memTableFactoryName().equals("SkipListFactory"));

		  options.setTableFormatConfig(new PlainTableConfig());
		  // Plain-Table requires mmap read
		  options.setAllowMmapReads(true);
		  assert (options.tableFactoryName().equals("PlainTable"));

		  options.setRateLimiter(rateLimiter);

		  final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
		  Cache cache = new LRUCache(64 * 1024, 6);
		  table_options.setBlockCache(cache)
		      .setFilterPolicy(bloomFilter)
		      .setBlockSizeDeviation(5)
		      .setBlockRestartInterval(10)
		      .setCacheIndexAndFilterBlocks(true)
		      .setBlockCacheCompressed(new LRUCache(64 * 1000, 10));

		  assert (table_options.blockSizeDeviation() == 5);
		  assert (table_options.blockRestartInterval() == 10);
		  assert (table_options.cacheIndexAndFilterBlocks() == true);

		  options.setTableFormatConfig(table_options);
		  assert (options.tableFactoryName().equals("BlockBasedTable"));
	}
	
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
	 * Start the DB with no logging for debugging purposes
	 * or to run read only without logging for some reason
	 * @param dbname the path to the database (path+dbname)
	 * @param remoteDBName The remote path to database tablespace directories (tablespace prepended to endof path) or null
	 * @return
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public static synchronized RockSackSession ConnectNoRecovery(String dbname, String keystoreType, String backingstoreType, int poolBlocks) throws IOException, IllegalAccessException {
		if( DEBUG ) {
			System.out.println("Connecting WITHOUT RECOVERY to "+dbname);
		}
		// translate user name to uid and group
		// we can restrict access at database level here possibly
		int uid = 0;
		int gid = 1;
		//if( SessionTable.size() >= MAX_USERS && MAX_USERS != -1) throw new IllegalAccessException("Maximum number of users exceeded");
		if (OfflineDBs.contains(dbname))
			throw new IllegalAccessException("Database is offline, try later");
		RockSackSession hps = (SessionTable.get(dbname));
		if (hps == null) {
			// did'nt find it, create anew, throws IllegalAccessException if no go
			// Global IO and main KeyValue implementation
			if( DEBUG )
				System.out.println("SessionManager.ConectNoRecovery bringing up IO");
			Options o = new Options();
			setOptions(o);
			RocksDB db = OpenDB(dbname, o);
			hps = new RockSackSession(db, o, uid, gid);
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
	* Set the database offline, kill all sessions using it
	* @param dbname The database to offline
	* @exception IOException if we can't force the close
	*/
	protected static synchronized void setDBOffline(String dbname) throws IOException {
		OfflineDBs.addElement(dbname);
		// look for session instance, then signal close
		RockSackSession hps = (SessionTable.get(dbname));
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

	public static ConcurrentHashMap<String, RockSackSession> getSessionTable() {
		return SessionTable;
	}
	
	/**
	 * For those that wish to maintain admin tables
	 * @return The Hashtable of Admin sessions - you define
	 */
	protected static ConcurrentHashMap<?, ?> getAdminSessionTable() {
		return AdminSessionTable;
	}

	private static TransactionDB OpenTransactionDB(String dbPath, Options options) {
	    final TransactionDBOptions txnDbOptions = new TransactionDBOptions();
	    TransactionDB txnDb = null;
		try {
			txnDb = TransactionDB.open(options, txnDbOptions, dbPath);
		} catch (RocksDBException e) {
		    System.out.format("[ERROR] caught the unexpected exception -- %s\n", e);
		}
	    return txnDb;
	    	/*
	          try (final WriteOptions writeOptions = new WriteOptions();
	               final ReadOptions readOptions = new ReadOptions()) {

	            ////////////////////////////////////////////////////////
	            //
	            // Simple Transaction Example ("Read Committed")
	            //
	            ////////////////////////////////////////////////////////
	            readCommitted(txnDb, writeOptions, readOptions);


	            ////////////////////////////////////////////////////////
	            //
	            // "Repeatable Read" (Snapshot Isolation) Example
	            //   -- Using a single Snapshot
	            //
	            ////////////////////////////////////////////////////////
	            repeatableRead(txnDb, writeOptions, readOptions);


	            ////////////////////////////////////////////////////////
	            //
	            // "Read Committed" (Monotonic Atomic Views) Example
	            //   --Using multiple Snapshots
	            //
	            ////////////////////////////////////////////////////////
	            readCommitted_monotonicAtomicViews(txnDb, writeOptions, readOptions);
	          }
	          */
	}
/**
 * Demonstrates "Read Committed" isolation
 */
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
/**
 * Demonstrates "Repeatable Read" (Snapshot Isolation) isolation
 */
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

/**
 * Demonstrates "Read Committed" (Monotonic Atomic Views) isolation
 *
 * In this example, we set the snapshot multiple times.  This is probably
 * only necessary if you have very strict isolation requirements to
 * implement.
 */
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

}
