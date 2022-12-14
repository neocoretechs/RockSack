package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;
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
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.Statistics;
import org.rocksdb.Transaction;
import org.rocksdb.VectorMemTableConfig;
import org.rocksdb.util.SizeUnit;

import com.neocoretechs.rocksack.DBPhysicalConstants;
import com.neocoretechs.rocksack.SerializedComparator;

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
	private static Options options = null;
	
	static {
		RocksDB.loadLibrary();
	}
	private static ConcurrentHashMap<String, SetInterface> classToIso = new ConcurrentHashMap<String,SetInterface>();
	private static ConcurrentHashMap<String, ConcurrentHashMap<String,SetInterface>> classToIsoTransaction = new ConcurrentHashMap<String,ConcurrentHashMap<String,SetInterface>>();
	private static ConcurrentHashMap<String, Transaction> idToTransaction = new ConcurrentHashMap<String, Transaction>();
	
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
	
	public static void setDatabaseOptions(Options dboptions) {
		options = dboptions;
	}
	
	private static Options getDefaultOptions() {
		//RocksDB db = null;
		//final String db_path = dbpath;
		//final String db_path_not_found = db_path + "_not_found";
		Options options = new Options();
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
		  return options;
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
			if(options == null)
				options = getDefaultOptions();
			ret =  new BufferedMap(SessionManager.Connect(tableSpaceDir+xClass, options));
			classToIso.put(xClass, ret);
		}
		return ret;
	}
	
	public static String getRockSackTransactionId() {
		return UUID.randomUUID().toString();
	}
	
	public static synchronized void removeRockSackTransaction(String xid) {
		removeRockSackTransactionalMap(xid);
		Transaction t = idToTransaction.get(xid);
		if(t != null) {
			t.close();
			idToTransaction.remove(t);
		}
	}
	
	public static void commitRockSackTransaction(String xid) throws IOException {
		Transaction t = idToTransaction.get(xid);
		if(t != null) {
			try {
				t.commit();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		}
	}
	public static void rollbackRockSackTransaction(String xid) throws IOException {
		Transaction t = idToTransaction.get(xid);
		if(t != null) {
			try {
				t.rollback();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		}
	}
	public static void checkpointRockSackTransaction(String xid) throws IOException {
		Transaction t = idToTransaction.get(xid);
		if(t != null) {
			try {
				t.setSavePoint();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		}
	}
	public static void rollbackToCheckpoint(String xid) throws IOException {
		Transaction t = idToTransaction.get(xid);
		if(t != null) {
			try {
				t.rollbackToSavePoint();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		}
	}
	/**
	 * Start a new transaction for the given class in the current database
	 * @param clazz
	 * @return
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static synchronized TransactionalMap getRockSackTransactionalMap(Class clazz, String xid) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		ConcurrentHashMap<String, SetInterface> xactions = classToIsoTransaction.get(xClass);
		RockSackTransactionSession txn = null;
		if(idToTransaction.containsKey(xid)) {
			if(xactions != null) {
				// transaction exists, transactions for this class exist, does this transaction exist for this class?
				TransactionalMap tm  = (TransactionalMap) xactions.get(xid);
				if(tm != null) {
					if(DEBUG)
						System.out.println(RockSackAdapter.class.getName()+" About to return EXISTING map with EXISTING xid "+xid+" from: "+tableSpaceDir+xClass+" TransactionalMap:"+tm.toString()+" total xactions this class:"+xactions.size()+" total classes:"+classToIsoTransaction.mappingCount());
					return tm;
				} else {
					// transaction exists, but not for this class
					// Get the database session, add the existing transaction
					if(options == null)
						options = getDefaultOptions();
					txn = SessionManager.ConnectTransaction(tableSpaceDir+xClass, options);
					Transaction tx = idToTransaction.get(xid);
					tm = new TransactionalMap(txn, tx);
					xactions.put(tx.getName(), tm);
					if(DEBUG)
						System.out.println(RockSackAdapter.class.getName()+" About to return NEW map with EXISTING xid "+tx.getName()+" from: "+tableSpaceDir+xClass+" TransactionalMap:"+tm.toString()+" total xactions this class:"+xactions.size()+" total classes:"+classToIsoTransaction.mappingCount());
					return tm;
				}
			}
			// transaction exists, but xactions null, nothing for this class
			xactions = new ConcurrentHashMap<String, SetInterface>();
			classToIsoTransaction.put(xClass, xactions);
			// add out new transaction id/transaction map to the collection keyed by class
			Transaction tx = idToTransaction.get(xid);
			if(options == null)
				options = getDefaultOptions();
			txn = SessionManager.ConnectTransaction(tableSpaceDir+xClass, options);
			TransactionalMap tm = new TransactionalMap(txn, tx);
			xactions.put(tx.getName(), tm);
			if(DEBUG)
				System.out.println(RockSackAdapter.class.getName()+" About to return NEW INITIAL map with EXISTING xid "+tx.getName()+" from: "+tableSpaceDir+xClass+" TransactionalMap:"+tm.toString()+" total xactions this class:"+xactions.size()+" total classes:"+classToIsoTransaction.mappingCount());
			return tm;
		}
		// Transaction Id was not present, construct new transaction
		if(options == null)
			options = getDefaultOptions();
		txn = SessionManager.ConnectTransaction(tableSpaceDir+xClass, options);
		Transaction tx = txn.BeginTransaction();
		try {
			tx.setName(xid);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
		idToTransaction.put(tx.getName(), tx);
		TransactionalMap tm = new TransactionalMap(txn, tx);
		// do any transactions exist for this class/db?
		if(xactions == null) {
			xactions = new ConcurrentHashMap<String, SetInterface>();
			classToIsoTransaction.put(xClass, xactions);
		}
		// add out new transaction id/transaction map to the collection keyed by class
		xactions.put(tx.getName(), tm);
		if(DEBUG)
			System.out.println(RockSackAdapter.class.getName()+" About to return NEW map with NEW xid "+tx.getName()+" from: "+tableSpaceDir+xClass+" TransactionalMap:"+tm.toString()+" total xactions this class:"+xactions.size()+" total classes:"+classToIsoTransaction.mappingCount());
		return tm;
	}

	/**
	 * Remove the given TransactionalMap from active DB/transaction collection
	 * @param tmap the TransactionalMap for a given transaction Id
	 */
	public static synchronized  void removeRockSackTransactionalMap(SetInterface tmap) {
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
	 * Remove the given TransactionalMap from active DB/transaction collection
	 * @param xid The Transaction Id
	 */
	public static synchronized void removeRockSackTransactionalMap(String xid) {
		Collection<ConcurrentHashMap<String, SetInterface>> xactions = classToIsoTransaction.values();
		xactions.forEach(c -> {
			c.forEach((k,v) -> {
				if(k.equals(xid)) {
					TransactionalMap verify = (TransactionalMap) c.remove(xid);
					if(DEBUG)
						System.out.println("RockSackAdapter.removeRockSackTransactionalMap removing xaction "+xid+" for DB "+k+" which should match "+verify.txn.getName());
					return;
				}
			});
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
