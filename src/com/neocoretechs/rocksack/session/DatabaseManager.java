package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.AbstractComparator;
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
import com.neocoretechs.rocksack.session.VolumeManager.Volume;

/**
 * This factory class enforces a strong typing for the RockSack using the database naming convention linked to the
 * class name of the class stored there.<p/>
 * In almost all cases, this is the main entry point to obtain a BufferedMap or a TransactionalMap.<p/>
 * To override options call setDatabaseOptions(Options). If options are not set in this manner the default options will be used.
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
public class DatabaseManager {
	private static boolean DEBUG = false;
	private static String tableSpaceDir = "/";
	private static final char[] ILLEGAL_CHARS = { '[', ']', '!', '+', '=', '|', ';', '?', '*', '\\', '<', '>', '|', '\"', ':' };
	private static final char[] OK_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E' };
	private static Options options = null;
	
	static {
		RocksDB.loadLibrary();
		options = getDefaultOptions();
	}
	
	/**
	 * Get the tablespace by given alias
	 * @param alias
	 * @return The path for this alias or null if none
	 */
	public static String getTableSpaceDir(String alias) {
		return VolumeManager.getAliasToPath(alias);
	}
	/**
	 * Get the default tablespace set by explicit previous call.
	 * @return
	 */
	public static String getTableSpaceDir() {
		return tableSpaceDir;
	}
	/**
	 * 
	 * @return The aliases and paths as 2d array, if none first dimension is zero.
	 */
	public static String[][] getAliases() {
		return VolumeManager.getAliases();
	}
	/**
	 * Set the tablespace for a given alias
	 * @param alias
	 * @param tableSpaceDir
	 */
	public static void setTableSpaceDir(String alias, String tableSpaceDir) {
		VolumeManager.createAlias(alias, tableSpaceDir);
	}
	/**
	 * Set the default tablespace for operations not using alias
	 * @param tableSpaceDir
	 */
	public static void setTableSpaceDir(String tableSpaceDir) {
		DatabaseManager.tableSpaceDir = tableSpaceDir;
	}
	/**
	 * Remove the given alias.
	 * @param alias
	 */
	public static void removeAlias(String alias) {
		VolumeManager.removeAlias(alias);
	}
	/**
	 * Remove the given tablespace path.
	 * @param alias
	 */
	public static void remove(String path) {
		VolumeManager.remove(path);
	}
	/**
	 * Get the fully qualified database name using default tablespace and translated class name
	 * @param clazz
	 * @return
	 */
	public static String getDatabaseName(Class clazz) {
		String xClass = translateClass(clazz.getName());
		return tableSpaceDir+xClass;
	}
	/**
	 * Get the fully qualified database name using string version of class and default tablespace.
	 * @param clazz
	 * @return
	 */
	public static String getDatabaseName(String clazz) {
		return tableSpaceDir+clazz;
	}
	/**
	 * Get the tablespace path for the given alias {@link VolumeManager.getAliasToPath}
	 * @param alias the database alias
	 * @return the path for this alias or null if none
	 */
	public static String getAliasToPath(String alias) {
		return VolumeManager.getAliasToPath(alias);
	}
	
	public static List<Transaction> getOutstandingTransactions(String path) {
		return VolumeManager.getOutstandingTransactions(path);
	}
	/**
	 * Return a list all RocksDB transactions with id's mapped to transactions
	 * in the set of active volumes for a particular alias to a path
	 * @param alias the path alias
	 * @return the List of RocksDB Transactions. Use with care.
	 */
	public static List<Transaction> getOutstandingTransactionsAlias(String alias) {
		return VolumeManager.getOutstandingTransactionsAlias(alias);
	}
	
	public static List<Transaction> getOutstandingTransactionsById(String uid) {
		return VolumeManager.getOutstandingTransactionsById(uid);
	}
	
	public static List<Transaction> getOutstandingTransactionsByAliasAndId(String alias, String uid) {
		return VolumeManager.getOutstandingTransactionsByAliasAndId(alias, uid);
	}
	
	public static List<Transaction> getOutstandingTransactionsByPathAndId(String path, String uid) {
		return VolumeManager.getOutstandingTransactionsByPathAndId(path, uid);
	}
	
	/**
	 * Return a list of the state of all transactions with id's mapped to transactions
	 * in the set of active volumes. According to docs states are:
	 * AWAITING_COMMIT AWAITING_PREPARE AWAITING_ROLLBACK COMMITED COMMITTED (?)
	 * LOCKS_STOLEN PREPARED ROLLEDBACK STARTED. In practice, it seems to mainly vary between
	 * STARTED and COMMITTED. The 'COMMITED' state doesnt seem to manifest.
	 * @return
	 */
	public static List<String> getOutstandingTransactionState() {
		return VolumeManager.getOutstandingTransactionState();
	}
	
	/**
	 * Set the RocksDB options for all subsequent databases
	 * @param dboptions
	 */
	public static void setDatabaseOptions(Options dboptions) {
		options = dboptions;
	}
	/**
	 * Get the default options using the RockSack {@link SerializedComparator}
	 * @return the populated RocksDB options instance
	 */
	private static Options getDefaultOptions() {
		return getDefaultOptions(new SerializedComparator());
	}
	/**
	 * Get the default options using a different comparator, primarily to provide hooks inside compare method.
	 * The call sequence would be DatabaseManager.setDatabaseOptions(DatabaseManager.getDefaultOptions(your SerializedComparator))
	 * WARNING: must provide all functionality of RockSack {@link SerializedComparator}
	 * @param comparator the AbstractComparator instance
	 * @return the populated RocksDB options instance
	 */
	public static Options getDefaultOptions(AbstractComparator comparator) {
		//RocksDB db = null;
		//final String db_path = dbpath;
		//final String db_path_not_found = db_path + "_not_found";
		Options options = new Options();
		final Filter bloomFilter = new BloomFilter(10);
		//final ReadOptions readOptions = new ReadOptions().setFillCache(false);
		final Statistics stats = new Statistics();
		//final RateLimiter rateLimiter = new RateLimiter(10000000,10000, 10);
		options.setComparator(comparator);
		try {
		    options.setCreateIfMissing(true)
		        .setStatistics(stats)
		        .setWriteBufferSize(8 * SizeUnit.MB)
		        .setMaxWriteBufferNumber(3)
		        .setMaxBackgroundJobs(24)
		        .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
		        .setCompactionStyle(CompactionStyle.LEVEL);
		 } catch (final IllegalArgumentException e) {
		    //assert (false);
			 bloomFilter.close();
			 throw new RuntimeException(e);
		 }
		  //assert (options.createIfMissing() == true);
		  //assert (options.writeBufferSize() == 8 * SizeUnit.KB);
		  //assert (options.maxWriteBufferNumber() == 3);
		  //assert (options.maxBackgroundJobs() == 10);
		  //assert (options.compressionType() == CompressionType.ZLIB_COMPRESSION);
		  //assert (options.compactionStyle() == CompactionStyle.UNIVERSAL);

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

		  //options.setRateLimiter(rateLimiter);

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
	 * @return A {@link BufferedMap} for the clazz instances.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedMap getMap(Comparable clazz) throws IllegalAccessException, IOException {
		return getMap(clazz.getClass());
	}
	/**
	 * Get a Map via Java Class type.
	 * @param clazz The Java Class of the intended database
	 * @return The {@link BufferedMap} for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedMap getMap(Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		Volume v = VolumeManager.get(tableSpaceDir);
		BufferedMap ret = (BufferedMap) v.classToIso.get(xClass);
		if(DEBUG)
			System.out.println("DatabaseManager.getMap About to return designator for dir:"+tableSpaceDir+" class:"+xClass+" formed from "+clazz.getName()+" for volume:"+v);
		if( ret == null ) {
			//if(options == null)
			//	options = getDefaultOptions();
			ret =  new BufferedMap(SessionManager.Connect(tableSpaceDir+xClass, options));
			if(DEBUG)
				System.out.println("DatabaseManager.getMap About to create new map:"+ret);
			v.classToIso.put(xClass, ret);
		}
		if(DEBUG)
			System.out.println("DatabaseManager.getMap About to return map:"+ret);
		return ret;
	}
	/**
	 * Get a Map via Comparable instance.
	 * @param alias The database alias for tablespace
	 * @param clazz The Comparable object that the java class name is extracted from
	 * @return A {@link BufferedMap} for the clazz instances.
	 * @throws IllegalAccessException
	 * @throws NoSuchElementException if alias was not found
	 * @throws IOException
	 */
	public static BufferedMap getMap(String alias, Comparable clazz) throws IllegalAccessException, IOException, NoSuchElementException {
		return getMap(alias, clazz.getClass());
	}
	/**
	 * Get a Map via Java Class type.
	 * @param alias The database alias for tablespace
	 * @param clazz The Java Class of the intended database
	 * @return The {@link BufferedMap} for the clazz type.
	 * @throws IllegalAccessException
	 * @throws NoSuchElementException if alias was not found
	 * @throws IOException
	 */
	public static BufferedMap getMap(String alias, Class clazz) throws IllegalAccessException, IOException, NoSuchElementException {
		String xClass = translateClass(clazz.getName());
		Volume v = VolumeManager.getByAlias(alias);
		BufferedMap ret = (BufferedMap) v.classToIso.get(xClass);
		if(DEBUG)
			System.out.println("DatabaseManager.getMap About to return designator for alias:"+alias+" class:"+xClass+" formed from "+clazz.getName()+" for volume:"+v);
		if( ret == null ) {
			//if(options == null)
			//	options = getDefaultOptions();
			ret =  new BufferedMap(SessionManager.Connect(VolumeManager.getAliasToPath(alias)+xClass, options));
			if(DEBUG)
				System.out.println("DatabaseManager.getMap About to create new map:"+ret);
			v.classToIso.put(xClass, ret);
		}
		if(DEBUG)
			System.out.println("DatabaseManager.getMap About to return map:"+ret);
		return ret;
	}	
	/**
	 * Get a Map via absolute path and Comparable instance. If the {@link VolumeManager} instance does not exist it will be created
	 * @param path The database path for tablespace
	 * @param clazz The Comparable object that the java class name is extracted from
	 * @return A {@link BufferedMap} for the clazz instances.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedMap getMapByPath(String path, Comparable clazz) throws IllegalAccessException, IOException {
		return getMapByPath(path, clazz.getClass());
	}
	/**
	 * Get a Map via absolute path and Java Class type. If the {@link VolumeManager} instance does not exist it will be created
	 * @param path The database path for tablespace
	 * @param clazz The Java Class of the intended database
	 * @return The {@link BufferedMap} for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedMap getMapByPath(String path, Class clazz) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		Volume v = VolumeManager.get(path);
		BufferedMap ret = (BufferedMap) v.classToIso.get(xClass);
		if(DEBUG)
			System.out.println("DatabaseManager.getMapByPath About to return designator for path:"+path+" class:"+xClass+" formed from "+clazz.getName()+" for volume:"+v);
		if( ret == null ) {
			//if(options == null)
			//	options = getDefaultOptions();
			ret =  new BufferedMap(SessionManager.Connect(path+xClass, options));
			if(DEBUG)
				System.out.println("DatabaseManager.getMapByPath About to create new map:"+ret);
			v.classToIso.put(xClass, ret);
		}
		if(DEBUG)
			System.out.println("DatabaseManager.getMap About to return map:"+ret);
		return ret;
	}
	
	public static String getTransactionId() {
		return UUID.randomUUID().toString();
	}
	
	/**
	 * Remove from classToIso then idToTransaction in {@link VolumeManager}
	 * @param xid
	 * @throws IOException If the transaction is not in a state to be removed. i.e. not COMMITTED, ROLLEDBACK or STARTED
	 */
	public static synchronized void removeTransaction(String xid) throws IOException {
		removeTransactionalMap(xid);
		VolumeManager.removeTransaction(xid);
	}
	/**
	 * Remove from classToIso then idToTransaction in {@link VolumeManager}
	 * @param alias
	 * @param xid
	 * @throws NoSuchElementException if the alias doesnt exist
	 * @throws IOException If the transaction is not in a state to be removed. i.e. not COMMITTED, ROLLEDBACK or STARTED
	 */
	public static synchronized void removeTransaction(String alias, String xid) throws NoSuchElementException, IOException {
		removeTransactionalMap(alias, xid);
		VolumeManager.removeTransaction(xid);
	}
	
	public static void commitTransaction(String xid) throws IOException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByPathAndId(tableSpaceDir, xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx)
					t.commit();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void commitTransaction(String alias, String xid) throws IOException, NoSuchElementException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByAliasAndId(alias, xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx)
					t.commit();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void rollbackTransaction(String xid) throws IOException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByPathAndId(tableSpaceDir, xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx)
					t.rollback();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void rollbackTransaction(String alias, String xid) throws IOException, NoSuchElementException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByAliasAndId(alias, xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx)
					t.rollback();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void checkpointTransaction(String xid) throws IOException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsById(xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx)
					t.setSavePoint();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void checkpointTransaction(String alias, String xid) throws IOException, NoSuchElementException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByAliasAndId(alias, xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx)
					t.setSavePoint();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void rollbackToCheckpoint(String xid) throws IOException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByPathAndId(tableSpaceDir, xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx)
					t.rollbackToSavePoint();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void rollbackToCheckpoint(String alias, String xid) throws IOException, NoSuchElementException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByAliasAndId(alias, xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx)
					t.rollbackToSavePoint();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	/**
	 * Start a new transaction for the given class in the current database
	 * @param clazz
	 * @return
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static synchronized TransactionalMap getTransactionalMap(Comparable clazz, String xid) throws IllegalAccessException, IOException {
		return getTransactionalMap(clazz.getClass(), xid);
	}
	/**
	 * Start a new transaction for the given class in the current database
	 * @param clazz
	 * @return
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static synchronized TransactionalMap getTransactionalMap(Class clazz, String xid) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		Volume v = VolumeManager.get(tableSpaceDir);
		return getTransactionalMap(xid, tableSpaceDir, xClass, v);
	}
	/**
	 * Start a new transaction for the given class in the aliased database
	 * @param alias The alias for the tablespace
	 * @param clazz
	 * @return the TransactionalMap for the alias/class/xid
	 * @throws IllegalAccessException
	 * @throws NoSuchElementException if The alias cant be located
	 * @throws IOException
	 */
	public static synchronized TransactionalMap getTransactionalMap(String alias, Comparable clazz, String xid) throws IllegalAccessException, IOException, NoSuchElementException {
		return getTransactionalMap(alias, clazz.getClass(), xid);
	}
	/**
	 * Start a new transaction for the given class in the aliased database
	 * @param alias The alias for the tablespace
	 * @param clazz
	 * @return the TransactionalMap for the alias/class/xid
	 * @throws IllegalAccessException
	 * @throws NoSuchElementException if The alias cant be located
	 * @throws IOException
	 */
	public static synchronized TransactionalMap getTransactionalMap(String alias, Class clazz, String xid) throws IllegalAccessException, IOException, NoSuchElementException {
		String xClass = translateClass(clazz.getName());
		Volume v = VolumeManager.getByAlias(alias);
		return getTransactionalMap(xid, VolumeManager.getAliasToPath(alias), xClass, v);
	}
	/**
	 * Start a new transaction for the given class in the database absolute path
	 * @param path the database tablespace 
	 * @param clazz
	 * @return the TransactionalMap for the alias/class/xid
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static synchronized TransactionalMap getTransactionalMapByPath(String path, Comparable clazz, String xid) throws IllegalAccessException, IOException {
		return getTransactionalMap(path, clazz.getClass(), xid);
	}
	/**
	 * Start a new transaction for the given class in the database absolute path
	 * @param path The path for the tablespace
	 * @param clazz
	 * @return the TransactionalMap for the alias/class/xid
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static synchronized TransactionalMap getTransactionalMapByPath(String path, Class clazz, String xid) throws IllegalAccessException, IOException {
		String xClass = translateClass(clazz.getName());
		Volume v = VolumeManager.get(path);
		return getTransactionalMap(xid, path, xClass, v);
	}
	/**
	 * Main method to retrieve all permutations of {@link TransactionalMap} from 
	 * transaction id, database tablespace name, translated class, and Volume from {@link VolumeManager}
	 * @param xid The transaction id
	 * @param dbname The database tablespace name
	 * @param xClass the translated class name for tablespace inclusion
	 * @param v the Volume
	 * @return the TransactionalMap
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	private static synchronized TransactionalMap getTransactionalMap(String xid, String dbname, String xClass, Volume v) throws IllegalAccessException, IOException {
		ConcurrentHashMap<String, SetInterface> xactions = v.classToIsoTransaction.get(xClass);
		TransactionSession txn = null;
		if(v.idToTransaction.containsKey(xid)) {
			if(xactions != null) {
				// transaction exists, transactions for this class exist, does this transaction exist for this class?
				TransactionalMap tm  = (TransactionalMap) xactions.get(xid);
				if(tm != null) {
					if(DEBUG)
						System.out.println(DatabaseManager.class.getName()+" About to return EXISTING map with EXISTING xid "+xid+" from: "+dbname+xClass+" TransactionalMap:"+tm.toString()+" total xactions this class:"+xactions.size()+" total classes:"+v.classToIsoTransaction.mappingCount());
					return tm;
				} else {
					// transaction exists, but not for this class
					// Get the database session, add the existing transaction
					if(options == null)
						options = getDefaultOptions();
					txn = SessionManager.ConnectTransaction(dbname+xClass, options);
					Transaction tx = v.idToTransaction.get(xid);
					tm = new TransactionalMap(txn, tx);
					xactions.put(tx.getName(), tm);
					if(DEBUG)
						System.out.println(DatabaseManager.class.getName()+" About to return NEW map with EXISTING xid "+tx.getName()+" from: "+dbname+xClass+" TransactionalMap:"+tm.toString()+" total xactions this class:"+xactions.size()+" total classes:"+v.classToIsoTransaction.mappingCount());
					return tm;
				}
			}
			// transaction exists, but xactions null, nothing for this class
			xactions = new ConcurrentHashMap<String, SetInterface>();
			v.classToIsoTransaction.put(xClass, xactions);
			// add out new transaction id/transaction map to the collection keyed by class
			Transaction tx = v.idToTransaction.get(xid);
			if(options == null)
				options = getDefaultOptions();
			txn = SessionManager.ConnectTransaction(dbname+xClass, options);
			TransactionalMap tm = new TransactionalMap(txn, tx);
			xactions.put(tx.getName(), tm);
			if(DEBUG)
				System.out.println(DatabaseManager.class.getName()+" About to return NEW INITIAL map with EXISTING xid "+tx.getName()+" from: "+dbname+xClass+" TransactionalMap:"+tm.toString()+" total xactions this class:"+xactions.size()+" total classes:"+v.classToIsoTransaction.mappingCount());
			return tm;
		}
		// Transaction Id was not present, construct new transaction
		if(options == null)
			options = getDefaultOptions();
		txn = SessionManager.ConnectTransaction(dbname+xClass, options);
		Transaction tx = txn.BeginTransaction();
		try {
			tx.setName(xid);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
		v.idToTransaction.put(tx.getName(), tx);
		TransactionalMap tm = new TransactionalMap(txn, tx);
		// do any transactions exist for this class/db?
		if(xactions == null) {
			xactions = new ConcurrentHashMap<String, SetInterface>();
			v.classToIsoTransaction.put(xClass, xactions);
		}
		// add out new transaction id/transaction map to the collection keyed by class
		xactions.put(tx.getName(), tm);
		if(DEBUG)
			System.out.println(DatabaseManager.class.getName()+" About to return NEW map with NEW xid "+tx.getName()+" from: "+dbname+xClass+" TransactionalMap:"+tm.toString()+" total xactions this class:"+xactions.size()+" total classes:"+v.classToIsoTransaction.mappingCount());
		return tm;
	}
	
	/**
	 * Remove the given TransactionalMap from active DB/transaction collection
	 * @param tmap the TransactionalMap for a given transaction Id
	 */
	public static synchronized void removeTransactionalMap(SetInterface tmap) {
		Volume vm = VolumeManager.get(tableSpaceDir);
		vm.classToIsoTransaction.forEach((k,v) -> {
			if(v.contains(tmap)) {
				TransactionalMap verify = (TransactionalMap) v.remove(((TransactionalMap)tmap).txn.getName());
				try {
					verify.session.Close(); // close RocksDB database
				} catch (IOException e) {}
				if(DEBUG)
					System.out.println("DatabaseManager.removeRockSackTransactionalMap removing xaction "+((TransactionalMap)tmap).txn.getName()+" for DB "+k+" which should match "+verify.txn.getName());
				return;
			}
		});
	}
	
	/**
	 * Remove the given Map from active DB/transaction collection
	 * @param alias The alias for the tablespace
	 * @param tmap the Map for a given transaction Id
	 */
	public static synchronized void removeMap(String alias, SetInterface tmap) throws NoSuchElementException {
		Volume vm = VolumeManager.getByAlias(alias);
		vm.classToIso.forEach((k,v) -> {
			if(v.equals(tmap)) {
				try {
					v.Close(); // close RocksDB database
				} catch (IOException e) {}
				if(DEBUG)
					System.out.println("DatabaseManager.removeRockSackTransactionalMap removing "+tmap.getDBName()+" for DB "+k+" which should match "+v.getDBName());
				return;
			}
		});	
	}
	
	/**
	 * Remove the given Map from active DB/transaction collection
	 * @param tmap the Map to remove
	 */
	public static synchronized void removeMap(SetInterface tmap) {
		Volume vm = VolumeManager.get(tableSpaceDir);
		vm.classToIso.forEach((k,v) -> {
			if(v.equals(tmap)) {
				try {
					v.Close(); // close RocksDB database
				} catch (IOException e) {}
				if(DEBUG)
					System.out.println("DatabaseManager.removeRockSackTransactionalMap removing "+tmap.getDBName()+" for DB "+k+" which should match "+v.getDBName());
				return;
			}
		});
	}
	
	/**
	 * Remove the given TransactionalMap from active DB/transaction collection
	 * @param alias The alias for the tablespace
	 * @param tmap the TransactionalMap for a given transaction Id
	 */
	public static synchronized void removeTransactionalMap(String alias, SetInterface tmap) throws NoSuchElementException {
		Volume vm = VolumeManager.getByAlias(alias);
		vm.classToIsoTransaction.forEach((k,v) -> {
			if(v.contains(tmap)) {
				TransactionalMap verify = (TransactionalMap) v.remove(((TransactionalMap)tmap).txn.getName());
				try {
					verify.session.Close(); // close RocksDB database
				} catch (IOException e) {}
				if(DEBUG)
					System.out.println("DatabaseManager.removeRockSackTransactionalMap removing xaction "+((TransactionalMap)tmap).txn.getName()+" for DB "+k+" which should match "+verify.txn.getName());
				return;
			}
		});
	}
	
	/**
	 * Remove the given TransactionalMap from active DB/transaction collection
	 * @param xid The Transaction Id
	 */
	public static synchronized void removeTransactionalMap(String xid) {
		Volume vm = VolumeManager.get(tableSpaceDir);
		Collection<ConcurrentHashMap<String, SetInterface>> xactions = vm.classToIsoTransaction.values();
		xactions.forEach(c -> {
			c.forEach((k,v) -> {
				if(k.equals(xid)) {
					TransactionalMap verify = (TransactionalMap) c.remove(xid);
					try {
						verify.session.Close(); //close RocksDB database
					} catch (IOException e) {}
					if(DEBUG)
						System.out.println("DatabaseManager.removeRockSackTransactionalMap removing xaction "+xid+" for DB "+k+" which should match "+verify.txn.getName());
					return;
				}
			});
		});
	}
	
	/**
	 * Remove the given TransactionalMap from active DB/transaction collection
	 * @param alias The alias for the tablespace
	 * @param xid The Transaction Id
	 * @throws NoSuchElementException if the alias does not exist
	 */
	public static synchronized void removeTransactionalMap(String alias, String xid) throws NoSuchElementException {
		Volume vm = VolumeManager.getByAlias(alias);
		Collection<ConcurrentHashMap<String, SetInterface>> xactions = vm.classToIsoTransaction.values();
		xactions.forEach(c -> {
			c.forEach((k,v) -> {
				if(k.equals(xid)) {
					TransactionalMap verify = (TransactionalMap) c.remove(xid);
					try {
						verify.session.Close(); // close RocksDB database
					} catch (IOException e) {}
					if(DEBUG)
						System.out.println("DatabaseManager.removeRockSackTransactionalMap removing xaction "+xid+" for DB "+k+" which should match "+verify.txn.getName());
					return;
				}
			});
		});
	}
	
	public static void endTransaction(String uid) throws IOException {
		VolumeManager.removeTransaction(uid);
	}
	
	public static void clearAllOutstandingTransactions() {
		VolumeManager.clearAllOutstandingTransactions();
	}
	
	public static void clearOutstandingTransaction(String uid) {
		VolumeManager.clearOutstandingTransaction(uid);
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

	public static void main(String[] args) throws Exception {
		DatabaseManager.setTableSpaceDir(args[0]);
		BufferedMap bm = DatabaseManager.getMap(args[1]);
		bm.entrySetStream().forEach(e -> {
			System.out.println(e.toString());
		});
	}
}
