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
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
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
import com.neocoretechs.rocksack.DatabaseClass;
import com.neocoretechs.rocksack.SerializedComparator;
import com.neocoretechs.rocksack.TransactionId;
import com.neocoretechs.rocksack.session.VolumeManager.Volume;

/**
 * This factory class enforces a strong typing for the RockSack using the database naming convention linked to the
 * class name of the class stored there.<p/>
 * In almost all cases, this is the main entry point to obtain a BufferedMap or a TransactionalMap.<p/>
 * To override options call setDatabaseOptions(Options). If options are not set in this manner the default options will be used.
 * <p/>
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
 * transaction context using the originally opened database, and so must be maintained in another context for each transaction.<p>
 * 
 * Full control over placement of instances can be achieved with the {@link DatabaseClass} annotation on a class to be stored in the RockSack.
 * This annotation controls storage in a particular tablespace, and column. RocksDB uses the 'ColumnFamily' concept to represent 'columns' or collections
 * of data than can be grouped together under a common set of attributes defined in the options upon opening.<p/>
 * It is analogous to separate databases stored under a unified set of files and directories.  
 * It can be considered separate columns or tablespaces or other
 * logical divisions in other systems. Here, we can store different class instances such as subclasses.<p/>
 * As described above, the database is stored under the tablespace directory which has the database name concatenated with the class name 
 * and is used to obtain the Map. This tablespace directory will then contain the RocksDB files and logs etc.</>
 * Using one of the methods in the class below, such as 'getMap', and transparent to
 * the user, this annotation then controls whether instances are stored in a different tablspace and internal column of that tablespace.<p/>
 * Looking at the example in {@link com.neocoretechs.rocksack.test.BatteryKVDerived} we see that if we want to store subclass
 * instances with a superclass, we have just the 'column' attribute with the fully qualified name of the superclass. This will
 * ensure that sets retrieved include both subclasses and superclasses. If we want to store the subclass in a different column within the same
 * tablespace, we could have a different column name or omit the column attribute, which would then store the instances under the derived
 * class name in the tablespace of the direct superclass. So, omitting both tablespace and column attributes stores the instances in the direct
 * superclass under the column name of the subclass. So using this annotations and combinations of the attributes gives the user full
 * control over placement of the instances. 
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
	 * Get the default options using default options.
	 * @return the populated DBOptions from default getDefaultOptions method
	 */
	public static DBOptions getDefaultDBOptions() {
		DBOptions options = new DBOptions(getDefaultOptions());
		return options;
	}	
	/**
	 * Get the default ColumnFamily options using default options.
	 * @return the populated ColumnFamilyOptions from default getDefaultOptions method
	 */	
	public static ColumnFamilyOptions getDefaultColumnFamilyOptions() {
		ColumnFamilyOptions options = new ColumnFamilyOptions(getDefaultOptions());
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
		boolean isDerivedClass = false;
		String xClass,dClass = null;
		BufferedMap ret = null;
		//
		Volume v = VolumeManager.get(tableSpaceDir);
		// are we working with marked derived class? if so open as column family in main class tablespace
		if(clazz.isAnnotationPresent(DatabaseClass.class)) {
			isDerivedClass = true;
			DatabaseClass dc = (DatabaseClass)clazz.getAnnotation(DatabaseClass.class);
			String ts = dc.tablespace();
			if(ts.equals(""))
				ts = clazz.getSuperclass().getName();
			xClass = translateClass(ts);
			String ds = dc.column();
			if(ds.equals(""))
				ds = clazz.getName();
			dClass = translateClass(ds);
			ret = (BufferedMap) v.classToIso.get(dClass);
		} else {
			xClass = translateClass(clazz.getName());
			ret = (BufferedMap) v.classToIso.get(xClass);
		}
		if( ret == null ) {
			try {
				if(isDerivedClass) {
					BufferedMap def = (BufferedMap) v.classToIso.get(xClass);
					// have we already opened the main database?
					if(def == null) {
						Session ts = SessionManager.Connect(tableSpaceDir+xClass, options, dClass);
						// put the main class default ColumnFamily, its not there
						v.classToIso.put(xClass, (BufferedMap)(new BufferedMap(ts)));
						ret = (BufferedMap)(new BufferedMap(ts, dClass));
					} else {
						// create derived with session of main, previously instantiated default ColumnFamily
						ret = (BufferedMap)(new BufferedMap(def.getSession(), dClass));
					}
					v.classToIso.put(dClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getMap About to return DERIVED map:"+ret+" for dir:"+tableSpaceDir+" class:"+xClass+" derived:"+dClass+" for volume:"+v);
				} else {
					ret =  new BufferedMap(SessionManager.Connect(tableSpaceDir+xClass, options));
					v.classToIso.put(xClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getMap About to return BASE map:"+ret+" for dir:"+tableSpaceDir+" class:"+xClass+" formed from "+clazz.getName()+" for volume:"+v);
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
			if(DEBUG)
				System.out.println("DatabaseManager.getMap About to create new map:"+ret);
		}
		if(DEBUG)
			System.out.println("DatabaseManager.getMap About to return map:"+ret+" for class:"+xClass+" isDerivedClass:"+isDerivedClass);
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
		boolean isDerivedClass = false;
		String xClass,dClass = null;
		BufferedMap ret = null;
		//
		Volume v = VolumeManager.getByAlias(alias);
		// are we working with marked derived class? if so open as column family in main class tablespace
		if(clazz.isAnnotationPresent(DatabaseClass.class)) {
			isDerivedClass = true;
			DatabaseClass dc = (DatabaseClass)clazz.getAnnotation(DatabaseClass.class);
			String ts = dc.tablespace();
			if(ts.equals(""))
				ts = clazz.getSuperclass().getName();
			xClass = translateClass(ts);
			String ds = dc.column();
			if(ds.equals(""))
				ds = clazz.getName();
			dClass = translateClass(ds);
			ret = (BufferedMap) v.classToIso.get(dClass);
		} else {
			xClass = translateClass(clazz.getName());
			ret = (BufferedMap) v.classToIso.get(xClass);
		}
		if( ret == null ) {
			try {
				if(isDerivedClass) {
					BufferedMap def = (BufferedMap) v.classToIso.get(xClass);
					// have we already opened the main database?
					if(def == null) {
						Session ts = SessionManager.Connect(VolumeManager.getAliasToPath(alias)+xClass, options, dClass);
						// put the main class default ColumnFamily, its not there
						v.classToIso.put(xClass, (BufferedMap)(new BufferedMap(ts)));
						ret = (BufferedMap)(new BufferedMap(ts, dClass));
					} else {
						// create derived with session of main, previously instantiated default ColumnFamily
						ret = (BufferedMap)(new BufferedMap(def.getSession(), dClass));
					}
					v.classToIso.put(dClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getMap About to return DERIVED map:"+ret+" for alias:"+alias+" path:"+(VolumeManager.getAliasToPath(alias)+xClass)+" class:"+xClass+" derived:"+dClass+" for volume:"+v);
				} else {
					ret =  new BufferedMap(SessionManager.Connect(VolumeManager.getAliasToPath(alias)+xClass, options));
					v.classToIso.put(xClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getMap About to return BASE map:"+ret+" alias:"+alias+" for dir:"+(VolumeManager.getAliasToPath(alias)+xClass)+" class:"+xClass+" formed from "+clazz.getName()+" for volume:"+v);
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
			if(DEBUG)
				System.out.println("DatabaseManager.getMap About to create new map:"+ret+" for alias:"+alias);
		}
		if(DEBUG)
			System.out.println("DatabaseManager.getMap About to return map:"+ret+" for class:"+xClass+" isDerivedClass:"+isDerivedClass+" alias:"+alias);
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
	 * @return The {@link BufferedMap} or {@link BufferedMap} for the clazz type.
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public static BufferedMap getMapByPath(String path, Class clazz) throws IllegalAccessException, IOException {
		boolean isDerivedClass = false;
		String xClass,dClass = null;
		BufferedMap ret = null;
		//
		Volume v = VolumeManager.get(path);
		// are we working with marked derived class? if so open as column family in main class tablespace
		if(clazz.isAnnotationPresent(DatabaseClass.class)) {
			isDerivedClass = true;
			DatabaseClass dc = (DatabaseClass)clazz.getAnnotation(DatabaseClass.class);
			String ts = dc.tablespace();
			if(ts.equals(""))
				ts = clazz.getSuperclass().getName();
			xClass = translateClass(ts);
			String ds = dc.column();
			if(ds.equals(""))
				ds = clazz.getName();
			dClass = translateClass(ds);
			ret = (BufferedMap) v.classToIso.get(dClass);
		} else {
			xClass = translateClass(clazz.getName());
			ret = (BufferedMap) v.classToIso.get(xClass);
		}
		if( ret == null ) {
			try {
				if(isDerivedClass) {
					BufferedMap def = (BufferedMap) v.classToIso.get(xClass);
					// have we already opened the main database?
					if(def == null) {
						Session ts = SessionManager.Connect(path+xClass, options, dClass);
						// put the main class default ColumnFamily, its not there
						v.classToIso.put(xClass, (BufferedMap)(new BufferedMap(ts)));
						ret = (BufferedMap)(new BufferedMap(ts, dClass));
					} else {
						// create derived with session of main, previously instantiated default ColumnFamily
						ret = (BufferedMap)(new BufferedMap(def.getSession(), dClass));
					}
					v.classToIso.put(dClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getMapByPath About to return DERIVED map:"+ret+" for path:"+path+" class:"+xClass+" derived:"+dClass+" for volume:"+v);
				} else {
					ret =  new BufferedMap(SessionManager.Connect(path+xClass, options));
					v.classToIso.put(xClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getMapByPath About to return BASE map:"+ret+" for path:"+path+" class:"+xClass+" formed from "+clazz.getName()+" for volume:"+v);
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
			if(DEBUG)
				System.out.println("DatabaseManager.getMapByPath path:"+path+" About to create new map:"+ret);
		}
		if(DEBUG)
			System.out.println("DatabaseManager.getMapByPath path:"+path+" About to return map:"+ret+" for class:"+xClass+" isDerivedClass:"+isDerivedClass);
		return ret;
	}
	
	/**
	 * Generate a randomUUID
	 * @return the string values randomUUID that will serve as unique globally unique transaction ID
	 */
	public static TransactionId getTransactionId() {
		return TransactionId.generate();
	}
	
	/**
	 * Start a new transaction for the given class in the current database
	 * @param clazz
	 * @return
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws RocksDBException 
	 */
	public static synchronized TransactionalMap getTransactionalMap(Comparable clazz, TransactionId xid) throws IllegalAccessException, IOException {
		return getTransactionalMap(clazz.getClass(), xid);
	}
	/**
	 * Start a new transaction for the given class in the current database
	 * @param clazz
	 * @return
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws RocksDBException 
	 */
	public static synchronized TransactionalMap getTransactionalMap(Class clazz, TransactionId xid) throws IllegalAccessException, IOException {
		boolean isDerivedClass = false;
		String xClass,dClass = null;
		TransactionalMap ret = null;
		//
		Volume v = VolumeManager.get(tableSpaceDir);
		// are we working with marked derived class? if so open as column family in main class tablespace
		if(clazz.isAnnotationPresent(DatabaseClass.class)) {
			isDerivedClass = true;
			DatabaseClass dc = (DatabaseClass)clazz.getAnnotation(DatabaseClass.class);
			String ts = dc.tablespace();
			if(ts.equals(""))
				ts = clazz.getSuperclass().getName();
			xClass = translateClass(ts);
			String ds = dc.column();
			if(ds.equals(""))
				ds = clazz.getName();
			dClass = translateClass(ds);
			ret = (TransactionalMap) v.classToIsoTransaction.get(dClass);
		} else {
			xClass = translateClass(clazz.getName());
			ret = (TransactionalMap) v.classToIsoTransaction.get(xClass);
		}
		if( ret == null ) {
			try {
				if(isDerivedClass) {
					TransactionalMap def = (TransactionalMap) v.classToIsoTransaction.get(xClass);
					// have we already opened the main database?
					if(def == null) {
						TransactionSession ts = SessionManager.ConnectTransaction(tableSpaceDir+xClass, options, dClass);
						// put the main class default ColumnFamily, its not there
						v.classToIsoTransaction.put(xClass, (TransactionalMap)(new TransactionalMap(ts, xid)));
						ret = (TransactionalMap)(new TransactionalMap(ts, xid, dClass));
					} else {
						// create derived with session of main, previously instantiated default ColumnFamily
						ret = (TransactionalMap)(new TransactionalMap(def.getSession(), xid, dClass));
					}
					v.classToIsoTransaction.put(dClass, ret);
					v.idToTransaction.put(xid.getTransactionId(), ret.getTransaction());
					if(DEBUG)
						System.out.println("DatabaseManager.getTransactionalMap xid:"+xid+" About to return DERIVED map:"+ret+" for dir:"+tableSpaceDir+" class:"+xClass+" derived:"+dClass+" for volume:"+v);
				} else {
					ret =  new TransactionalMap(SessionManager.ConnectTransaction(tableSpaceDir+xClass, options), xid);
					v.classToIsoTransaction.put(xClass, ret);
					v.idToTransaction.put(xid.getTransactionId(), ret.getTransaction());
					if(DEBUG)
						System.out.println("DatabaseManager.getTransactionalMap xid:"+xid+" About to return BASE map:"+ret+" for dir:"+tableSpaceDir+" class:"+xClass+" formed from "+clazz.getName()+" for volume:"+v);
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
			if(DEBUG)
				System.out.println("DatabaseManager.getTransactionalMap xid:"+xid+" About to create new map:"+ret);
		}
		if(DEBUG)
			System.out.println("DatabaseManager.getTransactionalMap xid:"+xid+" About to return map:"+ret+" for class:"+xClass+" isDerivedClass:"+isDerivedClass);
		return ret;		
	}
	
	public static synchronized TransactionalMap getTransactionalMap(Volume v, String tDir, Class clazz, TransactionId xid) throws IllegalAccessException, IOException, RocksDBException {
		boolean isDerivedClass = false;
		String xClass,dClass = null;
		TransactionalMap ret = null;
		//
		// are we working with marked derived class? if so open as column family in main class tablespace
		if(clazz.isAnnotationPresent(DatabaseClass.class)) {
			isDerivedClass = true;
			DatabaseClass dc = (DatabaseClass)clazz.getAnnotation(DatabaseClass.class);
			String ts = dc.tablespace();
			if(ts.equals(""))
				ts = clazz.getSuperclass().getName();
			xClass = translateClass(ts);
			String ds = dc.column();
			if(ds.equals(""))
				ds = clazz.getName();
			dClass = translateClass(ds);
			ret = (TransactionalMap) v.classToIsoTransaction.get(dClass);
		} else {
			xClass = translateClass(clazz.getName());
			ret = (TransactionalMap) v.classToIsoTransaction.get(xClass);
		}
		if( ret == null ) {
			try {
				if(isDerivedClass) {
					TransactionalMap def = (TransactionalMap) v.classToIsoTransaction.get(xClass);
					// have we already opened the main database?
					if(def == null) {
						TransactionSession ts = SessionManager.ConnectTransaction(tDir+xClass, options, dClass);
						// put the main class default ColumnFamily, its not there
						v.classToIsoTransaction.put(xClass, (TransactionalMap)(new TransactionalMap(ts, xid)));
						ret = (TransactionalMap)(new TransactionalMap(ts, xid, dClass));
					} else {
						// create derived with session of main, previously instantiated default ColumnFamily
						ret = (TransactionalMap)(new TransactionalMap(def.getSession(), xid, dClass));
					}
					v.classToIsoTransaction.put(dClass, ret);
					v.idToTransaction.put(xid.getTransactionId(), ret.getTransaction());
					if(DEBUG)
						System.out.println("DatabaseManager.getTransactionalMap xid:"+xid+" About to return DERIVED map:"+ret+" for dir:"+(tDir+xClass)+" class:"+xClass+" derived:"+dClass+" for volume:"+v);
				} else {
					ret =  new TransactionalMap(SessionManager.ConnectTransaction(tDir+xClass, options), xid);
					v.classToIsoTransaction.put(xClass, ret);
					v.idToTransaction.put(xid.getTransactionId(), ret.getTransaction());
					if(DEBUG)
						System.out.println("DatabaseManager.getTransactionalMap xid:"+xid+" About to return BASE map:"+ret+" for dir:"+(tDir+xClass)+" class:"+xClass+" formed from "+clazz.getName()+" for volume:"+v);
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
			if(DEBUG)
				System.out.println("DatabaseManager.getTransactionalMap xid:"+xid+" About to create new map:"+ret);
		}
		if(DEBUG)
			System.out.println("DatabaseManager.getTransactionalMap xid:"+xid+" About to return map:"+ret+" for class:"+xClass+" isDerivedClass:"+isDerivedClass);
		return ret;	
	}
	/**
	 * Start a new transaction for the given class in the aliased database
	 * @param alias The alias for the tablespace
	 * @param clazz
	 * @return the TransactionalMap for the alias/class/xid
	 * @throws IllegalAccessException
	 * @throws NoSuchElementException if The alias cant be located
	 * @throws IOException
	 * @throws RocksDBException 
	 */
	public static synchronized TransactionalMap getTransactionalMap(String alias, Comparable clazz, TransactionId xid) throws IllegalAccessException, IOException, NoSuchElementException {
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
	 * @throws RocksDBException 
	 */
	public static synchronized TransactionalMap getTransactionalMap(String alias, Class clazz, TransactionId xid) throws IllegalAccessException, IOException, NoSuchElementException {
		Volume v = VolumeManager.getByAlias(alias);
		try {
			return getTransactionalMap(v, VolumeManager.getAliasToPath(alias), clazz, xid);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}
	/**
	 * Start a new transaction for the given class in the database absolute path
	 * @param path the database tablespace 
	 * @param clazz
	 * @return the TransactionalMap for the alias/class/xid
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws RocksDBException 
	 * @throws NoSuchElementException 
	 */
	public static synchronized TransactionalMap getTransactionalMapByPath(String path, Comparable clazz, TransactionId xid) throws IllegalAccessException, IOException, NoSuchElementException {
		return getTransactionalMap(path, clazz.getClass(), xid);
	}
	/**
	 * Start a new transaction for the given class in the database absolute path
	 * @param path The path for the tablespace
	 * @param clazz
	 * @return the TransactionalMap for the alias/class/xid
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws RocksDBException 
	 */
	public static synchronized TransactionalMap getTransactionalMapByPath(String path, Class clazz, TransactionId xid) throws IllegalAccessException, IOException {
		Volume v = VolumeManager.get(path);
		try {
			return getTransactionalMap(v, path, clazz, xid);
		} catch ( RocksDBException e) {
			throw new IOException(e);
		}
	}
	
	/**
	 * Remove the given TransactionalMap from active DB/transaction collection
	 * @param tmap the TransactionalMap for a given transaction Id
	 */
	public static synchronized void removeTransactionalMap(SetInterface tmap) {
		Volume vm = VolumeManager.get(tableSpaceDir);
		vm.classToIsoTransaction.forEach((k,v) -> {
			if(v.equals(tmap)) {
				try {
					TransactionalMap verify = (TransactionalMap) v.remove(((TransactionalMap)tmap).txn.getName());
					verify.session.Close(); // close RocksDB database
				} catch (IOException e) {}
				if(DEBUG)
					System.out.println("DatabaseManager.removeRockSackTransactionalMap removing xaction "+((TransactionalMap)tmap).txn.getName()+" for DB "+k);
				return;
			}
		});
	}
	
	/**
	 * Remove from classToIso then idToTransaction in {@link VolumeManager}
	 * @param xid
	 * @throws IOException If the transaction is not in a state to be removed. i.e. not COMMITTED, ROLLEDBACK or STARTED
	 */
	public static synchronized void removeTransaction(TransactionId xid) throws IOException {
		removeTransactionalMap(xid.getTransactionId());
		VolumeManager.removeTransaction(xid.getTransactionId());
	}

	/**
	 * Remove from classToIso then idToTransaction in {@link VolumeManager}
	 * @param alias
	 * @param xid
	 * @throws NoSuchElementException if the alias doesnt exist
	 * @throws IOException If the transaction is not in a state to be removed. i.e. not COMMITTED, ROLLEDBACK or STARTED
	 */
	public static synchronized void removeTransaction(String alias, TransactionId xid) throws NoSuchElementException, IOException {
		removeTransactionalMap(alias, xid.getTransactionId());
		VolumeManager.removeTransaction(xid.getTransactionId());
	}
	
	public static void commitTransaction(TransactionId xid) throws IOException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByPathAndId(tableSpaceDir, xid.getTransactionId());
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
	
	public static void commitTransaction(String alias, TransactionId xid) throws IOException, NoSuchElementException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByAliasAndId(alias, xid.getTransactionId());
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
	
	public static void rollbackTransaction(TransactionId xid) throws IOException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByPathAndId(tableSpaceDir, xid.getTransactionId());
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
	
	public static void rollbackTransaction(String alias, TransactionId xid) throws IOException, NoSuchElementException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByAliasAndId(alias, xid.getTransactionId());
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
	
	public static void checkpointTransaction(TransactionId xid) throws IOException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsById(xid.getTransactionId());
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
	
	public static void checkpointTransaction(String alias, TransactionId xid) throws IOException, NoSuchElementException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByAliasAndId(alias, xid.getTransactionId());
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
	
	public static void rollbackToCheckpoint(TransactionId xid) throws IOException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByPathAndId(tableSpaceDir, xid.getTransactionId());
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
	
	public static void rollbackToCheckpoint(String alias, TransactionId xid) throws IOException, NoSuchElementException {
		List<Transaction> tx = VolumeManager.getOutstandingTransactionsByAliasAndId(alias, xid.getTransactionId());
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
			if(v.equals(tmap)) {
				try {
					TransactionalMap verify = (TransactionalMap) v.remove(((TransactionalMap)tmap).txn.getName());
					verify.session.Close(); // close RocksDB database
				} catch (IOException e) {}
				if(DEBUG)
					System.out.println("DatabaseManager.removeRockSackTransactionalMap removing xaction "+((TransactionalMap)tmap).txn.getName()+" for DB "+k);
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
		vm.classToIsoTransaction.forEach((k,c) -> {
			if(k.equals(xid)) {
				TransactionalMap verify = (TransactionalMap) vm.classToIsoTransaction.remove(xid);
				try {
					verify.session.Close(); //close RocksDB database
				} catch (IOException e) {}
				if(DEBUG)
					System.out.println("DatabaseManager.removeRockSackTransactionalMap removing xaction "+xid+" for DB "+k+" which should match "+verify.txn.getName());
				return;
			}
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
		vm.classToIsoTransaction.forEach((k,c) -> {
			if(k.equals(xid)) {
				try {
					TransactionalMap verify = (TransactionalMap) c.remove(xid);
					verify.session.Close(); // close RocksDB database
				} catch (IOException e) {}
				if(DEBUG)
					System.out.println("DatabaseManager.removeRockSackTransactionalMap removing xaction "+xid+" for DB "+k);
				return;
			}
		});
	}

	public static void endTransaction(TransactionId xid) throws IOException {
		VolumeManager.removeTransaction(xid.getTransactionId());
	}
	
	public static void clearAllOutstandingTransactions() {
		VolumeManager.clearAllOutstandingTransactions();
	}
	
	public static void clearOutstandingTransaction(TransactionId xid) {
		VolumeManager.clearOutstandingTransaction(xid.getTransactionId());
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
