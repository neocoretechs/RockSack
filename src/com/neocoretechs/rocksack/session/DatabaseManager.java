package com.neocoretechs.rocksack.session;

import java.io.File;
import java.io.IOException;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
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
import org.rocksdb.HashSkipListMemTableConfig;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.Transaction;
import org.rocksdb.Transaction.TransactionState;
import org.rocksdb.util.SizeUnit;

import com.neocoretechs.rocksack.Alias;
import com.neocoretechs.rocksack.DatabaseClass;
import com.neocoretechs.rocksack.LockingTransactionId;
import com.neocoretechs.rocksack.SerializedComparator;
import com.neocoretechs.rocksack.TransactionId;
import com.neocoretechs.rocksack.session.TransactionManager.SessionAndTransaction;
import com.neocoretechs.rocksack.session.VolumeManager.Volume;

/**
 * This factory class enforces a strong typing for the RockSack using the database naming convention linked to the
 * class name of the class stored there.<p>
 * In almost all cases, this is the main entry point to obtain a BufferedMap or a TransactionalMap.<p>
 * The DatabaseManager coordinates the {@link VolumeManager} the {@link SessionManager} and the {@link TransactionManager}.<p>
 * To override options call setDatabaseOptions(Options). If options are not set in this manner the default options will be used.
 * <p>
 * The main function of this adapter is to ensure that the appropriate map is instantiated.<br>
 * A map can be obtained by instance of Comparable to impart ordering.<br>
 * A Buffered map has atomic transactions bounded automatically with each insert/delete.<br>
 * A transactional map requires commit/rollback and can be checkpointed.<br>
 * An optimistic transactional map requires commit/rollback, can be snapshotted, and uses optimistic concurrency control, acquiring
 * no locks and checking for conflicts during commit.<br>
 * The database name is the full path of the top level tablespace and log directory, i.e.
 * /home/db/test would create a 'test' database in the /home/db directory consisting of a series of RocksDB files
 * with the database name appended to the class contained in each 'instance'. For example;
 * if we were to store a series of String instances, the database tablespace name would translate to: /home/db/testjava.lang.String.
 * Upon encountering each new class type a new set of 'instances' for the particular database tablespace would be created.
 * The class name is translated into the appropriate file name via a simple translation table to give us a
 * database/class tablespace identifier for each file used.
 * BufferedMap returns one instance of the class for each call to get the map. Transactional maps create a new instance with a new
 * transaction context using the originally opened database, and so must be maintained in another context for each transaction.<p>
 * 
 * Full control over placement of instances can be achieved with the {@link DatabaseClass} annotation on a class to be stored in the RockSack.
 * This annotation controls storage in a particular tablespace, and column. RocksDB uses the 'ColumnFamily' concept to represent 'columns' or collections
 * of data than can be grouped together under a common set of attributes defined in the options upon opening.<p>
 * It is analogous to separate databases stored under a unified set of files and directories.  
 * It can be considered separate columns or tablespaces or other
 * logical divisions in other systems. Here, we can store different class instances such as subclasses.<p>
 * As described above, the database is stored under the tablespace directory which has the database name concatenated with the class name 
 * and is used to obtain the Map. This tablespace directory will then contain the RocksDB files and logs etc.<p>
 * Using one of the methods in the class below, such as 'getMap', and transparent to
 * the user, DatabaseClass annotation then controls whether instances are stored in a different tablespace and internal column of that tablespace.<p>
 * Looking at the example in {@link com.neocoretechs.rocksack.test.BatteryKVDerived} we see that if we want to store subclass
 * instances with a superclass, we have just the 'column' attribute with the fully qualified name of the superclass. This will
 * ensure that sets retrieved include both subclasses and superclasses. If we want to store the subclass in a different column within the same
 * tablespace, we could have a different column name or omit the column attribute, which would then store the instances under the derived
 * class name in the tablespace of the direct superclass. So, omitting both tablespace and column attributes stores the instances in the direct
 * superclass under the column name of the subclass. So using this construct, annotations and combinations of the attributes gives the user full
 * control over placement of the instances. 
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2014,2015,2021,2022,2024
 *
 */
public final class DatabaseManager {
	private static boolean DEBUG = false;
	private static String tableSpaceDir = "/";
	private static final char[] ILLEGAL_CHARS = { '[', ']', '!', '+', '=', '|', ';', '?', '*', '\\', '<', '>', '|', '\"', ':' };
	private static final char[] OK_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E' };
	private Options options = null;
	private DBOptions dbOptions = null;
	private BlockBasedTableConfig baseTable = null;
	private ConcurrentHashMap<String, ColumnFamilyOptions> CFOptionsCache = new ConcurrentHashMap<String,ColumnFamilyOptions>();
	
	// Multithreaded double check Singleton setups:
	// 1.) privatized constructor; no other class can call
	private DatabaseManager() {
		baseTable = getPrivateBaseTable();
		setupColumnFamilies(baseTable);
		dbOptions = getPrivateDBOptions(baseTable);
		options = getDefaultOptions(baseTable);
	}
	// 2.) volatile instance
	private static volatile DatabaseManager instance = null;
	// 3.) lock class, assign instance if null
	public static DatabaseManager getInstance() {
		synchronized(DatabaseManager.class) {
			if(instance == null) {
				instance = new DatabaseManager();
			}
		}
		return instance;
	}	
	
	static {
		RocksDB.loadLibrary();
	}
	/**
	 * Get the default options using the RockSack {@link SerializedComparator}
	 * @return the populated RocksDB options instance
	 */
	private static Options getDefaultOptions(BlockBasedTableConfig baseTbl) {
		return getDefaultOptions(new SerializedComparator(), baseTbl);
	}
	/**
	 * Get the default options using a different comparator, primarily to provide hooks inside compare method.
	 * The call sequence would be DatabaseManager.setDatabaseOptions(DatabaseManager.getDefaultOptions(your SerializedComparator))
	 * WARNING: must provide all functionality of RockSack {@link SerializedComparator}
	 * @param comparator the AbstractComparator instance
	 * @return the populated RocksDB options instance
	 */
	private static Options getDefaultOptions(AbstractComparator comparator, BlockBasedTableConfig baseTable) {
		Options options = new Options();
		final Filter bloomFilter = new BloomFilter(10);
		//final ReadOptions readOptions = new ReadOptions().setFillCache(false);
		final Statistics stats = new Statistics();
		//final RateLimiter rateLimiter = new RateLimiter(10000000,10000, 10);
		//options.setRateLimiter(rateLimiter);
		options.setComparator(comparator);
		try {
			options.setCreateIfMissing(true)
			.setIncreaseParallelism(8)
			.setStatistics(stats)
			.setWriteBufferSize(64 * SizeUnit.MB)
			.setMaxWriteBufferNumber(25)
			.setMaxBackgroundJobs(24)
			.setCompressionType(CompressionType.SNAPPY_COMPRESSION)
			.setCompactionStyle(CompactionStyle.LEVEL);
		} catch (final IllegalArgumentException e) {
			//assert (false);
			bloomFilter.close();
			throw new RuntimeException(e);
		}
		options.setMemTableConfig(
				new HashSkipListMemTableConfig()
				.setHeight(4)
				.setBranchingFactor(4)
				.setBucketCount(2000000));
		// options.setMemTableConfig(new VectorMemTableConfig().setReservedSize(10000));
		// options.setMemTableConfig(new SkipListMemTableConfig());
		// options.setTableFormatConfig(new PlainTableConfig());
		// Plain-Table requires mmap read
		options.setTableFormatConfig(baseTable);
		options.setUseDirectReads(false);
		options.setAllowMmapReads(true);
		return options;
	}

	private static BlockBasedTableConfig getPrivateBaseTable() {
		// Shared cache for all CFs
		final Cache sharedCache = new LRUCache(1024L * 1024 * 1024, 6, true);
		BlockBasedTableConfig baseTbl = new BlockBasedTableConfig()
				.setBlockCache(sharedCache)
				.setBlockSize(64 * 1024)
				.setCacheIndexAndFilterBlocks(true)
				.setPinL0FilterAndIndexBlocksInCache(true)
				.setWholeKeyFiltering(false)
				.setFilterPolicy(new BloomFilter(10));
		return baseTbl;
	}
	
	private static DBOptions getPrivateDBOptions(BlockBasedTableConfig baseTable) {
		DBOptions dbOpts = new DBOptions(getDefaultOptions(baseTable))
				.setCreateIfMissing(true)
				.setCreateMissingColumnFamilies(true)
				.setUseDirectReads(false)
				.setAllowMmapReads(true)
				.setUseDirectIoForFlushAndCompaction(true)
				.setIncreaseParallelism(2)
				.setMaxBackgroundJobs(2);
		return dbOpts;
	}
	/**
	 * Compression_Compaction_Writebuffer_size, X is'use default'
	 * DEFAULT_COLUMN_FAMILY: equivalent to LZ4_X_3_96, <p>
	 * LZ4_X_64_X = LZ4 compression, default compaction, 64 MB write buffer, default max write buffer <p>
	 * LZ4_X_64_2 <p>
	 * NO_FIFO_32_2 = No compression, FIFO writebuffer, 32MB write buffer, max 2 write buffers<p>
	 * @param baseTbl
	 */
	private void setupColumnFamilies(BlockBasedTableConfig baseTbl) {
		ColumnFamilyOptions cfPrimary = new ColumnFamilyOptions()
				.setTableFormatConfig(baseTbl)
				.setWriteBufferSize(96L * SizeUnit.MB)
				.setMaxWriteBufferNumber(3)
				.setCompressionType(CompressionType.LZ4_COMPRESSION)
				.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION); //bottom most, or cold long term compression disabled by default
		cfPrimary.setComparator(new SerializedComparator());
		
		ColumnFamilyOptions cfReverse = new ColumnFamilyOptions()
				.setWriteBufferSize(64L * SizeUnit.MB)
				.setCompressionType(CompressionType.LZ4_COMPRESSION);
		cfReverse.setComparator(new SerializedComparator());
		
		ColumnFamilyOptions cfIndex = new ColumnFamilyOptions()
				.setWriteBufferSize(64L * SizeUnit.MB)
				.setMaxWriteBufferNumber(2)
				.setCompressionType(CompressionType.LZ4_COMPRESSION);
		cfIndex.setComparator(new SerializedComparator());

		ColumnFamilyOptions cfCounters = new ColumnFamilyOptions()
				.setWriteBufferSize(32L * SizeUnit.MB)
				.setMaxWriteBufferNumber(2)
				.setCompactionStyle(CompactionStyle.FIFO)
				.setCompressionType(CompressionType.NO_COMPRESSION);
		cfCounters.setComparator(new SerializedComparator());

		// Define CF descriptors Compression_Compaction_Writebuffer_size, X is'use default'
		CFOptionsCache.put("DEFAULT_COLUMN_FAMILY", cfPrimary);  //  primary semantic
		CFOptionsCache.put("LZ4_X_64_X", cfReverse);                //  reverse index
		CFOptionsCache.put("LZ4_X_64_2", cfIndex);                    //  main index
		CFOptionsCache.put("NO_FIFO_32_2", cfCounters);              // stats/counters
	}
	
	public ColumnFamilyOptions getOptionsCache(String optName) {
		return CFOptionsCache.get(optName);
	}
	
	/**
	 * Get the default options using default options.
	 * @return the populated DBOptions from default getDefaultOptions method
	 */
	public DBOptions getDefaultDBOptions() {
		return dbOptions;
	}
	
	public Options getDefaultOptions() {
		return options;
	}
	
	public BlockBasedTableConfig getBaseTable() {
		return baseTable;
	}
	
	
	/**
	 * Get the default ColumnFamily options using default options.
	 * @return the populated ColumnFamilyOptions from default getDefaultOptions method
	 */	
	public static ColumnFamilyOptions getDefaultColumnFamilyOptions() {
		return instance.CFOptionsCache.get("DEFAULT_COLUMN_FAMILY");
	}
	/**
	 * Extract the options from the {@link DatabaseClass} methods and build a ColumnFamilyOptions
	 * instance to place in the cache based on cfKey from DatabaseClass. 
	 * @param anno
	 * @param baseTbl
	 * @return
	 */
	private ColumnFamilyOptions buildOptionsFromAnnotation(DatabaseClass anno, BlockBasedTableConfig baseTbl) {
	    ColumnFamilyOptions opts = new ColumnFamilyOptions().setTableFormatConfig(baseTbl);
	    // Apply overrides if present
	    if (anno.writeBufferSize() > 0) {
	        opts.setWriteBufferSize(anno.writeBufferSize());
	    }
	    if (anno.maxWriteBufferNumber() > 0) {
	        opts.setMaxWriteBufferNumber(anno.maxWriteBufferNumber());
	    }
	    if (anno.compression() != CompressionType.NO_COMPRESSION) {
	        opts.setCompressionType(anno.compression());
	    }
	    if (anno.compactionStyle() != CompactionStyle.UNIVERSAL) {
	        opts.setCompactionStyle(anno.compactionStyle());
	    }
	    opts.setComparator(new SerializedComparator());
	    String ds = anno.column();
		if(ds.equals(""))
			ds = translateClass(anno.getClass().getName());
	    CFOptionsCache.put(ds, opts);
	    return opts;
	}
	/**
	 * Get the tablespace by given alias
	 * @param alias
	 * @return The path for this alias or null if none
	 */
	public static String getTableSpaceDir(Alias alias) {
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
	 * @param alias the alias name, which will be appended to tablespace path to create a database name, that class names will be then appended to.
	 * @param path Tablespace path
	 * @throws IOException 
	 */
	public static void setTableSpaceDir(Alias alias, String path) throws IOException {
		File p = new File(path);
		if(!new File(p.getParent()).isDirectory())
			throw new IOException("Cannot set alias for tablespace directory using fileset "+path+" to allocate persistent storage.");
		VolumeManager.createAlias(alias, path);
	}
	/**
	 * Set the default tablespace for operations not using alias
	 * @param path The path of the tablespace; which is a directory appended with a database name, which will then have class names appended. Such as /home/dir/data with database name being data.
	 * @throws IOException 
	 */
	public static void setTableSpaceDir(String path) throws IOException {
		File p = new File(path);
		if(!new File(p.getParent()).isDirectory())
			throw new IOException("Cannot set tablespace directory for fileset "+path+" to allocate persistent storage.");
		DatabaseManager.tableSpaceDir = path;
	}
	/**
	 * Remove the given alias. Affects only the alias reference, nothing else.
	 * @param alias
	 */
	public static void removeAlias(Alias alias) {
		VolumeManager.removeAlias(alias);
	}
	/**
	 * Remove the given tablespace path.
	 * @param path The tablespace path, which is a directory with appended database name such as /home/dir/data with database name being data
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
	 * Get the tablespace path for the given alias {@link VolumeManager}
	 * @param alias the database alias
	 * @return the path for this alias or null if none
	 */
	public static String getAliasToPath(Alias alias) {
		return VolumeManager.getAliasToPath(alias);
	}
	
	public static List<Transaction> getOutstandingTransactionsByAliasAndId(Alias alias, TransactionId uid) {
		return TransactionManager.getOutstandingTransactionsByAliasAndId(alias.getAlias(), uid);
	}
	
	public static List<Transaction> getOutstandingTransactionsByPathAndId(String path, TransactionId uid) throws IOException {
		return TransactionManager.getOutstandingTransactionsByPathAndId(path, uid);
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
		return TransactionManager.getOutstandingTransactionState();
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
			DatabaseManager.getInstance().buildOptionsFromAnnotation(dc, DatabaseManager.getInstance().getBaseTable());
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
						Session ts = SessionManager.Connect(tableSpaceDir+xClass, getInstance().getDefaultOptions(), dClass);
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
					ret =  new BufferedMap(SessionManager.Connect(tableSpaceDir+xClass, getInstance().getDefaultOptions()));
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
	public static BufferedMap getMap(Alias alias, Comparable clazz) throws IllegalAccessException, IOException, NoSuchElementException {
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
	public static BufferedMap getMap(Alias alias, Class clazz) throws IllegalAccessException, IOException, NoSuchElementException {
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
						Session ts = SessionManager.Connect(VolumeManager.getAliasToPath(alias)+xClass, getInstance().getDefaultOptions(), dClass);
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
					ret =  new BufferedMap(SessionManager.Connect(VolumeManager.getAliasToPath(alias)+xClass, getInstance().getDefaultOptions()));
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
	 * Generate a transaction with random UUID
	 * @return the {@link TransactionId} with string value randomUUID that will serve as unique globally unique transaction ID
	 */
	public static TransactionId getTransactionId() {
		return TransactionId.generate();
	}
	
	/**
	 * Generate a transaction with random UUID
	 * @param timeout the lock timeout in milliseconds
	 * @return the {@link LockingTransactionId} with timeout and string value randomUUID that will serve as unique globally unique transaction ID
	 */
	public static TransactionId getTransactionId(long timeout) {
		return LockingTransactionId.generate(timeout);
	}
	/**
	 * Start a new transaction for the given class in the current database
	 * @param clazz
	 * @return
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws RocksDBException 
	 */
	public static synchronized TransactionalMap getTransactionalMap(Comparable clazz, TransactionId xid) throws IllegalAccessException, IOException, RocksDBException {
		return getMap(VolumeManager.get(tableSpaceDir), tableSpaceDir, clazz.getClass(), xid);
	}
	/**
	 * Start a new transaction for the given class in the current database
	 * @param clazz
	 * @return
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws RocksDBException 
	 */
	public static synchronized TransactionalMap getTransactionalMap(Class clazz, TransactionId xid) throws IllegalAccessException, IOException, RocksDBException {
		return getMap(VolumeManager.get(tableSpaceDir), tableSpaceDir, clazz, xid);
	}
	/***
	 * Private helper method to obtain the {@link TransactionalMap} from a volume, tablespace, class, and transactionId
	 * @param v
	 * @param tDir
	 * @param clazz
	 * @param xid
	 * @return
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws RocksDBException
	 */
	private static TransactionalMap getMap(Volume v, String tDir, Class clazz, TransactionId xid) throws IllegalAccessException, IOException, RocksDBException {
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
						TransactionSession ts;
						if(xid instanceof LockingTransactionId)
							ts = SessionManager.ConnectTransaction(tDir+xClass, getInstance().getDefaultOptions(), dClass, ((LockingTransactionId)xid).getLockTimeout());
						else
							ts = SessionManager.ConnectTransaction(tDir+xClass, getInstance().getDefaultOptions(), dClass);
						// put the main class default ColumnFamily, its not there
						TransactionalMap tm = new TransactionalMap(ts, xClass, false);
						v.classToIsoTransaction.put(xClass, tm);
						associateSession(xid, tm);
						ret = new TransactionalMap(ts, dClass, isDerivedClass);
					} else {
						// create derived with session of main, previously instantiated default ColumnFamily
						ret = new TransactionalMap(def.getSession(), dClass, isDerivedClass);
					}
					v.classToIsoTransaction.put(dClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getMap xid:"+xid+" About to return DERIVED map:"+ret+" for dir:"+(tDir+xClass)+" class:"+xClass+" derived:"+dClass+" for volume:"+v);
				} else {
					TransactionSession ts;
					if(xid instanceof LockingTransactionId)
						ts = SessionManager.ConnectTransaction(tDir+xClass, getInstance().getDefaultOptions(), ((LockingTransactionId)xid).getLockTimeout());
					else
						ts = SessionManager.ConnectTransaction(tDir+xClass, getInstance().getDefaultOptions());
					ret =  new TransactionalMap(ts, xClass, isDerivedClass);
					v.classToIsoTransaction.put(xClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getMap xid:"+xid+" About to return BASE map:"+ret+" for dir:"+(tDir+xClass)+" class:"+xClass+" formed from "+clazz.getName()+" for volume:"+v);
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
			if(DEBUG)
				System.out.println("DatabaseManager.getMap xid:"+xid+" About to create new map:"+ret);
			associateSession(xid, ret);
			return ret;
		}
		if(DEBUG)
			System.out.println("DatabaseManager.getMap xid:"+xid+" About to return map:"+ret+" for class:"+xClass+" isDerivedClass:"+isDerivedClass);
		return ret;	
	}
	
	/**
	 * Start a new optimistic transaction for the given class in the current database
	 * @param clazz
	 * @return
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws RocksDBException 
	 */
	public static synchronized TransactionalMap getOptimisticTransactionalMap(Comparable clazz, TransactionId xid) throws IllegalAccessException, IOException, RocksDBException {
		return getOptimisticMap(VolumeManager.get(tableSpaceDir), tableSpaceDir, clazz.getClass(), xid);
	}
	/**
	 * Start a new optimistic transaction for the given class in the current database
	 * @param clazz
	 * @return
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws RocksDBException 
	 */
	public static synchronized TransactionalMap getOptimisticTransactionalMap(Class clazz, TransactionId xid) throws IllegalAccessException, IOException, RocksDBException {
		return getOptimisticMap(VolumeManager.get(tableSpaceDir), tableSpaceDir, clazz, xid);
	}
	/**
	 * Private helper method to obtain the {@link TransactionalMap} from a volume, tablespace, class, and transactionId
	 * using optimistic concurrency control
	 * @param v
	 * @param tDir
	 * @param clazz
	 * @param xid
	 * @return
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws RocksDBException
	 */
	private static TransactionalMap getOptimisticMap(Volume v, String tDir, Class clazz, TransactionId xid) throws IllegalAccessException, IOException, RocksDBException {
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
						TransactionSession ts = SessionManager.ConnectOptimisticTransaction(tDir+xClass, getInstance().getDefaultOptions(), dClass);
						// put the main class default ColumnFamily, its not there
						TransactionalMap tm = new TransactionalMap(ts, xClass, false);
						v.classToIsoTransaction.put(xClass, tm);
						associateSession(xid, tm);
						ret = new TransactionalMap(ts, dClass, isDerivedClass);
					} else {
						// create derived with session of main, previously instantiated default ColumnFamily
						ret = new TransactionalMap(def.getSession(), dClass, isDerivedClass);
					}
					v.classToIsoTransaction.put(dClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getOptimisticMap xid:"+xid+" About to return DERIVED map:"+ret+" for dir:"+(tDir+xClass)+" class:"+xClass+" derived:"+dClass+" for volume:"+v);
				} else {
					ret =  new TransactionalMap(SessionManager.ConnectOptimisticTransaction(tDir+xClass, getInstance().getDefaultOptions()), xClass, isDerivedClass);
					v.classToIsoTransaction.put(xClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getOptimisticMap xid:"+xid+" About to return BASE map:"+ret+" for dir:"+(tDir+xClass)+" class:"+xClass+" formed from "+clazz.getName()+" for volume:"+v);
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
			if(DEBUG)
				System.out.println("DatabaseManager.getOptimisticMap xid:"+xid+" About to create new map:"+ret);
			associateSession(xid, ret);
			return ret;
		}
		if(DEBUG)
			System.out.println("DatabaseManager.getOptimisticMap xid:"+xid+" About to return map:"+ret+" for class:"+xClass+" isDerivedClass:"+isDerivedClass);
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
	 */
	public static synchronized TransactionalMap getTransactionalMap(Alias alias, Comparable clazz, TransactionId xid) throws IllegalAccessException, IOException, NoSuchElementException {
		return getMap(alias, clazz.getClass(), xid);
	}
	
	public static synchronized TransactionalMap getTransactionalMap(Alias alias, Class clazz, TransactionId xid) throws IllegalAccessException, IOException, NoSuchElementException {
		return getMap(alias, clazz, xid);
	}
	/**
	 * Private helper method to obtain a {@link TransactionalMap}
	 * Start a new transaction for the given class in the aliased database
	 * @param alias The alias for the tablespace
	 * @param clazz
	 * @return the TransactionalMap for the alias/class/xid
	 * @throws IllegalAccessException
	 * @throws NoSuchElementException if The alias cant be located
	 * @throws IOException
	 * @throws RocksDBException 
	 */
	private static TransactionalMap getMap(Alias alias, Class clazz, TransactionId xid) throws IllegalAccessException, IOException, NoSuchElementException {
		Volume v = VolumeManager.getByAlias(alias);
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
						TransactionSession ts;
						if(xid instanceof LockingTransactionId)
							ts = SessionManager.ConnectTransaction(alias,VolumeManager.getAliasToPath(alias)+xClass, getInstance().getDefaultOptions(), dClass, ((LockingTransactionId)xid).getLockTimeout());
						else
							ts = SessionManager.ConnectTransaction(alias,VolumeManager.getAliasToPath(alias)+xClass, getInstance().getDefaultOptions(), dClass);
						// put the main class default ColumnFamily, its not there
						TransactionalMap tm = new TransactionalMap(ts, xClass, false);
						v.classToIsoTransaction.put(xClass, tm);
						associateSession(xid, tm);
						ret = new TransactionalMap(ts, dClass, isDerivedClass);
					} else {
						// create derived with session of main, previously instantiated default ColumnFamily
						ret = new TransactionalMap(def.getSession(), dClass, isDerivedClass);
					}
					v.classToIsoTransaction.put(dClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getMap xid:"+xid+" About to return DERIVED map:"+ret+" for dir:"+VolumeManager.getAliasToPath(alias)+" class:"+xClass+" derived:"+dClass+" for volume:"+v);
				} else {
					TransactionSession ts;
					if(xid instanceof LockingTransactionId)
						ts = SessionManager.ConnectTransaction(alias,VolumeManager.getAliasToPath(alias)+xClass, getInstance().getDefaultOptions(), ((LockingTransactionId)xid).getLockTimeout());
					else
						ts = SessionManager.ConnectTransaction(alias,VolumeManager.getAliasToPath(alias)+xClass, getInstance().getDefaultOptions());
					ret = new TransactionalMap(ts, xClass, isDerivedClass);
					v.classToIsoTransaction.put(xClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getMap xid:"+xid+" About to return BASE map:"+ret+" for dir:"+VolumeManager.getAliasToPath(alias)+" class:"+xClass+" formed from "+clazz.getName()+" for volume:"+v);
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
			if(DEBUG)
				System.out.println("DatabaseManager.getMap xid:"+xid+" About to create new map:"+ret);
			associateSession(xid, ret);
			return ret;
		}
		// We had the map requested,
		if(DEBUG)
			System.out.println("DatabaseManager.getMap xid:"+xid+" About to return map:"+ret+" for class:"+xClass+" isDerivedClass:"+isDerivedClass);
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
	 */
	public static TransactionalMap getOptimisticTransactionalMap(Alias alias, Comparable clazz, TransactionId xid) throws IllegalAccessException, IOException, NoSuchElementException {
		return getOptimisticMap(alias, clazz.getClass(), xid);
	}
	
	public static TransactionalMap getOptimisticTransactionalMap(Alias alias, Class clazz, TransactionId xid) throws IllegalAccessException, IOException, NoSuchElementException {
		return getOptimisticMap(alias, clazz, xid);
	}
	/**
	 * Start a new optimistic transaction for the given class in the aliased database
	 * @param alias The alias for the tablespace
	 * @param clazz
	 * @return the TransactionalMap for the alias/class/xid
	 * @throws IllegalAccessException
	 * @throws NoSuchElementException if The alias cant be located
	 * @throws IOException
	 * @throws RocksDBException 
	 */
	private static TransactionalMap getOptimisticMap(Alias alias, Class clazz, TransactionId xid) throws IllegalAccessException, IOException, NoSuchElementException {
		Volume v = VolumeManager.getByAlias(alias);
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
						TransactionSession ts = SessionManager.ConnectOptimisticTransaction(alias,VolumeManager.getAliasToPath(alias)+xClass, getInstance().getDefaultOptions(), dClass);
						// put the main class default ColumnFamily, its not there
						TransactionalMap tm = new TransactionalMap(ts, xClass, false);
						v.classToIsoTransaction.put(xClass, tm);
						associateSession(xid, tm);
						ret = new TransactionalMap(ts, dClass, isDerivedClass);
					} else {
						// create derived with session of main, previously instantiated default ColumnFamily
						ret = new TransactionalMap(def.getSession(), dClass, isDerivedClass);
					}
					v.classToIsoTransaction.put(dClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getOptimisticMap xid:"+xid+" About to return DERIVED map:"+ret+" for dir:"+VolumeManager.getAliasToPath(alias)+" class:"+xClass+" derived:"+dClass+" for volume:"+v);
				} else {
					ret = new TransactionalMap(SessionManager.ConnectOptimisticTransaction(alias,VolumeManager.getAliasToPath(alias)+xClass, getInstance().getDefaultOptions()), xClass, isDerivedClass);
					v.classToIsoTransaction.put(xClass, ret);
					if(DEBUG)
						System.out.println("DatabaseManager.getOptimisticMap xid:"+xid+" About to return BASE map:"+ret+" for dir:"+VolumeManager.getAliasToPath(alias)+" class:"+xClass+" formed from "+clazz.getName()+" for volume:"+v);
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
			if(DEBUG)
				System.out.println("DatabaseManager.getOptimisticMap xid:"+xid+" About to create new map:"+ret);
			associateSession(xid, ret);
			return ret;
		}
		// We had the map requested,
		if(DEBUG)
			System.out.println("DatabaseManager.getOptimisticMap xid:"+xid+" About to return map:"+ret+" for class:"+xClass+" isDerivedClass:"+isDerivedClass);
		return ret;		
	}

	/**
	 * TransactionalMap contains a {@link Session} or its subclasses {@link TransactionSession} or {@link TransactionSessionAlias}.
	 * Purpose here is to associate a transaction id {@link TransactionId} with a mangled name and
	 * 'session to transaction' in SessionTransaction in {@link TransactionManager}. The overriden method in TransactionSession.setTransactionalMap
	 * will generate a mangled name and start a Transaction.
	 * @param xid
	 * @param tm
	 * @throws IOException
	 */
	public static synchronized void associateSession(TransactionId xid, TransactionalMap tm) throws IOException {
		ConcurrentHashMap<String, SessionAndTransaction> ts = TransactionManager.getTransactionSession(xid);
		if(DEBUG)
			System.out.printf("DatabaseManager.associateSession Enter Transaction id:%s TransactionMap:%s got session:%s%n",xid,tm,ts);
		if(ts == null) {
			if(DEBUG)
				System.out.printf("DatabaseManager.associateSession Setting Transaction id:%s TransactionMap:%s create new link for session:%s%n",xid,tm,tm.getSession());
			TransactionManager.setTransaction(xid, tm);
			return;
		}
		// if map of mangled name to SessionAndTransaction instances exists already, throw exception
		if(tm.getSession().linkSessionAndTransaction(xid, tm, ts))
			throw new IOException("Transactional Map "+tm+" already associated with id "+xid);
	}
	
	/**
	 * TransactionalMap contains a {@link Session} or its subclasses {@link TransactionSession} or {@link TransactionSessionAlias}.
	 * Purpose here is to determine whether a transaction id {@link TransactionId} with a mangled name and
	 * 'session to transaction' in SessionTransaction in {@link TransactionManager}. 
	 * contains a mangled name and a Transaction.
	 * @param xid
	 * @param tm
	 * @return true if transaction id is associated to a {@link TransactionalMap}
	 * @throws IOException
	 */
	public static boolean isSessionAssociated(TransactionId xid, TransactionalMap tm) throws IOException {
		ConcurrentHashMap<String, SessionAndTransaction> ts = TransactionManager.getTransactionSession(xid);
		if(DEBUG)
			System.out.printf("DatabaseManager.isSessionAssociated %s %s %s%n",xid,tm.getClassName(),ts);
		// does map of mangled name to SessionAndTransaction instances exist?
		return tm.getSession().isTransactionLinked(xid, tm, ts);
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
					TransactionalMap verify = (TransactionalMap) vm.classToIsoTransaction.remove(k);
					verify.Close(); // close RocksDB database
				} catch (IOException e) {}
				if(DEBUG)
					System.out.println("DatabaseManager.removeRockSackTransactionalMap removed "+k);
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
		TransactionManager.removeTransaction(xid);
	}

	/**
	 * Remove from classToIso then idToTransaction in {@link VolumeManager}
	 * @param alias
	 * @param xid
	 * @throws NoSuchElementException if the alias doesnt exist
	 * @throws IOException If the transaction is not in a state to be removed. i.e. not COMMITTED, ROLLEDBACK or STARTED
	 */
	public static synchronized void removeTransaction(Alias alias, TransactionId xid) throws NoSuchElementException, IOException {
		List<Transaction> tx = TransactionManager.getOutstandingTransactionsByAliasAndId(alias.getAlias(), xid);
		if(tx != null && !tx.isEmpty()) {
				TransactionManager.removeTransaction(alias,xid);
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void commitTransaction(TransactionId xid) throws IOException {
		List<Transaction> tx = TransactionManager.getOutstandingTransactionsByPathAndId(tableSpaceDir, xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx) {
					if(!t.getState().equals(TransactionState.COMMITTED) &&
					   !t.getState().equals(TransactionState.ROLLEDBACK))
					t.commit();
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void commitTransaction(Alias alias, TransactionId xid) throws IOException, NoSuchElementException {
		List<Transaction> tx = TransactionManager.getOutstandingTransactionsByAliasAndId(alias.getAlias(), xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx) {
					if(!t.getState().equals(TransactionState.COMMITTED) &&
					   !t.getState().equals(TransactionState.ROLLEDBACK))
						t.commit();
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void rollbackTransaction(TransactionId xid) throws IOException {
		List<Transaction> tx = TransactionManager.getOutstandingTransactionsByPathAndId(tableSpaceDir, xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx) {
					if(!t.getState().equals(TransactionState.COMMITTED) &&
					   !t.getState().equals(TransactionState.ROLLEDBACK))
						t.rollback();
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void rollbackTransaction(Alias alias, TransactionId xid) throws IOException, NoSuchElementException {
		List<Transaction> tx = TransactionManager.getOutstandingTransactionsByAliasAndId(alias.getAlias(), xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx) {
					if(!t.getState().equals(TransactionState.COMMITTED) &&
					   !t.getState().equals(TransactionState.ROLLEDBACK))
						t.rollback();
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void checkpointTransaction(TransactionId xid) throws IOException {
		List<Transaction> tx = TransactionManager.getOutstandingTransactionsById(xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx) {
					if(!t.getState().equals(TransactionState.COMMITTED) &&
					   !t.getState().equals(TransactionState.ROLLEDBACK))
						t.setSavePoint();
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void checkpointTransaction(Alias alias, TransactionId xid) throws IOException, NoSuchElementException {
		List<Transaction> tx = TransactionManager.getOutstandingTransactionsByAliasAndId(alias.getAlias(), xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx) {
					if(!t.getState().equals(TransactionState.COMMITTED) &&
					   !t.getState().equals(TransactionState.ROLLEDBACK))
						t.setSavePoint();
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void rollbackToCheckpoint(TransactionId xid) throws IOException {
		List<Transaction> tx = TransactionManager.getOutstandingTransactionsByPathAndId(tableSpaceDir, xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx) {
					if(!t.getState().equals(TransactionState.COMMITTED) &&
					   !t.getState().equals(TransactionState.ROLLEDBACK))
						t.rollbackToSavePoint();
				}
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		} else
			throw new IOException("Transaction id "+xid+" was not found.");
	}
	
	public static void rollbackToCheckpoint(Alias alias, TransactionId xid) throws IOException, NoSuchElementException {
		List<Transaction> tx = TransactionManager.getOutstandingTransactionsByAliasAndId(alias.getAlias(), xid);
		if(tx != null && !tx.isEmpty()) {
			try {
				for(Transaction t: tx) {
					if(!t.getState().equals(TransactionState.COMMITTED) &&
					   !t.getState().equals(TransactionState.ROLLEDBACK))
						t.rollbackToSavePoint();
				}
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
	public static synchronized void removeMap(Alias alias, SetInterface tmap) throws NoSuchElementException {
		Volume vm = VolumeManager.getByAlias(alias);
		vm.classToIso.forEach((k,v) -> {
			if(v.equals(tmap)) {
				try {
					vm.classToIso.remove(k);
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
					vm.classToIso.remove(k);
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
	public static synchronized void removeTransactionalMap(Alias alias, TransactionSetInterface tmap) throws NoSuchElementException {
		Volume vm = VolumeManager.getByAlias(alias);
		vm.classToIsoTransaction.forEach((k,v) -> {
			if(v.equals(tmap)) {
				try {
					TransactionalMap verify = (TransactionalMap) vm.classToIsoTransaction.remove(k);
					verify.Close(); // close RocksDB database
				} catch (IOException e) {}
				if(DEBUG)
					System.out.println("DatabaseManager.removeRockSackTransactionalMap removing "+k);
				return;
			}
		});
	}
	
	/**
	 * Remove the given TransactionalMap from active DB/transaction collection
	 * @param xid the transaction id
	 * @param tmap the TransactionalMap for a given transaction Id
	 * @throws IOException 
	 */
	public static synchronized void removeTransactionalMap(TransactionId xid, TransactionSetInterface tmap) throws IOException {
		// Get the TransactionalMap
		Collection<Volume> vms = VolumeManager.get();
		for(Volume vm : vms) {
			vm.classToIsoTransaction.forEach((k,v) -> {
				if(v.equals(tmap)) {
					TransactionalMap verify;
					try {
						verify = (TransactionalMap) vm.classToIsoTransaction.remove(k);
						verify.Close();
					} catch (IOException e) {} // close RocksDB database
					if(DEBUG)
						System.out.println("DatabaseManager.removeRockSackTransactionalMap removed "+k);
				}
			});
		}
		TransactionManager.removeTransaction(xid);
	}
	
	/**
	 * Bridge method
	 * Remove the given TransactionalMap from active DB/transaction collection
	 * @param xid The Transaction Id
	 */
	private static synchronized void removeTransactionalMap(String xid) {
		Volume vm = VolumeManager.get(tableSpaceDir);
		vm.classToIsoTransaction.forEach((k,c) -> {
			if(k.equals(xid)) {
				TransactionalMap verify = (TransactionalMap) vm.classToIsoTransaction.remove(k);
				try {
					verify.Close(); //close RocksDB database
				} catch (IOException e) {}
				if(DEBUG)
					System.out.println("DatabaseManager.removeRockSackTransactionalMap removing xaction "+xid+" for DB "+k);
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
	public static synchronized void removeTransactionalMap(Alias alias, TransactionId xid) throws NoSuchElementException {
		Volume vm = VolumeManager.getByAlias(alias);
		vm.classToIsoTransaction.forEach((k,c) -> {
			if(k.equals(xid.getTransactionId())) {
				try {
					TransactionalMap verify = (TransactionalMap)c;
					verify.Close(); // close RocksDB database
				} catch (IOException e) {}
				if(DEBUG)
					System.out.println("DatabaseManager.removeRockSackTransactionalMap removing xaction "+xid+" for DB "+k);
				return;
			}
		});
	}

	public static void endTransaction(TransactionId xid) throws IOException {
		TransactionManager.removeTransaction(xid);
	}
	
	public static void clearAllOutstandingTransactions() {
		TransactionManager.clearAllOutstandingTransactions();
	}
	
	public static void clearOutstandingTransaction(TransactionId xid) throws IOException, RocksDBException {
		TransactionManager.clearOutstandingTransaction(xid);
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
