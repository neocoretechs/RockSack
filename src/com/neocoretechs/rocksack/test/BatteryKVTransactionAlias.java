package com.neocoretechs.rocksack.test;

import java.util.Iterator;
import java.util.Map;

import com.neocoretechs.rocksack.Alias;
import com.neocoretechs.rocksack.TransactionId;
import com.neocoretechs.rocksack.iterator.Entry;
import com.neocoretechs.rocksack.session.DatabaseManager;
import com.neocoretechs.rocksack.session.TransactionalMap;


/**
 * Transaction KV test battery with alias interleaved operations.<p>
 * We will operate with 2 main transactions on 2 aliased databases. We will also toss in
 * some operations on an intermediate transaction that will be rolled back and discarded.<p>
 * The 2 main transactions will contain even and odd records. In one database it will represent
 * the set of even records, in the other, the set of odd records. We will verify that on each
 * occasion the transactions are faithfully maintained.<p>
 * RockSack transactions are a bit more flexible and encompass a larger domain than RocksDb
 * transactions, but still preserve the concepts of isolation, durability and atomicity.
 * RockSack transactions can span multiple classes and databases. RocksSack transactions
 * function as more of a container whose context can be defined as needed.
 * NOTES:
 * program argument is database tablespace i.e. C:/users/you/RockSack/
 * C:/users/you/RockSack should be valid path. C:/users/you/RockSack/ALIAS1java.lang.String, etc. will be created.
 * @author Jonathan Groff (C) NeoCoreTechs 2022,2024
 *
 */
public class BatteryKVTransactionAlias {
	public static boolean DEBUG = false;
	static String key = "This is a test"; // holds the base random key string for tests
	static String val = "Of a RockSack element!"; // holds base random value string
	static String uniqKeyFmt = "%0100d"; // base + counter formatted with this gives equal length strings for canonical ordering
	static int min = 0;
	static int max = 100000;
	static int numDelete = 100; // for delete test
	private static int dupes;
	private static int numLookupByValue = 10;
	private static TransactionalMap bmap;
	private static TransactionalMap bmap2;
	private static Alias alias1 = new Alias("ALIAS1");
	private static Alias alias2 = new Alias("ALIAS2");
	/**
	* Main test fixture driver
	*/
	public static void main(String[] argv) throws Exception {
		if(argv.length < 1) {
			System.out.println("Usage: java com.neocoretechs.relatrix.test.kv.BatteryKVTransactionAlias <path>");
			System.exit(1);
		}
		String tablespace = argv[0];
		if(!tablespace.endsWith("/"))
			tablespace += "/";
		
		System.out.println("Tablespace:"+tablespace);
		DatabaseManager.setTableSpaceDir(alias1,tablespace+alias1);
		DatabaseManager.setTableSpaceDir(alias2,tablespace+alias2);
		
		TransactionId xid = DatabaseManager.getTransactionId();
		TransactionId xid0 = DatabaseManager.getTransactionId();
		
		bmap = DatabaseManager.getTransactionalMap(alias1, String.class, xid);
		DatabaseManager.associateSession(xid0, bmap);
		battery1(xid, xid0, alias1, bmap);
		
		// expand these transactions to the next database
		// even values are stored under transaction xid, odd values are under xid0, in ALIAS1 
		// and even is xid0, odd is xid in database ALIAS2.
		bmap2 = DatabaseManager.getTransactionalMap(alias2, String.class, xid);
		System.out.println(bmap2);
		DatabaseManager.associateSession(xid0, bmap2);
		battery1(xid0, xid, alias2, bmap2);
		
		// Throw in a new transaction that we will roll back just to ensure we dont have interference
		// from an intermediate transaction and to verify rollback is operational.
		TransactionId xid2 = DatabaseManager.getTransactionId();
		DatabaseManager.associateSession(xid2, bmap2);
		// xid2 will be rolled back then removed
		battery11(xid2, alias2, bmap2);
		
		// xid, and xid0 should still be valid
		// even is xid, odd is xid0, in ALIAS1 
		// even is xid0, odd is xid in database ALIAS2.
		battery1AR6(xid, alias1, bmap, 0);
		battery1AR6(xid0, alias1, bmap, 1);
		battery1AR6(xid, alias2, bmap2, 1);
		battery1AR6(xid0, alias2, bmap2, 0);
		
		battery1AR7(xid, alias1, bmap, 0);
		battery1AR7(xid0, alias1, bmap, 1);
		battery1AR7(xid, alias2, bmap2, 1);
		battery1AR7(xid0, alias2, bmap2, 0);
		
		battery1AR8(xid, alias1, bmap, 0);
		battery1AR8(xid0, alias1, bmap, 1);
		battery1AR8(xid, alias2, bmap2, 1);
		battery1AR8(xid0, alias2, bmap2, 0);
		
		battery1AR9(xid, alias1, bmap, 0);
		battery1AR9(xid0, alias1, bmap, 1);
		battery1AR9(xid, alias2, bmap2, 1);
		battery1AR9(xid0, alias2, bmap2, 0);
		
		battery1AR10(xid, alias1, bmap, 0);
		battery1AR10(xid0, alias1, bmap,1);
		battery1AR10(xid, alias2, bmap2, 1);
		battery1AR10(xid0, alias2, bmap2,0);
		
		battery1AR101(xid, alias1, bmap, 0);
		battery1AR101(xid0, alias1, bmap, 1);
		battery1AR101(xid, alias2, bmap2, 1);
		battery1AR101(xid0, alias2, bmap2, 0);
		
		battery1AR11(xid, alias1, bmap, 0);
		battery1AR11(xid0, alias1, bmap, 1);
		battery1AR11(xid, alias2, bmap2, 1);
		battery1AR11(xid0, alias2, bmap2, 0);
		
		battery1AR12(xid, alias1, bmap, 0);
		battery1AR12(xid0, alias1, bmap, 1);
		battery1AR12(xid, alias2, bmap2, 1);
		battery1AR12(xid0, alias2, bmap2, 0);
		
		battery1AR13(xid, alias1, bmap, 0);
		battery1AR13(xid0, alias1, bmap, 1);
		battery1AR13(xid, alias2, bmap2, 1);
		battery1AR13(xid0, alias2, bmap2, 0);
		
		battery1AR14(xid, alias1, bmap, 0);
		battery1AR14(xid0, alias1, bmap, 1);
		battery1AR14(xid, alias2, bmap2, 1);
		battery1AR14(xid0, alias2, bmap2, 0);
		
		battery1AR15(xid, alias1, bmap, 0);
		battery1AR15(xid0, alias1, bmap, 1);
		battery1AR15(xid, alias2, bmap2, 1);
		battery1AR15(xid0, alias2, bmap2, 0);
		
		battery1AR16(xid, alias1, bmap, 0);
		battery1AR16(xid0, alias1, bmap, 1);
		battery1AR16(xid, alias2, bmap2, 1);
		battery1AR16(xid0, alias2, bmap2, 0);
		
		DatabaseManager.commitTransaction(alias1,xid);
		DatabaseManager.commitTransaction(alias1,xid0);
		DatabaseManager.commitTransaction(alias2,xid);
		DatabaseManager.commitTransaction(alias2,xid0);
	
		// Perform a checkpoint test on the passed db to verify checkpoint is operational
		battery18(alias1, bmap);

		System.out.println("BatteryKVTransactionAlias TEST BATTERY COMPLETE.");
		DatabaseManager.removeTransaction(alias1,xid);
		DatabaseManager.removeTransaction(alias1,xid0);
		DatabaseManager.removeTransaction(alias2,xid);
		DatabaseManager.removeTransaction(alias2,xid0);
	}
	/**
	 * Use 2 different interleaved transactions to insert to the same database without conflict.
	 * @param xid Transaction 1
	 * @param xid0 Transaction 2
	 * @param alias12 database alias we are operating upon; we just use it as convenient data to help form the key
	 * @param bmap3 TransactionalMap linked to the alias
	 * @throws Exception
	 */
	public static void battery1(TransactionId xid, TransactionId xid0, Alias alias12, TransactionalMap bmap3) throws Exception {
		System.out.println(xid+" and "+xid0+" KV Battery1 "+alias12);
		long tims = System.currentTimeMillis();
		int dupes = 0;
		int recs = 0;
		String fkey = null;
		int j = (int) bmap3.size(xid);
		if(bmap3.size(xid) > 0) {
			System.out.println("Cleaning exising DB "+alias12+" of "+j+" elements.");
			batteryCleanDB(alias12, bmap3);		
		}
		// increment by 2, storing even in passed xid, and odd in passed xid0
		// for each database will will pass opposite values
		for(int i = min; i < max; i+=2) {
			fkey = alias12+String.format(uniqKeyFmt, i);
			bmap3.put(xid, fkey, new Long(i));
			fkey = alias12+String.format(uniqKeyFmt, i+1);
			bmap3.put(xid0, fkey, new Long(i+1));
			recs+=2;
		}
		System.out.println("KV BATTERY1 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms. Stored "+recs+" records, rejected "+dupes+" dupes.");
	}
	
	/**
	 * Store another transaction with twice the max records, then roll it back and remove transaction.
	 * @param xid
	 * @param alias12
	 * @param bmap3  
	 * @throws Exception
	 */
	public static void battery11(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		System.out.println(xid+" KV Battery11 "+alias12);
		long tims = System.currentTimeMillis();
		int recs = 0;
		String fkey = null;
		for(int i = max; i < max*2; i++) {
			fkey = alias12+String.format(uniqKeyFmt, i);
			bmap2.put(xid, fkey, new Long(i));
			++recs;
		}
		if( recs > 0) {
			DatabaseManager.rollbackTransaction(alias12,xid);
			System.out.println("KV BATTERY11 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
		}
		DatabaseManager.removeTransaction(alias12,xid);
	}
	
	/**
	 * Test the higher level functions in the RelatrixKV.
	 * public Set entrySet()
	 * Returns a Set view of the mappings contained in this map. 
	 * The set's iterator returns the entries in ascending key order. 
	 * The set is backed by the map, so changes to the map are reflected in the set, and vice-versa.
	 * If the map is modified while an iteration over the set is in progress (except through the iterator's 
	 * own remove operation, or through the setValue operation on a map entry returned by the iterator) the results
	 * of the iteration are undefined. The set supports element removal, which removes the corresponding mapping from the map, 
	 * via the Iterator.remove, Set.remove, removeAll, retainAll and clear operations. 
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @param j 
	 * @throws Exception
	 */
	public static void battery1AR6(TransactionId xid, Alias alias12, TransactionalMap bmap3, int j) throws Exception {
		int i = min+j;
		long tims = System.currentTimeMillis();
		Iterator<?> its = bmap3.entrySet(xid);
		System.out.println(xid+" KV Battery1AR6 "+alias12);
		while(its.hasNext()) {
			Object nex =  its.next();
			Entry enex = (Entry)nex;
			//System.out.println(i+"="+nex);
			if(((Long)enex.getValue()).intValue() != i)
				System.out.println("RANGE KEY MISMATCH:"+i+" - "+nex);
			i+=2;
		}
		if( i != (max+j) ) { // account for odd record increment
			System.out.println("BATTERY1AR6 unexpected number of keys "+i);
			throw new Exception("BATTERY1AR6 unexpected number of keys "+i);
		}
		 System.out.println("BATTERY1AR6 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Testing of Iterator its = RelatrixKV.keySet;
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @param j controls even/odd transaction
	 * @throws Exception
	 */
	public static void battery1AR7(TransactionId xid, Alias alias12, TransactionalMap bmap3, int j) throws Exception {
		int i = min+j;
		long tims = System.currentTimeMillis();
		Iterator<?> its = bmap3.keySet(xid);
		System.out.println(xid+" KV Battery1AR7 "+alias12);
		while(its.hasNext()) {
			String nex = (String) its.next();
			if(Integer.parseInt(nex.substring(alias12.getAlias().length())) != i)
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+nex);
			i+=2;
		}
		if( i != (max+j) ) {
			System.out.println("KV BATTERY1AR7 unexpected number of keys "+i);
			throw new Exception("KV BATTERY1AR7 unexpected number of keys "+i);
		}
		 System.out.println("KV BATTERY1AR7 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Testing of contains, forward and back
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @param k 
	 * @throws Exception
	 */
	public static void battery1AR8(TransactionId xid, Alias alias12, TransactionalMap bmap3, int k) throws Exception {
		int i = min+k;
		System.out.println(xid+" KV Battery1AR8 "+alias12);
		long tims = System.currentTimeMillis();
		for(int j = min+k; j < max; j+=2) {
			String fkey = alias12+String.format(uniqKeyFmt, j);
			boolean bits = bmap3.contains(xid, fkey);
			if( !bits ) {
				System.out.println("KV BATTERY1A8 cant find contains key "+j);
				throw new Exception("KV BATTERY1AR8 unexpected cant find contains of key "+fkey);
			}
		}
		 System.out.println("KV BATTERY1AR8 FORWARD CONTAINS KEY TOOK "+(System.currentTimeMillis()-tims)+" ms.");
		 tims = System.currentTimeMillis();
		 for(int j = (max-(2-(1*k))); j > min; j-=2) {
				String fkey = alias12+String.format(uniqKeyFmt, j);
				boolean bits = bmap3.contains(xid, fkey);
				if( !bits ) {
					System.out.println("KV BATTERY1A8 cant find contains key "+j);
					throw new Exception("KV BATTERY1AR8 unexpected cant find contains of key "+fkey);
				}
			}
			 System.out.println("KV BATTERY1AR8 REVERSE CONTAINS KEY TOOK "+(System.currentTimeMillis()-tims)+" ms." );
		//i = max-1;
		tims = System.currentTimeMillis();
		for(int j = min+k; j < min+numLookupByValue; j+=2) {
			// careful here, have to do the conversion explicitly
			boolean bits = bmap3.containsValue(xid, (long)j);
			if( !bits ) {
				System.out.println("KV BATTERY1AR8 cant find contains value "+j);
				throw new Exception("KV BATTERY1AR8 unexpected number cant find contains of value "+i);
			}
		}
		System.out.println("KV BATTERY1AR8 FORWARD "+numLookupByValue+" CONTAINS VALUE TOOK "+(System.currentTimeMillis()-tims)+" ms.");
		tims = System.currentTimeMillis();
		for(int j = (max-(2-(1*k))); j > max-numLookupByValue ; j-=2) {
				// have to do the conversion explicitly
				boolean bits = bmap3.containsValue(xid, (long)j);
				if( !bits ) {
					System.out.println("KV BATTERY1AR8 cant find contains value "+j);
					throw new Exception("KV BATTERY1AR8 unexpected number cant find contains of value "+i);
				}
		}
		System.out.println("KV BATTERY1AR8 REVERSE "+numLookupByValue+" CONTAINS VALUE TOOK "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * 
	 * Testing of firstKey/first
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @param j 
	 * @throws Exception
	 */
	public static void battery1AR9(TransactionId xid, Alias alias12, TransactionalMap bmap3, int j) throws Exception {
		int i = min+j;
		long tims = System.currentTimeMillis();
		Object k = bmap3.firstKey(xid); // first key
		System.out.println(xid+" KV Battery1AR9 "+alias12);
		if( Integer.parseInt(((String)k).substring(alias12.getAlias().length())) != i ) {
			System.out.println("KV BATTERY1A9 cant find contains key "+i+" from "+k);
			throw new Exception("KV BATTERY1AR9 unexpected cant find contains of key "+i+" from "+k);
		}
		long ks = (long) bmap3.first(xid);
		if( ks != i) {
			System.out.println("KV BATTERY1A9 cant find contains value "+i+" from "+ks);
			throw new Exception("KV BATTERY1AR9 unexpected cant find contains of value "+i+" from "+ks);
		}
		System.out.println("KV BATTERY1AR9 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}

	/**
	 * test last and lastKey
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @param j 
	 * @throws Exception
	 */
	public static void battery1AR10(TransactionId xid, Alias alias12, TransactionalMap bmap3, int j) throws Exception {
		int i = (max-(2-(1*j)));
		long tims = System.currentTimeMillis();
		Object k = bmap3.lastKey(xid); // key
		System.out.println(xid+" KV Battery1AR10 "+alias12);
		if( Long.parseLong(((String) k).substring(alias12.getAlias().length())) != (long)i ) {
			System.out.println("KV BATTERY1AR10 cant find last key "+i+" from "+k);
			throw new Exception("KV BATTERY1AR10 unexpected cant find last of key "+i+" from "+k);
		}
		long ks = (long)bmap3.last(xid);
		if( ks != i) {
			System.out.println("KV BATTERY1AR10 cant find last value "+i+" from "+ks);
			throw new Exception("KV BATTERY1AR10 unexpected cant find last of key "+i+" from "+ks);
		}
		System.out.println("KV BATTERY1AR10 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * test size
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @param j 
	 * @throws Exception
	 */
	public static void battery1AR101(TransactionId xid, Alias alias12, TransactionalMap bmap3, int j) throws Exception {
		int i = max/2;
		long tims = System.currentTimeMillis();
		long bits = bmap3.size(xid);
		System.out.println(xid+" KV Battery1AR101 "+alias12);
		if( bits != i ) {
			System.out.println("KV BATTERY1AR101 size mismatch "+bits+" should be:"+i);
			throw new Exception("KV BATTERY1AR101 size mismatch "+bits+" should be "+i);
		}
		System.out.println("BATTERY1AR101 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * tailMap test
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @param j 
	 * @throws Exception
	 */
	public static void battery1AR11(TransactionId xid, Alias alias12, TransactionalMap bmap3, int j) throws Exception {
		long tims = System.currentTimeMillis();
		int i = min+j;
		String fkey = alias12+String.format(uniqKeyFmt, i);
		Iterator<?> its = bmap3.tailMap(xid, fkey);
		System.out.println(xid+" KV Battery1AR11 "+alias12);
		while(its.hasNext()) {
			String nex = (String) its.next();
			if(Integer.parseInt(nex.substring(alias12.getAlias().length())) != i) {
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE KEY MISMATCH:"+i+" - "+nex);
			}
			i+=2;
		}
		 System.out.println("BATTERY1AR11 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * tailmapKV
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @param j 
	 * @throws Exception
	 */
	public static void battery1AR12(TransactionId xid, Alias alias12, TransactionalMap bmap3, int j) throws Exception {
		long tims = System.currentTimeMillis();
		int i = min+j;
		String fkey = alias12+String.format(uniqKeyFmt, i);
		Iterator<?> its = bmap3.tailMapKV(xid, fkey);
		System.out.println(xid+" KV Battery1AR12 "+alias12);
		while(its.hasNext()) {
			Comparable nex = (Comparable) its.next();
			Map.Entry<String, Long> nexe = (Map.Entry<String,Long>)nex;
			if(Integer.parseInt(nexe.getKey().substring(alias12.getAlias().length())) != i) {
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE KEY MISMATCH:"+i+" - "+nex);
			}
			i+=2;
		}
		 System.out.println("BATTERY1AR12 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * findHeadMap - Returns a view of the portion of this map whose keys are strictly less than toKey.
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @param j 
	 * @throws Exception
	 */
	public static void battery1AR13(TransactionId xid, Alias alias12, TransactionalMap bmap3, int j) throws Exception {
		long tims = System.currentTimeMillis();
		int i = max;
		String fkey = alias12+String.format(uniqKeyFmt, i);
		Iterator<?> its = bmap3.headMap(xid, fkey);
		System.out.println(xid+" KV Battery1AR13 "+alias12);
		// with i at max, should catch them all
		i = min+j;
		while(its.hasNext()) {
			String nex = (String) its.next();
			if(Integer.parseInt(nex.substring(alias12.getAlias().length())) != i) {
			// Map.Entry
				System.out.println("KV RANGE 1AR13 KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE 1AR13 KEY MISMATCH:"+i+" - "+nex);
			}
			i+=2;
		}
		 System.out.println("BATTERY1AR13 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * headMapKV
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @param j 
	 * @throws Exception
	 */
	public static void battery1AR14(TransactionId xid, Alias alias12, TransactionalMap bmap3, int j) throws Exception {
		long tims = System.currentTimeMillis();
		int i = max;
		String fkey = alias12+String.format(uniqKeyFmt, i);
		Iterator<?> its = bmap3.headMapKV(xid, fkey);
		System.out.println(xid+" KV Battery1AR14 "+alias12);
		i = min+j;
		while(its.hasNext()) {
			Comparable nex = (Comparable) its.next();
			Map.Entry<String, Long> nexe = (Map.Entry<String,Long>)nex;
			if(Integer.parseInt(nexe.getKey().substring(alias12.getAlias().length())) != i) {
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE KEY MISMATCH:"+i+" - "+nex);
			}
			i+=2;
		}
		 System.out.println("BATTERY1AR14 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * subMap - Returns a view of the portion of this map whose keys range from fromKey, inclusive, to toKey, exclusive.
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @param k 
	 * @throws Exception
	 */
	public static void battery1AR15(TransactionId xid, Alias alias12, TransactionalMap bmap3, int k) throws Exception {
		long tims = System.currentTimeMillis();
		int i = min+k;
		int j = max;
		String fkey = alias12+String.format(uniqKeyFmt, i);
		// with j at max, should get them all since we stored to max -1
		String tkey = alias12+String.format(uniqKeyFmt, j);
		Iterator<?> its = bmap3.subMap(xid, fkey, tkey);
		System.out.println(xid+" KV Battery1AR15 "+alias12);
		while(its.hasNext()) {
			String nex = (String) its.next();
			if(Integer.parseInt(nex.substring((alias12.getAlias().length()))) != i) {
				System.out.println("KV RANGE 1AR15 KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE 1AR15 KEY MISMATCH:"+i+" - "+nex);
			}
			i+=2;
		}
		 System.out.println("BATTERY1AR15 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * subMapKV - Returns a view of the portion of this map whose keys range from fromKey, inclusive, to toKey, exclusive.
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @param k 
	 * @throws Exception
	 */
	public static void battery1AR16(TransactionId xid, Alias alias12, TransactionalMap bmap3, int k) throws Exception {
		long tims = System.currentTimeMillis();
		int i = min+k;
		int j = max;
		String fkey = alias12+String.format(uniqKeyFmt, i);
		// with j at max, should get them all since we stored to max -1
		String tkey = alias12+String.format(uniqKeyFmt, j);
		Iterator<?> its = bmap3.subMapKV(xid, fkey, tkey);
		System.out.println(xid+" KV Battery1AR16 "+alias12);
		// with i at max, should catch them all
		while(its.hasNext()) {
			Comparable nex = (Comparable) its.next();
			Map.Entry<String, Long> nexe = (Map.Entry<String,Long>)nex;
			if(Integer.parseInt(nexe.getKey().substring(alias12.getAlias().length())) != i) {
				System.out.println("KV RANGE 1AR16 KEY MISMATCH:"+i+" - "+nexe);
				throw new Exception("KV RANGE 1AR16 KEY MISMATCH:"+i+" - "+nexe);
			}
			i+=2;
		}
		 System.out.println("BATTERY1AR16 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * Get next transaction id,
	 * Insert half the keys, checkpoint, then insert other half, commit, then remove trans.
	 * @param alias12 
	 * @throws Exception
	 */
	public static void battery18(Alias alias12, TransactionalMap bmap2) throws Exception {
		TransactionId xid2 = DatabaseManager.getTransactionId();
		DatabaseManager.associateSession(xid2, bmap2);
		System.out.println(xid2+" KV Battery18 "+alias12);
		int max1 = max - (max/2);
		long tims = System.currentTimeMillis();
		int recs = 0;
		String fkey = null;
		for(int i = min; i < max1; i++) {
			fkey = alias12+String.format(uniqKeyFmt, i);
			bmap2.put(xid2, fkey, new Long(i));
			++recs;
		}
		System.out.println(xid2+" Checkpointing..");
		DatabaseManager.checkpointTransaction(alias12, xid2);
		for(int i = max1; i < max; i++) {
			fkey = alias12+String.format(uniqKeyFmt, i);
			bmap2.put(xid2, fkey, new Long(i));
			++recs;
		}
		DatabaseManager.commitTransaction(alias12,xid2);
		DatabaseManager.removeTransaction(alias12,xid2);
		System.out.println("KV BATTERY18 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms. Stored "+recs+" records.");
	}
	
	/**
	 * remove entries
	 * @param bmap3 
	 * @param alias12 
	 * @throws Exception
	 */
	private static void batteryCleanDB(Alias alias12, TransactionalMap bmap3) throws Exception {
		long tims = System.currentTimeMillis();
		TransactionId xid = DatabaseManager.getTransactionId();
		DatabaseManager.associateSession(xid, bmap3);
		System.out.println(alias12+" CleanDB "+xid);
		for(int i = min; i < max; i++) {
			String fkey = alias12+String.format(uniqKeyFmt, i);
			bmap3.remove(xid, fkey);
		}
		DatabaseManager.commitTransaction(alias12,xid);
		DatabaseManager.removeTransaction(alias12,xid);
		System.out.println("CleanDB SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
}
