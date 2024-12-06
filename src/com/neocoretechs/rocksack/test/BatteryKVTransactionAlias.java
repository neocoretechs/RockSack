package com.neocoretechs.rocksack.test;

import java.util.Iterator;
import java.util.Map;

import com.neocoretechs.rocksack.Alias;
import com.neocoretechs.rocksack.TransactionId;
import com.neocoretechs.rocksack.iterator.Entry;
import com.neocoretechs.rocksack.session.DatabaseManager;
import com.neocoretechs.rocksack.session.TransactionalMap;


/**
 * Transaction KV test battery with alias interleaved operations.
 * 
 * NOTES:
 * program argument is database tablespace i.e. C:/users/you/RockSack/
 * C:/users/you/RockSack should be valid path. C:/users/you/RockSack/ALIAS1java.lang.String, etc. will be created.
 * @author Jonathan Groff (C) NeoCoreTechs 2022
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
		DatabaseManager.setTableSpaceDir(argv[0]);
		TransactionId xid = DatabaseManager.getTransactionId();
		TransactionId xid0 = DatabaseManager.getTransactionId();
		bmap = DatabaseManager.getTransactionalMap(alias1, String.class, xid);
		DatabaseManager.associateSession(alias1, xid0, bmap);
		battery1(xid, xid0, alias1, bmap);
		// Test 1 commits the transaction id's xid xid0
		// expand these transactions to the next database
		bmap2 = DatabaseManager.getTransactionalMap(alias2, String.class, xid);
		DatabaseManager.associateSession(alias2, xid0, bmap2);
		battery1(xid, xid0, alias2, bmap2);
		TransactionId xid2 = DatabaseManager.getTransactionId();
		bmap2 = DatabaseManager.getTransactionalMap(alias2, String.class, xid2);
		// xid2 will be rolled back then removed
		battery11(xid2, alias2, bmap2);
		// xid, and xid0 shoudl still be valid
		battery1AR6(xid, alias1, bmap);
		battery1AR6(xid0, alias2, bmap2);
		battery1AR7(xid, alias1, bmap);
		battery1AR7(xid0, alias2, bmap2);
		battery1AR8(xid, alias1, bmap);
		battery1AR8(xid0, alias2, bmap2);
		battery1AR9(xid, alias1, bmap);
		battery1AR9(xid0, alias2, bmap2);
		battery1AR10(xid, alias1, bmap);
		battery1AR10(xid0, alias2, bmap2);
		battery1AR101(xid, alias1, bmap);
		battery1AR101(xid0, alias2, bmap2);
		battery1AR11(xid, alias1, bmap);
		battery1AR11(xid0, alias2, bmap2);
		battery1AR12(xid, alias1, bmap);
		battery1AR12(xid0, alias2, bmap2);
		battery1AR13(xid, alias1, bmap);
		battery1AR13(xid0, alias2, bmap2);
		battery1AR14(xid, alias1, bmap);
		battery1AR14(xid0, alias2, bmap2);
		battery1AR15(xid, alias1, bmap);
		battery1AR15(xid0, alias2, bmap2);
		battery1AR16(xid, alias1, bmap);
		battery1AR16(xid0, alias2, bmap2);
		battery1AR17(xid, alias1, bmap);
		battery1AR17(xid0, alias2, bmap2);
		// commit in this method
		battery18(xid, alias1, bmap);
		battery18(xid0, alias2, bmap2);
		System.out.println("BatteryKVTransactionAlias TEST BATTERY COMPLETE.");
		DatabaseManager.removeTransaction(alias1,xid);
		DatabaseManager.removeTransaction(alias1,xid0);
		DatabaseManager.removeTransaction(alias2,xid);
		DatabaseManager.removeTransaction(alias2,xid0);
	}
	/**
	 * Use 2 different interleaved transactions to insert to the same database without conflict.
	 * commit the 2 transactions.
	 * @param xid
	 * @param xid0 
	 * @param alias12 
	 * @param bmap3 
	 * @throws Exception
	 */
	public static void battery1(TransactionId xid, TransactionId xid0, Alias alias12, TransactionalMap bmap3) throws Exception {
		System.out.println(xid+" and "+xid0+" KV Battery1 "+alias12);
		long tims = System.currentTimeMillis();
		int dupes = 0;
		int recs = 0;
		String fkey = null;
		int j = min;
		j = (int) bmap3.size(xid);
		if(j > 0) {
			System.out.println("Cleaning DB of "+j+" elements.");
			batteryCleanDB(xid, alias12, bmap3);		
		}
		for(int i = min; i < max; i+=2) {
			fkey = alias12+String.format(uniqKeyFmt, i);
			bmap3.put(xid, fkey, new Long(i));
			fkey = alias12+String.format(uniqKeyFmt, i+1);
			bmap3.put(xid0, fkey, new Long(i+1));
			++recs;
		}
		DatabaseManager.commitTransaction(alias12,xid);
		DatabaseManager.commitTransaction(alias12,xid0);
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
			bmap2.put(xid, fkey, new Long(fkey));
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
	 * public Set<Map.Entry<K,V>> entrySet()
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
	 * @throws Exception
	 */
	public static void battery1AR6(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		int i = min;
		long tims = System.currentTimeMillis();
		Iterator<?> its = bmap3.entrySet(xid);
		System.out.println(xid+" KV Battery1AR6 "+alias12);
		while(its.hasNext()) {
			Object nex =  its.next();
			Entry enex = (Entry)nex;
			//System.out.println(i+"="+nex);
			if(((Long)enex.getValue()).intValue() != i)
				System.out.println("RANGE KEY MISMATCH:"+i+" - "+nex);
			else
				++i;
		}
		if( i != max ) {
			System.out.println("BATTERY1AR6 unexpected number of keys "+i);
			throw new Exception("BATTERY1AR6 unexpected number of keys "+i);
		}
		 System.out.println("BATTERY1AR6 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Testing of Iterator<?> its = RelatrixKV.keySet;
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @throws Exception
	 */
	public static void battery1AR7(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		int i = min;
		long tims = System.currentTimeMillis();
		Iterator<?> its = bmap3.keySet(xid);
		System.out.println(xid+" KV Battery1AR7 "+alias12);
		while(its.hasNext()) {
			String nex = (String) its.next();
			if(Integer.parseInt(nex) != i)
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+nex);
			else
				++i;
		}
		if( i != max ) {
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
	 * @throws Exception
	 */
	public static void battery1AR8(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		int i = min;
		System.out.println(xid+" KV Battery1AR8 "+alias12);
		long tims = System.currentTimeMillis();
		for(int j = min; j < max; j++) {
			String fkey = alias12+String.format(uniqKeyFmt, j);
			boolean bits = bmap3.contains(xid, fkey);
			if( !bits ) {
				System.out.println("KV BATTERY1A8 cant find contains key "+j);
				throw new Exception("KV BATTERY1AR8 unexpected cant find contains of key "+fkey);
			}
		}
		 System.out.println("KV BATTERY1AR8 FORWARD CONTAINS KEY TOOK "+(System.currentTimeMillis()-tims)+" ms.");
		 tims = System.currentTimeMillis();
		 for(int j = max-1; j > min; j--) {
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
		for(int j = min; j < min+numLookupByValue; j++) {
			// careful here, have to do the conversion explicitly
			boolean bits = bmap3.containsValue(xid, (long)j);
			if( !bits ) {
				System.out.println("KV BATTERY1AR8 cant find contains value "+j);
				throw new Exception("KV BATTERY1AR8 unexpected number cant find contains of value "+i);
			}
		}
		System.out.println("KV BATTERY1AR8 FORWARD "+numLookupByValue+" CONTAINS VALUE TOOK "+(System.currentTimeMillis()-tims)+" ms.");
		tims = System.currentTimeMillis();
		for(int j = max; j > max-numLookupByValue ; j--) {
				// careful here, have to do the conversion explicitly
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
	 * @throws Exception
	 */
	public static void battery1AR9(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		int i = min;
		long tims = System.currentTimeMillis();
		Object k = bmap3.firstKey(xid); // first key
		System.out.println(xid+" KV Battery1AR9 "+alias12);
		if( Integer.parseInt(((String)k).substring(alias12.getAlias().length())) != i ) {
			System.out.println("KV BATTERY1A9 cant find contains key "+i);
			throw new Exception("KV BATTERY1AR9 unexpected cant find contains of key "+i);
		}
		long ks = (long) bmap3.first(xid);
		if( ks != i) {
			System.out.println("KV BATTERY1A9 cant find contains value "+i);
			throw new Exception("KV BATTERY1AR9 unexpected cant find contains of value "+i);
		}
		System.out.println("KV BATTERY1AR9 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}

	/**
	 * test last and lastKey
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @throws Exception
	 */
	public static void battery1AR10(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		int i = max-1;
		long tims = System.currentTimeMillis();
		Object k = bmap3.lastKey(xid); // key
		System.out.println(xid+" KV Battery1AR10 "+alias12);
		if( Long.parseLong(((String) k).substring(alias12.getAlias().length())) != (long)i ) {
			System.out.println("KV BATTERY1AR10 cant find last key "+i);
			throw new Exception("KV BATTERY1AR10 unexpected cant find last of key "+i);
		}
		long ks = (long)bmap3.last(xid);
		if( ks != i) {
			System.out.println("KV BATTERY1AR10 cant find last value "+i);
			throw new Exception("KV BATTERY1AR10 unexpected cant find last of key "+i);
		}
		System.out.println("KV BATTERY1AR10 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * test size
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @throws Exception
	 */
	public static void battery1AR101(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		int i = max;
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
	 * @throws Exception
	 */
	public static void battery1AR11(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		long tims = System.currentTimeMillis();
		int i = min;
		String fkey = alias12+String.format(uniqKeyFmt, i);
		Iterator<?> its = bmap3.tailMap(xid, fkey);
		System.out.println(xid+" KV Battery1AR11 "+alias12);
		while(its.hasNext()) {
			String nex = (String) its.next();
			if(Integer.parseInt(nex.substring(alias12.getAlias().length())) != i) {
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE KEY MISMATCH:"+i+" - "+nex);
			}
			++i;
		}
		 System.out.println("BATTERY1AR11 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * tailmapKV
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @throws Exception
	 */
	public static void battery1AR12(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		long tims = System.currentTimeMillis();
		int i = min;
		String fkey = alias12+String.format(uniqKeyFmt, i);
		Iterator its = bmap3.tailMapKV(xid, fkey);
		System.out.println(xid+" KV Battery1AR12 "+alias12);
		while(its.hasNext()) {
			Comparable nex = (Comparable) its.next();
			Map.Entry<String, Long> nexe = (Map.Entry<String,Long>)nex;
			if(Integer.parseInt(nexe.getKey().substring(alias12.getAlias().length())) != i) {
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE KEY MISMATCH:"+i+" - "+nex);
			}
			++i;
		}
		 System.out.println("BATTERY1AR12 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * findHeadMap - Returns a view of the portion of this map whose keys are strictly less than toKey.
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @throws Exception
	 */
	public static void battery1AR13(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		long tims = System.currentTimeMillis();
		int i = max;
		String fkey = alias12+String.format(uniqKeyFmt, i);
		Iterator its = bmap3.headMap(xid, fkey);
		System.out.println(xid+" KV Battery1AR13 "+alias12);
		// with i at max, should catch them all
		i = min;
		while(its.hasNext()) {
			String nex = (String) its.next();
			if(Integer.parseInt(nex.substring(alias12.getAlias().length())) != i) {
			// Map.Entry
				System.out.println("KV RANGE 1AR13 KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE 1AR13 KEY MISMATCH:"+i+" - "+nex);
			}
			++i;
		}
		 System.out.println("BATTERY1AR13 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * headMapKV
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @throws Exception
	 */
	public static void battery1AR14(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		long tims = System.currentTimeMillis();
		int i = max;
		String fkey = alias12+String.format(uniqKeyFmt, i);
		Iterator<?> its = bmap3.headMapKV(xid, fkey);
		System.out.println(xid+" KV Battery1AR14 "+alias12);
		i = min;
		while(its.hasNext()) {
			Comparable nex = (Comparable) its.next();
			Map.Entry<String, Long> nexe = (Map.Entry<String,Long>)nex;
			if(Integer.parseInt(nexe.getKey().substring(alias12.getAlias().length())) != i) {
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE KEY MISMATCH:"+i+" - "+nex);
			}
			++i;
		}
		 System.out.println("BATTERY1AR14 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * subMap - Returns a view of the portion of this map whose keys range from fromKey, inclusive, to toKey, exclusive.
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @throws Exception
	 */
	public static void battery1AR15(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		long tims = System.currentTimeMillis();
		int i = min;
		int j = max;
		String fkey = alias12+String.format(uniqKeyFmt, i);
		// with j at max, should get them all since we stored to max -1
		String tkey = alias12+String.format(uniqKeyFmt, j);
		Iterator<?> its = bmap3.subMap(xid, fkey, tkey);
		System.out.println(xid+" KV Battery1AR15 "+alias12);
		// with i at max, should catch them all
		while(its.hasNext()) {
			String nex = (String) its.next();
			if(Integer.parseInt(nex.substring((alias12.getAlias().length()))) != i) {
				System.out.println("KV RANGE 1AR15 KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE 1AR15 KEY MISMATCH:"+i+" - "+nex);
			}
			++i;
		}
		 System.out.println("BATTERY1AR15 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * subMapKV - Returns a view of the portion of this map whose keys range from fromKey, inclusive, to toKey, exclusive.
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @throws Exception
	 */
	public static void battery1AR16(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		long tims = System.currentTimeMillis();
		int i = min;
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
			++i;
		}
		 System.out.println("BATTERY1AR16 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * remove entries
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @throws Exception
	 */
	public static void battery1AR17(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		long tims = System.currentTimeMillis();
		TransactionId xid2 = DatabaseManager.getTransactionId();
		TransactionalMap bmap2 = DatabaseManager.getTransactionalMap(String.class, xid2);
		// with j at max, should get them all since we stored to max -1
		//String tkey = String.format(uniqKeyFmt, j);
		System.out.println(xid+" KV Battery1AR17 "+alias12);
		// with i at max, should catch them all
		for(int i = min; i < max; i++) {
			String fkey = alias12+String.format(uniqKeyFmt, i);
			bmap2.remove(xid2, fkey);
			if(bmap2.contains(xid2, fkey)) { 
				System.out.println("KV RANGE 1AR17 KEY MISMATCH:"+i);
				throw new Exception("KV RANGE 1AR17 KEY MISMATCH:"+i);
			}
		}
		DatabaseManager.commitTransaction(alias12,xid2);
		long siz = bmap2.size(xid2);
		if(siz > 0) {
			Iterator<?> its = bmap2.entrySet(xid2);
			while(its.hasNext()) {
				Object nex = its.next();
				//System.out.println(i+"="+nex);
				System.out.println(nex);
			}
			System.out.println("KV RANGE 1AR17 KEY MISMATCH:"+siz+" > 0 after all deleted and committed");
			throw new Exception("KV RANGE 1AR17 KEY MISMATCH:"+siz+" > 0 after delete/commit");
		}
		DatabaseManager.removeTransaction(alias12,xid2);
		System.out.println("BATTERY1AR17 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * Get next transaction id,
	 * Insert half the keys, checkpoint, then insert other half, commit, then remove trans.
	 * @param xid
	 * @param alias12 
	 * @param bmap3 
	 * @throws Exception
	 */
	public static void battery18(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		System.out.println(xid+" KV Battery18 "+alias12);
		TransactionId xid2 = DatabaseManager.getTransactionId();
		TransactionalMap bmap2 = DatabaseManager.getTransactionalMap(alias12, String.class, xid2);
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
		DatabaseManager.checkpointTransaction(xid2);
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
	 * @param argv
	 * @throws Exception
	 */
	private static void batteryCleanDB(TransactionId xid, Alias alias12, TransactionalMap bmap3) throws Exception {
		long tims = System.currentTimeMillis();
		System.out.println(alias12+" CleanDB "+xid);
		for(int i = min; i < max; i++) {
			String fkey = alias12+String.format(uniqKeyFmt, i);
			bmap3.remove(xid, fkey);
		}
		 System.out.println("CleanDB SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
}
