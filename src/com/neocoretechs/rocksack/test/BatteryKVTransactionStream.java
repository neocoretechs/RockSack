package com.neocoretechs.rocksack.test;

import java.util.Map;
import java.util.stream.Stream;

import com.neocoretechs.rocksack.TransactionId;
import com.neocoretechs.rocksack.session.DatabaseManager;
import com.neocoretechs.rocksack.session.TransactionalMap;

/**
 * Yes, this should be a nice JUnit fixture someday. Test of KV transaction stream ops.
 * The static constant fields in the class control the key generation for the tests
 * In general, the keys and values are formatted according to uniqKeyFmt to produce
 * a series of canonically correct sort order strings for the DB in the range of min to max vals
 * In general most of the testing relies on checking order against expected values hence the importance of
 * canonical ordering in the sample strings.
 * Of course, you can substitute any class for the Strings here providing its Comparable.
 * This test the client side Java 8 streams obtained from the server
 * NOTES:
 * A database unique to this test module should be used.
 * program argument is database i.e. C:/users/you/RockSack/TestDB2 
 * C:/users/you/RockSack should be valid path. C:/users/you/RockSack/TestDB2java.lang.String will be created.
 * @author Jonathan Groff (C) NeoCoreTechs 2022
 *
 */
public class BatteryKVTransactionStream {
	public static boolean DEBUG = false;
	static String key = "This is a test"; // holds the base random key string for tests
	static String val = "Of a RockSack element!"; // holds base random value string
	static String uniqKeyFmt = "%0100d"; // base + counter formatted with this gives equal length strings for canonical ordering
	static int min = 0;
	static int max = 100000;
	static int numDelete = 100; // for delete test
	static int i;
	static int j;
	private static int dupes;
	private static int numLookupByValue = 10;
	static TransactionalMap bmap;
	/**
	* Main test fixture driver
	*/
	public static void main(String[] argv) throws Exception {
		DatabaseManager.setTableSpaceDir(argv[0]);
		TransactionId xid = DatabaseManager.getTransactionId();
		bmap = DatabaseManager.getTransactionalMap(String.class, xid);
		battery1(xid);	// build and store
		// Store records in another transaction then attempt to roll it back
		battery11(xid);  // build and store
		battery1AR6(xid);
		battery1AR7(xid);
		battery1AR8(xid); // search by value, slow operation no key
		battery1AR9(xid);
		battery1AR10(xid);
		battery1AR101(xid);
		battery1AR11(xid);
		battery1AR12(xid);
		battery1AR13(xid);
		battery1AR14(xid);
		battery1AR15(xid);
		battery1AR16(xid);
		battery1AR17(xid);
		battery18(xid);
		System.out.println("BatteryKVTransactionStream TEST BATTERY COMPLETE.");
		DatabaseManager.removeTransaction(xid);
	}
	/**
	 * Loads up on keys, should be 0 to max-1, or min, to max -1
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1(TransactionId xid) throws Exception {
		System.out.println("KV Battery1 "+xid);
		long tims = System.currentTimeMillis();
		int dupes = 0;
		int recs = 0;
		String fkey = null;
		int j = min;
		j = (int) bmap.size(xid);
		if(j > 0) {
			System.out.println("Cleaning DB of "+j+" elements.");
			batteryCleanDB(xid);		
		}
		for(int i = min; i < max; i++) {
			fkey = String.format(uniqKeyFmt, i);
				bmap.put(xid, fkey, Long.valueOf(i));
				++recs;
		}
		DatabaseManager.commitTransaction(xid);
		System.out.println("KV BATTERY1 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms. Stored "+recs+" records. ");
	}
	
	/**
	 * Store another transaction then roll it back.
	 * @param xid
	 * @throws Exception
	 */
	public static void battery11(TransactionId xid) throws Exception {
		System.out.println("KV Battery11 "+xid);
		long tims = System.currentTimeMillis();
		int recs = 0;
		String fkey = null;
		TransactionId xid2 = DatabaseManager.getTransactionId();
		DatabaseManager.associateSession(xid2, bmap);
		TransactionalMap bmap2 = bmap;
		for(int i = max; i < max*2; i++) {
			fkey = String.format(uniqKeyFmt, i);
			bmap2.put(xid2, fkey, Long.valueOf(fkey));
			++recs;
		}
		if( recs > 0) {
			DatabaseManager.rollbackTransaction(xid2);
			System.out.println("KV BATTERY11 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
		}
		DatabaseManager.removeTransaction(xid2);
	}
	
	/**
	 * Test the higher level functions in the map.
	 * public Set entrySet()
	 * Returns a Set view of the mappings contained in this map. 
	 * The set's stream returns the entries in ascending key order. 
	 * The set is backed by the map, so changes to the map are reflected in the set, and vice-versa.
	 * If the map is modified while an iteration over the set is in progress (except through the stream's 
	 * own remove operation, or through the setValue operation on a map entry returned by the stream) the results
	 * of the streaming are undefined. The set supports element removal, which removes the corresponding mapping from the map, 
	 * via the stream. Remove, Set.remove, removeAll, retainAll and clear operations. 
	 * It does not support the add or addAll operations.
	 * from battery1 we should have 0 to max, say 1000 keys of length 100
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1AR6(TransactionId xid) throws Exception {
		i = min;
		long tims = System.currentTimeMillis();
		Stream stream = bmap.entrySetStream(xid);
		System.out.println("KV Battery1AR6 "+xid);
		stream.forEach(e ->{
			if(((Map.Entry<String,Long>)e).getValue() != i) {
				System.out.println("RANGE KEY MISMATCH:"+i+" - "+e);
			} else
				++i;
		});
		if( i != max ) {
			System.out.println("BATTERY1AR6 unexpected number of keys "+i);
			throw new Exception("BATTERY1AR6 unexpected number of keys "+i);
		}
		 System.out.println("BATTERY1AR6 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Testing of Stream its = keySetStream;
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1AR7(TransactionId xid) throws Exception {
		i = min;
		long tims = System.currentTimeMillis();
		Stream stream = bmap.keySetStream(xid);
		System.out.println("KV Battery1AR7 "+xid);
		stream.forEach(e ->{
			if(Integer.parseInt((String)e) != i) {
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+e);
			} else
				++i;
		});
		if( i != max ) {
			System.out.println("KV BATTERY1AR7 unexpected number of keys "+i);
			throw new Exception("KV BATTERY1AR7 unexpected number of keys "+i);
		}
		 System.out.println("KV BATTERY1AR7 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Testing of contains
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1AR8(TransactionId xid) throws Exception {
		i = min;
		System.out.println("KV Battery1AR8 "+xid);
		long tims = System.currentTimeMillis();
		for(int j = min; j < max; j++) {
			String fkey = String.format(uniqKeyFmt, j);
			boolean bits = bmap.contains(xid, fkey);
			if( !bits ) {
				System.out.println("KV BATTERY1A8 cant find contains key "+j);
				throw new Exception("KV BATTERY1AR8 unexpected cant find contains of key "+fkey);
			}
		}
		 System.out.println("KV BATTERY1AR8 FORWARD CONTAINS KEY TOOK "+(System.currentTimeMillis()-tims)+" ms.");
		 tims = System.currentTimeMillis();
		 for(int j = max-1; j > min; j--) {
				String fkey = String.format(uniqKeyFmt, j);
				boolean bits = bmap.contains(xid, fkey);
				if( !bits ) {
					System.out.println("KV BATTERY1A8 cant find contains key "+j);
					throw new Exception("KV BATTERY1AR8 unexpected cant find contains of key "+fkey);
				}
			}
			 System.out.println("KV BATTERY1AR8 REVERSE CONTAINS KEY TOOK "+(System.currentTimeMillis()-tims)+" ms.");
		//i = max-1;
		tims = System.currentTimeMillis();
		for(int j = min; j < min+numLookupByValue; j++) {
			// careful here, have to do the conversion explicitly
			boolean bits = bmap.containsValue(xid, (long)j);
			if( !bits ) {
				System.out.println("KV BATTERY1AR8 cant find contains value "+j);
				throw new Exception("KV BATTERY1AR8 unexpected number cant find contains of value "+i);
			}
		}
		System.out.println("KV BATTERY1AR8 FORWARD "+numLookupByValue+" CONTAINS VALUE TOOK "+(System.currentTimeMillis()-tims)+" ms.");
		tims = System.currentTimeMillis();
		for(int j = max-1; j > max-numLookupByValue  ; j--) {
				// careful here, have to do the conversion explicitly
				boolean bits = bmap.containsValue(xid, (long)j);
				if( !bits ) {
					System.out.println("KV BATTERY1AR8 cant find contains value "+j);
					throw new Exception("KV BATTERY1AR8 unexpected number cant find contains of value "+i);
				}
		}
		System.out.println("KV BATTERY1AR8 REVERSE "+numLookupByValue+" CONTAINS VALUE TOOK "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * 
	 * Testing of first(), and firstValue
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1AR9(TransactionId xid) throws Exception {
		int i = min;
		long tims = System.currentTimeMillis();
		Object k = bmap.firstKey(xid); // first key
		System.out.println("KV Battery1AR9 "+xid);
		if( Integer.parseInt((String)k) != i ) {
			System.out.println("KV BATTERY1A9 cant find contains key "+i);
			throw new Exception("KV BATTERY1AR9 unexpected cant find contains of key "+i);
		}
		long ks = (long) bmap.first(xid);
		if( ks != i) {
			System.out.println("KV BATTERY1A9 cant find contains value "+i);
			throw new Exception("KV BATTERY1AR9 unexpected cant find contains of value "+i);
		}
		System.out.println("KV BATTERY1AR9 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}

	/**
	 * test last and lastKey
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1AR10(TransactionId xid) throws Exception {
		int i = max-1;
		long tims = System.currentTimeMillis();
		Object k = bmap.lastKey(xid); // key
		System.out.println("KV Battery1AR10 "+xid);
		if( Long.parseLong((String) k) != (long)i ) {
			System.out.println("KV BATTERY1AR10 cant find last key "+i);
			throw new Exception("KV BATTERY1AR10 unexpected cant find last of key "+i);
		}
		long ks = (long)bmap.last(xid);
		if( ks != i) {
			System.out.println("KV BATTERY1AR10 cant find last value "+i);
			throw new Exception("KV BATTERY1AR10 unexpected cant find last of key "+i);
		}
		System.out.println("KV BATTERY1AR10 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	* test size
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1AR101(TransactionId xid) throws Exception {
		int i = max;
		long tims = System.currentTimeMillis();
		long bits = bmap.size(xid);
		System.out.println("KV Battery1AR101 "+xid);
		if( bits != i ) {
			System.out.println("KV BATTERY1AR101 size mismatch "+bits+" should be:"+i);
			throw new Exception("KV BATTERY1AR101 size mismatch "+bits+" should be "+i);
		}
		System.out.println("BATTERY1AR101 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * findMap test, basically tailmap returning keys
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1AR11(TransactionId xid) throws Exception {
		long tims = System.currentTimeMillis();
		i = min;
		String fkey = String.format(uniqKeyFmt, i);
		Stream stream = bmap.tailMapStream(xid, fkey);
		System.out.println("KV Battery1AR11 "+xid);
		stream.forEach(e ->{
			if(Integer.parseInt((String)e) != i) {
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+e);
				throw new RuntimeException("KV RANGE KEY MISMATCH:"+i+" - "+e);
			}
			++i;
		});
		 System.out.println("BATTERY1AR11 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * findMapKV tailmapKV
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1AR12(TransactionId xid) throws Exception {
		long tims = System.currentTimeMillis();
		i = min;
		String fkey = String.format(uniqKeyFmt, i);
		Stream stream = bmap.tailMapKVStream(xid, fkey);
		System.out.println("KV Battery1AR12 "+xid);
		stream.forEach(e ->{
			if(Integer.parseInt(((Map.Entry<String,Long>)e).getKey()) != i) {
			// Map.Entry
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+e);
				throw new RuntimeException("KV RANGE KEY MISMATCH:"+i+" - "+e);
			}
			++i;
		});
		 System.out.println("BATTERY1AR12 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * headMapStream - Returns a view of the portion of this map whose keys are strictly less than toKey.
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1AR13(TransactionId xid) throws Exception {
		long tims = System.currentTimeMillis();
		i = max;
		String fkey = String.format(uniqKeyFmt, i);
		Stream stream = bmap.headMapStream(xid, fkey);
		System.out.println("KV Battery1AR13 "+xid);
		// with i at max, should catch them all
		i = min;
		stream.forEach(e ->{
			if(Integer.parseInt((String)e) != i) {
			// Map.Entry
				System.out.println("KV RANGE 1AR13 KEY MISMATCH:"+i+" - "+e);
				throw new RuntimeException("KV RANGE 1AR13 KEY MISMATCH:"+i+" - "+e);
			}
			++i;
		});
		 System.out.println("BATTERY1AR13 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * headMapKVStream
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1AR14(TransactionId xid) throws Exception {
		long tims = System.currentTimeMillis();
		i = max;
		String fkey = String.format(uniqKeyFmt, i);
		Stream stream = bmap.headMapKVStream(xid, fkey);
		System.out.println("KV Battery1AR14 "+xid);
		i = min;
		stream.forEach(e ->{
			if(Integer.parseInt(((Map.Entry<String,Long>)e).getKey()) != i) {
			// Map.Entry
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+e);
				throw new RuntimeException("KV RANGE KEY MISMATCH:"+i+" - "+e);
			}
			++i;
		});
		 System.out.println("BATTERY1AR14 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * subMapStream - Returns a view of the portion of this map whose keys range from fromKey, inclusive, to toKey, exclusive.
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1AR15(TransactionId xid) throws Exception {
		long tims = System.currentTimeMillis();
		i = min;
		j = max;
		String fkey = String.format(uniqKeyFmt, i);
		// with j at max, should get them all since we stored to max -1
		String tkey = String.format(uniqKeyFmt, j);
		Stream stream = bmap.subMapStream(xid, fkey, tkey);
		System.out.println("KV Battery1AR15 "+xid);
		// with i at max, should catch them all
		stream.forEach(e ->{
			if(Integer.parseInt((String) e) != i) {
			// Map.Entry
				System.out.println("KV RANGE 1AR15 KEY MISMATCH:"+i+" - "+e);
				throw new RuntimeException("KV RANGE 1AR15 KEY MISMATCH:"+i+" - "+e);
			}
			++i;
		});
		 System.out.println("BATTERY1AR15 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * subMapKVStream - Returns a view of the portion of this map whose keys range from fromKey, inclusive, to toKey, exclusive.
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1AR16(TransactionId xid) throws Exception {
		long tims = System.currentTimeMillis();
		i = min;
		j = max;
		String fkey = String.format(uniqKeyFmt, i);
		// with j at max, should get them all since we stored to max -1
		String tkey = String.format(uniqKeyFmt, j);
		Stream stream = bmap.subMapKVStream(xid, fkey, tkey);
		System.out.println("KV Battery1AR16 "+xid);
		// with i at max, should catch them all
		stream.forEach(e ->{
			if(Integer.parseInt(((Map.Entry<String,Long>)e).getKey()) != i) {
			// Map.Entry
				System.out.println("KV RANGE 1AR16 KEY MISMATCH:"+i+" - "+e);
				throw new RuntimeException("KV RANGE 1AR16 KEY MISMATCH:"+i+" - "+e);
			}
			++i;
		});
		 System.out.println("BATTERY1AR16 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * remove entries
	 * @param xid
	 * @throws Exception
	 */
	public static void battery1AR17(TransactionId xid) throws Exception {
		long tims = System.currentTimeMillis();
		//int i = min;
		//int j = max;
		TransactionId xid2 = DatabaseManager.getTransactionId();
		DatabaseManager.associateSession(xid2, bmap);
		TransactionalMap bmap2 = bmap;
		// with j at max, should get them all since we stored to max -1
		//String tkey = String.format(uniqKeyFmt, j);
		System.out.println("KV Battery1AR17 "+xid);
		// with i at max, should catch them all
		for(int i = min; i < max; i++) {
			String fkey = String.format(uniqKeyFmt, i);
			bmap2.remove(xid2, fkey);
			// Map.Entry
			if(bmap2.contains(xid2, xid2.getTransactionId())) { 
				System.out.println("KV RANGE 1AR17 KEY MISMATCH:"+i);
				throw new Exception("KV RANGE 1AR17 KEY MISMATCH:"+i);
			}
		}
		DatabaseManager.commitTransaction(xid2);
		long siz = bmap2.size(xid2);
		i = 0;
		if(siz > 0) {
			Stream stream = bmap2.entrySetStream(xid2);
			stream.forEach(e ->{
				if(((Map.Entry<String,Long>)e).getValue() != i) {
					System.out.println("RANGE KEY MISMATCH:"+i+" - "+e);
				}
				System.out.println(i+"="+e);
				++i;
			});
			System.out.println("KV RANGE 1AR17 KEY MISMATCH:"+siz+" > 0 after all deleted and committed. Total="+i);
			throw new Exception("KV RANGE 1AR17 KEY MISMATCH:"+siz+" > 0 after delete/commit");
		}
		DatabaseManager.removeTransaction(xid2);
		System.out.println("BATTERY1AR17 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}

	/**
	 * Loads up on keys, should be 0 to max-1, or min, to max -1
	 * @param xid
	 * @throws Exception
	 */
	public static void battery18(TransactionId xid) throws Exception {
		System.out.println("KV Battery18 "+xid);
		TransactionId xid2 = DatabaseManager.getTransactionId();
		DatabaseManager.associateSession(xid2, bmap);
		TransactionalMap bmap2 = bmap;
		int max1 = max - 50000;
		long tims = System.currentTimeMillis();
		int recs = 0;
		String fkey = null;
		for(int i = min; i < max1; i++) {
			fkey = String.format(uniqKeyFmt, i);
			bmap2.put(xid2, fkey, Long.valueOf(i));
			++recs;
		}
		System.out.println("Checkpointing.."+xid2);
		DatabaseManager.checkpointTransaction(xid2);
		for(int i = max1; i < max; i++) {
			fkey = String.format(uniqKeyFmt, i);
			bmap2.put(xid2, fkey, Long.valueOf(i));
			++recs;
		}
		DatabaseManager.commitTransaction(xid2);
		DatabaseManager.removeTransaction(xid2);
		System.out.println("KV BATTERY18 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms. Stored "+recs+" records.");
	}
	
	
	/**
	 * remove entries, we do this in the current transaction
	 * @param argv
	 * @throws Exception
	 */
	private static void batteryCleanDB(TransactionId xid) throws Exception {
		long tims = System.currentTimeMillis();
		//int i = min;
		//int j = max;
		// with j at max, should get them all since we stored to max -1
		//String tkey = String.format(uniqKeyFmt, j);
		System.out.println("CleanDB "+xid);
		// with i at max, should catch them all
		for(int i = min; i < max; i++) {
			String fkey = String.format(uniqKeyFmt, i);
			bmap.remove(xid, fkey);
		}
		 System.out.println("CleanDB SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
}
