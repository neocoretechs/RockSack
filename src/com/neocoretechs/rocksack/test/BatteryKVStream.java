package com.neocoretechs.rocksack.test;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import com.neocoretechs.rocksack.KeyValue;
import com.neocoretechs.rocksack.session.BufferedMap;
import com.neocoretechs.rocksack.session.DatabaseManager;

/**
 * Yes, this should be a nice JUnit fixture someday. Test of embedded KV stream retrieval ops.
 * The static constant fields in the class control the key generation for the tests
 * In general, the keys and values are formatted according to uniqKeyFmt to produce
 * a series of canonically correct sort order strings for the DB in the range of min to max vals
 * In general most of the testing relies on checking order against expected values hence the importance of
 * canonical ordering in the sample strings.
 * Of course, you can substitute any class for the Strings here providing its Comparable.
 * This tests the Java 8 streams obtained from the server
 * NOTES:
 * A database unique to this test module should be used.
 * program argument is database i.e. C:/users/you/RockSack/TestDB2 
 * C:/users/you/RockSack should be valid path. C:/users/you/RockSack/TestDB2java.lang.String will be created.
 * @author Jonathan Groff (C) NeoCoreTechs 2022
 *
 */
public class BatteryKVStream {
	public static boolean DEBUG = false;
	static String key = "This is a test"; // holds the base random key string for tests
	static String val = "Of a RockSack element!"; // holds base random value string
	static String uniqKeyFmt = "%0100d"; // base + counter formatted with this gives equal length strings for canonical ordering
	static int min = 0;
	static int max = 100000;
	static int numDelete = 100; // for delete test
	static int numLookupByValue = 10; // lookup by value quite slow
	static int i;
	static int j;
	static BufferedMap bmap;
	static long timx = System.currentTimeMillis();
	/**
	* Main test fixture driver
	*/
	public static void main(String[] argv) throws Exception {
		DatabaseManager.setTableSpaceDir(argv[0]);
		bmap = DatabaseManager.getMap(String.class);
		battery1(argv);	// build and store
		battery11(argv);  // build and store
		battery1AR6(argv);
		battery1AR7(argv);
		battery1AR8(argv); // search by value, slow operation no key
		battery1AR9(argv);
		battery1AR10(argv);
		battery1AR101(argv);
		battery1AR11(argv);
		battery1AR12(argv);
		battery1AR13(argv);
		battery1AR14(argv);
		battery1AR14A(argv);
		battery1AR15(argv);
		battery1AR16(argv);
		battery1AR17(argv);
		battery18(argv);
		System.out.println("BatteryKVStream TEST BATTERY COMPLETE.");
	}
	/**
	 * Loads up on keys, should be 0 to max-1, or min, to max -1
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1(String[] argv) throws Exception {
		System.out.println("KV Battery1 ");
		long tims = System.currentTimeMillis();
		int recs = 0;
		String fkey = null;
		long j = bmap.size();
		if(j > 0) {
			System.out.println("Cleaning DB of "+j+" elements.");
			battery1AR17(argv);		
		}
		for(int i = min; i < max; i++) {
			fkey = String.format(uniqKeyFmt, i);
			bmap.put(fkey, Long.valueOf(i));
			++recs;
		}
		System.out.println("KV BATTERY1 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms. Stored "+recs+" records.");
	}
	
	/**
	 * Tries to store partial key that should match existing keys, should reject all
	 * @param argv
	 * @throws Exception
	 */
	public static void battery11(String[] argv) throws Exception {
		System.out.println("KV Battery11 ");
		long tims = System.currentTimeMillis();
		int recs = 0;
		String fkey = null;
		for(int i = min; i < max; i++) {
			fkey = String.format(uniqKeyFmt, i);
				Object o = bmap.get(fkey);
				KeyValue kv = (KeyValue)o;
				if(i != ((Long)kv.getmValue()).intValue()) {
					System.out.println("RANGE KEY MISMATCH for 'get':"+i+" - "+o);
					++recs;
				}
		}
		if( recs > 0) {
			System.out.println("KV BATTERY11 FAIL, failed to get "+recs);
		} else {
			System.out.println("KV BATTERY11 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
		}
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
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR6(String[] argv) throws Exception {
		i = min;
		long tims = System.currentTimeMillis();
		Stream stream = bmap.entrySetStream();
		System.out.println("KV Battery1AR6");
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
	 * Testing of Stream its = bmap.keySetStream;
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR7(String[] argv) throws Exception {
		i = min;
		long tims = System.currentTimeMillis();
		Stream stream = bmap.keySetStream();
		System.out.println("KV Battery1AR7");
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
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR8(String[] argv) throws Exception {
		i = min;
		System.out.println("KV Battery1AR8");
		long tims = System.currentTimeMillis();
		for(int j = min; j < max; j++) {
			String fkey = String.format(uniqKeyFmt, j);
			boolean bits = bmap.contains(fkey);
			if( !bits ) {
				System.out.println("KV BATTERY1A8 cant find contains key "+j);
				throw new Exception("KV BATTERY1AR8 unexpected cant find contains of key "+fkey);
			}
		}
		 System.out.println("KV BATTERY1AR8 FORWARD CONTAINS KEY TOOK "+(System.currentTimeMillis()-tims)+" ms.");
		 tims = System.currentTimeMillis();
		 for(int j = max-1; j > min; j--) {
				String fkey = String.format(uniqKeyFmt, j);
				boolean bits = bmap.contains(fkey);
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
			boolean bits = bmap.containsValue((long)j);
			if( !bits ) {
				System.out.println("KV BATTERY1AR8 cant find contains value "+j);
				throw new Exception("KV BATTERY1AR8 unexpected number cant find contains of value "+i);
			}
		}
		System.out.println("KV BATTERY1AR8 FORWARD "+numLookupByValue+" CONTAINS VALUE TOOK "+(System.currentTimeMillis()-tims)+" ms.");
		tims = System.currentTimeMillis();
		for(int j = max-1; j > max-numLookupByValue; j--) {
				// careful here, have to do the conversion explicitly
				boolean bits = bmap.containsValue((long)j);
				if( !bits ) {
					System.out.println("KV BATTERY1AR8 cant find contains value "+j);
					throw new Exception("KV BATTERY1AR8 unexpected number cant find contains of value "+i);
				}
		}
		System.out.println("KV BATTERY1AR8 REVERSE "+numLookupByValue+" CONTAINS VALUE TOOK "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * 
	 * Testing of first()
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR9(String[] argv) throws Exception {
		int i = min;
		long tims = System.currentTimeMillis();
		Object k = bmap.firstKey(); // first key
		System.out.println("KV Battery1AR9");
		if( Integer.parseInt((String)k) != i ) {
			System.out.println("KV BATTERY1A9 cant find contains key "+i);
			throw new Exception("KV BATTERY1AR9 unexpected cant find contains of key "+i);
		}
		long ks = (long) bmap.first();
		if( ks != i) {
			System.out.println("KV BATTERY1A9 cant find contains value "+i);
			throw new Exception("KV BATTERY1AR9 unexpected cant find contains of value "+i);
		}
		System.out.println("KV BATTERY1AR9 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}

	/**
	 * test last 
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR10(String[] argv) throws Exception {
		int i = max-1;
		long tims = System.currentTimeMillis();
		Object k = bmap.lastKey(); // key
		System.out.println("KV Battery1AR10");
		if( Long.parseLong((String) k) != (long)i ) {
			System.out.println("KV BATTERY1AR10 cant find last key "+i);
			throw new Exception("KV BATTERY1AR10 unexpected cant find last of key "+i);
		}
		long ks = (long)bmap.last();
		if( ks != i) {
			System.out.println("KV BATTERY1AR10 cant find last value "+i);
			throw new Exception("KV BATTERY1AR10 unexpected cant find last of key "+i);
		}
		System.out.println("KV BATTERY1AR10 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	* test size
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR101(String[] argv) throws Exception {
		int i = max;
		long tims = System.currentTimeMillis();
		long bits = bmap.size();
		System.out.println("KV Battery1AR101");
		if( bits != i ) {
			System.out.println("KV BATTERY1AR101 size mismatch "+bits+" should be:"+i);
			throw new Exception("KV BATTERY1AR101 size mismatch "+bits+" should be "+i);
		}
		System.out.println("BATTERY1AR101 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * tailMapStream test, basically tailmap returning keys
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR11(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		i = min;
		String fkey = String.format(uniqKeyFmt, i);
		Stream stream = bmap.tailMapStream(fkey);
		System.out.println("KV Battery1AR11");
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
	 * tailmapKVStream
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR12(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		i = min;
		String fkey = String.format(uniqKeyFmt, i);
		Stream stream = bmap.tailMapKVStream(fkey);
		System.out.println("KV Battery1AR12");
		stream.forEach(e ->{
			if(Integer.parseInt(((Map.Entry<String,Long>)e).getKey()) != i) {
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+e);
				throw new RuntimeException("KV RANGE KEY MISMATCH:"+i+" - "+e);
			}
			++i;
		});
		 System.out.println("BATTERY1AR12 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * headMapStream - Returns a view of the portion of this map whose keys are strictly less than toKey.
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR13(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		i = max;
		String fkey = String.format(uniqKeyFmt, i);
		Stream stream = bmap.headMapStream(fkey);
		System.out.println("KV Battery1AR13");
		// with i at max, should catch them all
		i = min;
		stream.forEach(e ->{
			if(Integer.parseInt((String)e) != i) {
				System.out.println("KV RANGE 1AR13 KEY MISMATCH:"+i+" - "+e);
				throw new RuntimeException("KV RANGE 1AR13 KEY MISMATCH:"+i+" - "+e);
			}
			++i;
		});
		 System.out.println("BATTERY1AR13 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * headMapKVStream
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR14(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		i = max;
		String fkey = String.format(uniqKeyFmt, i);
		Stream stream = bmap.headMapKVStream(fkey);
		System.out.println("KV Battery1AR14");
		i = min;
		stream.forEach(e ->{
			if(Integer.parseInt(((Map.Entry<String,Long>)e).getKey()) != i) {
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+e);
				throw new RuntimeException("KV RANGE KEY MISMATCH:"+i+" - "+e);
			}
			++i;
		});
		 System.out.println("BATTERY1AR14 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * headMapKVStream with minimum check
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR14A(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		i = min;
		String fkey = String.format(uniqKeyFmt, i);
		Stream stream = bmap.headMapKVStream(fkey);
		System.out.println("KV Battery1AR14A");
		// should get none
		stream.forEach(e ->{
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+e);
				throw new RuntimeException("KV RANGE KEY MISMATCH:"+i+" - "+e);
		});
		System.out.println("BATTERY1AR14 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * subMapStream - Returns a view of the portion of this map whose keys range from fromKey, inclusive, to toKey, exclusive.
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR15(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		i = min;
		j = max;
		String fkey = String.format(uniqKeyFmt, i);
		// with j at max, should get them all since we stored to max -1
		String tkey = String.format(uniqKeyFmt, j);
		Stream stream = bmap.subMapStream(fkey, tkey);
		System.out.println("KV Battery1AR15");
		// with i at max, should catch them all
		stream.forEach(e ->{
			if(Integer.parseInt((String) e) != i) {
			// Map.Entry
				System.out.println("KV RANGE 1AR15 KEY MISMATCH:"+i+" - "+e);
				//throw new Exception("KV RANGE 1AR15 KEY MISMATCH:"+i+" - "+e);
			}
			++i;
		});
		 System.out.println("BATTERY1AR15 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * subMapKVStream - Returns a view of the portion of this map whose keys/values of Entry range from fromKey, inclusive, to toKey, exclusive.
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR16(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		i = min;
		j = max;
		String fkey = String.format(uniqKeyFmt, i);
		// with j at max, should get them all since we stored to max -1
		String tkey = String.format(uniqKeyFmt, j);
		Stream stream = bmap.subMapKVStream(fkey, tkey);
		System.out.println("KV Battery1AR16");
		// with i at max, should catch them all
		stream.forEach(e ->{
			if(Integer.parseInt(((Map.Entry<String,Long>)e).getKey()) != i) {
			// Map.Entry
				System.out.println("KV RANGE 1AR16 KEY MISMATCH:"+i+" - "+e);
				//throw new Exception("KV RANGE 1AR16 KEY MISMATCH:"+i+" - "+e);
			}
			++i;
		});
		 System.out.println("BATTERY1AR16 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * remove entries
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR17(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		//int i = min;
		//int j = max;
		// with j at max, should get them all since we stored to max -1
		//String tkey = String.format(uniqKeyFmt, j);
		System.out.println("KV Battery1AR17");
		long siz = bmap.size();
		timx = System.currentTimeMillis();
		i = 0;
		if(siz > 0) {
			Stream stream = bmap.keySetStream();
			stream.forEach(e ->{
				//System.out.println(i+"="+key);
				try {
					bmap.remove((Comparable) e);
					++i;
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				if((System.currentTimeMillis()-timx) > 5000) {
					System.out.println("Delete Element "+i);
					timx = System.currentTimeMillis();
				}
			});
		}
		siz = bmap.size();
		if(siz > 0) {
			Stream stream = bmap.entrySetStream();
			stream.forEach(e ->{
				//System.out.println(i+"="+key);
				System.out.println(key+"="+e);
			});
			System.out.println("KV RANGE 1AR17 KEY MISMATCH:"+siz+" > 0 after all deleted and committed");
			//throw new Exception("KV RANGE 1AR17 KEY MISMATCH:"+siz+" > 0 after delete/commit");
		}
		 System.out.println("BATTERY1AR17 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Loads up on keys, should be 0 to max-1, or min, to max -1
	 * @param argv
	 * @throws Exception
	 */
	public static void battery18(String[] argv) throws Exception {
		System.out.println("KV Battery18 ");
		int max1 = max - 50000;
		long tims = System.currentTimeMillis();
		int dupes = 0;
		int recs = 0;
		String fkey = null;
		for(int i = min; i < max1; i++) {
			fkey = String.format(uniqKeyFmt, i);
				bmap.put(fkey, Long.valueOf(i));
				++recs;
		}
		long s = bmap.size();
		if(s != max1)
			System.out.println("Size at halway point of restore incorrect:"+s+" should be "+max1);
		for(int i = max1; i < max; i++) {
			fkey = String.format(uniqKeyFmt, i);
			bmap.put(fkey, Long.valueOf(i));
			++recs;
		}
		System.out.println("KV BATTERY18 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms. Stored "+recs+" records ");
	}
	
}
