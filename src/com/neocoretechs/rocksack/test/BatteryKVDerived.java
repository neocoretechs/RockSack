package com.neocoretechs.rocksack.test;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import com.neocoretechs.rocksack.DatabaseClass;
import com.neocoretechs.rocksack.KeyValue;
import com.neocoretechs.rocksack.iterator.Entry;
import com.neocoretechs.rocksack.session.BufferedMap;
import com.neocoretechs.rocksack.session.BufferedMap;
import com.neocoretechs.rocksack.session.DatabaseManager;


/**
 * Test the derived class storage in main database via ColumnFamily.
 * 50/50 split of based and derived classes. We will always refer to base class because
 * downcast will fail. Base and derived classes will be 2 maps effectively referring to same column
 * due to annotation attributes. See classes at bottom. There are a few caveats with total order
 * and serialization adding extra info. Externalization may be in order for completely predictable behavior in all
 * cases.
 * NOTES:
 * A database unique to this test module should be used.
 * program argument is database i.e. /users/you/RockSack/TestDB2 
 * /users/you/RockSack should be valid path. /usrs/you/TestDB2RockSackcom.neocoretechs.rocksack.test.BatteryKVDerived$Based will be created.
 * @author Jonathan Groff (C) NeoCoreTechs 2022
 *
 */
public class BatteryKVDerived {
	public static boolean DEBUG = false;
	static String key = "This is a test"; // holds the base random key string for tests
	static String val = "Of a RockSack element!"; // holds base random value string
	static String uniqKeyFmt = "%0100d"; // base + counter formatted with this gives equal length strings for canonical ordering
	static int min = 0;
	static int max = 100000;
	static int numDelete = 100; // for delete test
	static BufferedMap bmap;
	static BufferedMap bmapd;
	/**
	* Main test fixture driver
	*/
	public static void main(String[] argv) throws Exception {
		DatabaseManager.setTableSpaceDir(argv[0]);
		bmap = DatabaseManager.getMap(Based.class);
		bmapd = (BufferedMap) DatabaseManager.getMap(Derived.class);
		battery1AR17(argv);	
		battery18(argv);
		battery11(argv);
		battery1AR6(argv);
		battery1AR7(argv);
		battery1AR8(argv);
		battery1AR9(argv);
		battery1AR10(argv);
		battery1AR101(argv);
		battery1AR11(argv);
		battery1AR12(argv);
		battery1AR13(argv);
		battery1AR14(argv);
		battery1AR15(argv);
		battery1AR16(argv);
		battery1AR17(argv);
		battery18(argv);
		 System.out.println("BatteryKV TEST BATTERY COMPLETE.");
		
	}
	
	/**
	 * 50/50 split on base and derived. We have to take care because serialization will add extra info
	 * that will effect total order, yet our overriden compare method will not recognize it.
	 * @param argv
	 * @throws Exception
	 */
	public static void battery11(String[] argv) throws Exception {
		System.out.println("KV Battery11 ");
		long tims = System.currentTimeMillis();
		int recs = 0;
		Based fkey = null;
		for(int i = min; i < max/2; i++) {
			fkey = new Based(String.format(uniqKeyFmt, i));
				Object o = bmap.get(fkey);
				KeyValue kv = (KeyValue)o;
				if(i != ((Long)kv.getmValue()).intValue()) {
					System.out.println("RANGE KEY MISMATCH for 'get':"+i+" - "+o);
					++recs;
				}
		}
		if( recs > 0) {
			System.out.println("KV BATTERY11 FAIL, base keys mismatched: "+recs);
		} else {
			System.out.println("KV BATTERY11 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
		}
		tims = System.currentTimeMillis();
		recs = 0;
		for(int i = max/2; i < max; i++) {
			Derived dkey = new Derived(String.format(uniqKeyFmt, i));
				Object o = bmapd.get(dkey);
				KeyValue kv = (KeyValue)o;
				if(i != ((Long)kv.getmValue()).intValue()) {
					System.out.println("KV BATTERY11 FAIL, derived key mismatched: "+i+" key:"+o);
					++recs;
				}
		}
		if( recs > 0) {
			System.out.println("KV BATTERY11 FAIL, derived keys mismatched: "+recs);
		} else {
			System.out.println("KV BATTERY11 SUCCESS derived in "+(System.currentTimeMillis()-tims)+" ms.");
		}
	}
	
	/**
	 * Test the higher level functions in the map.
	 * public Set entrySet()
	 * Returns a Set view of the mappings contained in this map. 
	 * The set's iterator returns the entries in ascending key order. 
	 * The set is backed by the map, so changes to the map are reflected in the set, and vice-versa.
	 * If the map is modified while an iteration over the set is in progress (except through the iterator's 
	 * own remove operation, or through the setValue operation on a map entry returned by the iterator) the results
	 * of the iteration are undefined. The set supports element removal, which removes the corresponding mapping from the map, 
	 * via the Iterator.remove, Set.remove, removeAll, retainAll and clear operations. 
	 * It does not support the add or addAll operations.
	 * from battery1 we should have 0 to max, say 1000 keys of length 100
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR6(String[] argv) throws Exception {
		int i = min;
		long tims = System.currentTimeMillis();
		Iterator<?> its = bmap.entrySet();
		System.out.println("KV Battery1AR6");
		while(its.hasNext()) {
			Entry nex = (Entry) its.next();
			//System.out.println(i+"="+nex);
			if(((Long)nex.getValue()).intValue() != i)
				System.out.println("RANGE KEY MISMATCH:"+i+" - "+nex);
			else
				++i;
		}
		if( i != max ) {
			System.out.println("BATTERY1AR6 unexpected number of keys "+i);
			throw new Exception("BATTERY1AR6 unexpected number of keys "+i);
		}
		i = min;
		its = bmapd.entrySet();
		System.out.println("KV Battery1AR6 derived");
		while(its.hasNext()) {
			Entry nex = (Entry) its.next();
			//System.out.println(i+"="+nex);
			if(((Long)nex.getValue()).intValue() != i)
				System.out.println("RANGE KEY MISMATCH derived:"+i+" - "+nex);
			else
				++i;
		}
		if( i != max ) {
			System.out.println("BATTERY1AR6 derived unexpected number of keys "+i);
			throw new Exception("BATTERY1AR6 derived unexpected number of keys "+i);
		}
		 System.out.println("BATTERY1AR6 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Testing of Iterator its = keySet;
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR7(String[] argv) throws Exception {
		int i = min;
		long tims = System.currentTimeMillis();
		Iterator<?> its = bmap.keySet();
		System.out.println("KV Battery1AR7");
		while(its.hasNext()) {
			Based nex = (Based) its.next();
			// Map.Entry
			if(Integer.parseInt(nex.basedKey) != i)
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+nex);
			else
				++i;
		}
		if( i != max ) {
			System.out.println("KV BATTERY1AR7 unexpected number of keys "+i);
			throw new Exception("KV BATTERY1AR7 unexpected number of keys "+i);
		}
		i = min;
		its = bmapd.keySet();
		System.out.println("KV Battery1AR7 derived");
		while(its.hasNext()) {
			Based nex = (Based) its.next();
			// Map.Entry
			if(Integer.parseInt(nex.basedKey) != i)
				System.out.println("KV RANGE KEY MISMATCH derived:"+i+" - "+nex);
			else
				++i;
		}
		if( i != max ) {
			System.out.println("KV BATTERY1AR7 derived unexpected number of keys "+i);
			throw new Exception("KV BATTERY1AR7 derived unexpected number of keys "+i);
		}
		 System.out.println("KV BATTERY1AR7 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * Testing of contains.
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR8(String[] argv) throws Exception {
		int i = min;
		long tims = System.currentTimeMillis();
		Based fkey = new Based(String.format(uniqKeyFmt, i));
		boolean bits = bmap.contains(fkey);
		System.out.println("KV Battery1AR8");
		if( !bits ) {
			System.out.println("KV BATTERY1A8 cant find contains key "+i);
			throw new Exception("KV BATTERY1AR8 unexpected cant find contains of key "+fkey);
		}
		i = max-1;
		// careful here, have to do the conversion explicitly
		bits = bmap.containsValue((long)i);
		if( !bits ) {
			System.out.println("KV BATTERY1AR8 unexpected cant find contains key "+i);
			throw new Exception("KV BATTERY1AR8 unexpected number cant find contains of value "+i);
		}
		fkey = new Derived(String.format(uniqKeyFmt, i));
		bits = bmapd.contains(fkey);
		System.out.println("KV Battery1AR8 derived");
		if( !bits ) {
			System.out.println("KV BATTERY1A8 derived cant find contains key "+i);
			throw new Exception("KV BATTERY1AR8 derived unexpected cant find contains of key "+fkey);
		}
		i = max-1;
		// careful here, have to do the conversion explicitly
		bits = bmapd.containsValue((long)i);
		if( !bits ) {
			System.out.println("KV BATTERY1AR8 derived unexpected cant find contains key "+i);
			throw new Exception("KV BATTERY1AR8 derived unexpected number cant find contains of value "+i);
		}
		 System.out.println("KV BATTERY1AR8 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * 
	 * Testing of first(), and firstKey()
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR9(String[] argv) throws Exception {
		int i = min;
		long tims = System.currentTimeMillis();
		Object k = bmap.firstKey(); // first key
		System.out.println("KV Battery1AR9");
		if( Integer.parseInt(((Based)k).basedKey) != i ) {
			System.out.println("KV BATTERY1A9 cant find contains key "+i);
			throw new Exception("KV BATTERY1AR9 unexpected cant find contains of key "+i);
		}
		long ks = (long) bmap.first();
		if( ks != i) {
			System.out.println("KV BATTERY1A9 cant find contains key "+i);
			throw new Exception("KV BATTERY1AR9 unexpected cant find contains of key "+i);
		}
		k = bmapd.firstKey(); // first key
		System.out.println("KV Battery1AR9 derived");
		if( Integer.parseInt(((Based)k).basedKey) != i ) {
			System.out.println("KV BATTERY1A9 derived cant find contains key "+i);
			throw new Exception("KV BATTERY1AR9 derived unexpected cant find contains of key "+i);
		}
		ks = (long) bmapd.first();
		if( ks != i) {
			System.out.println("KV BATTERY1A9 derived cant find contains key "+i);
			throw new Exception("KV BATTERY1AR9 derived unexpected cant find contains of key "+i);
		}
		System.out.println("KV BATTERY1AR9 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}

	/**
	 * test last and lastKey
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR10(String[] argv) throws Exception {
		int i = max-1;
		long tims = System.currentTimeMillis();
		Object k = bmap.lastKey(); // key
		System.out.println("KV Battery1AR10");
		if( Long.parseLong(((Based) k).basedKey) != (long)i ) {
			System.out.println("KV BATTERY1AR10 cant find last key "+i);
			throw new Exception("KV BATTERY1AR10 unexpected cant find last of key "+i);
		}
		long ks = (long)bmap.last();
		if( ks != i) {
			System.out.println("KV BATTERY1AR10 cant find last key "+i);
			throw new Exception("KV BATTERY1AR10 unexpected cant find last of key "+i);
		}
		k = bmapd.lastKey(); // key
		System.out.println("KV Battery1AR10 derived");
		if( Long.parseLong(((Based)k).basedKey) != (long)i ) {
			System.out.println("KV BATTERY1AR10 derived cant find last key "+i);
			throw new Exception("KV BATTERY1AR10 derived unexpected cant find last of key "+i);
		}
		ks = (long)bmapd.last();
		if( ks != i) {
			System.out.println("KV BATTERY1AR10 derived cant find last key "+i);
			throw new Exception("KV BATTERY1AR10 derived unexpected cant find last of key "+i);
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
		bits = bmapd.size();
		System.out.println("KV Battery1AR101 derived");
		if( bits != i ) {
			System.out.println("KV BATTERY1AR101 size mismatch derived "+bits+" should be:"+i);
			throw new Exception("KV BATTERY1AR101 size mismatch derived "+bits+" should be "+i);
		}
		System.out.println("BATTERY1AR101 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	/**
	 * tailmap returning keys
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR11(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		int i = min;
		Based fkey = new Based(String.format(uniqKeyFmt, i));
		Iterator<?> its = bmap.tailMap(fkey);
		System.out.println("KV Battery1AR11");
		while(its.hasNext()) {
			Based nex = (Based) its.next();
			// Map.Entry
			if(Integer.parseInt(nex.basedKey) != i) {
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE KEY MISMATCH:"+i+" - "+nex);
			}
			++i;
		}
		i = max/2;
		fkey = new Based(String.format(uniqKeyFmt, i));
		its = bmapd.tailMap(fkey);
		System.out.println("KV Battery1AR11 derived");
		while(its.hasNext()) {
			Based nex = (Based) its.next();
			// Map.Entry
			if(Integer.parseInt(nex.basedKey) != i) {
				System.out.println("KV RANGE KEY MISMATCH derived:"+i+" - "+nex);
				throw new Exception("KV RANGE KEY MISMATCH derived:"+i+" - "+nex);
			}
			++i;
		}
		 System.out.println("BATTERY1AR11 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
		 
	}
	/**
	 * tailmapKV
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR12(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		int i = min;
		Based fkey = new Based(String.format(uniqKeyFmt, i));
		Iterator<?> its = bmap.tailMapKV(fkey);
		System.out.println("KV Battery1AR12");
		while(its.hasNext()) {
			Comparable nex = (Comparable) its.next();
			Map.Entry<Based, Long> nexe = (Map.Entry<Based,Long>)nex;
			if(Integer.parseInt(nexe.getKey().basedKey) != i) {
			// Map.Entry
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE KEY MISMATCH:"+i+" - "+nex);
			}
			++i;
		}
		i = max/2;
		fkey = new Based(String.format(uniqKeyFmt, i));
		its = bmapd.tailMapKV(fkey);
		System.out.println("KV Battery1AR12 derived");
		while(its.hasNext()) {
			Comparable nex = (Comparable) its.next();
			Map.Entry<Based, Long> nexe = (Map.Entry<Based,Long>)nex;
			if(Integer.parseInt(nexe.getKey().basedKey) != i) {
			// Map.Entry
				System.out.println("KV RANGE KEY MISMATCH derived:"+i+" - "+nex);
				throw new Exception("KV RANGE KEY MISMATCH derived:"+i+" - "+nex);
			}
			++i;
		}
		 System.out.println("BATTERY1AR12 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * headMap - Returns a view of the portion of this map whose keys are strictly less than toKey.
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR13(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		int i = max;
		Based fkey = new Based(String.format(uniqKeyFmt, i));
		Iterator<?> its = bmap.headMap(fkey);
		System.out.println("KV Battery1AR13");
		// with i at max, should catch them all
		i = min;
		while(its.hasNext()) {
			Based nex = (Based) its.next();
			if(Integer.parseInt(nex.basedKey) != i) {
			// Map.Entry
				System.out.println("KV RANGE 1AR13 KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE 1AR13 KEY MISMATCH:"+i+" - "+nex);
			}
			++i;
		}
		i = max/2;
		fkey = new Based(String.format(uniqKeyFmt, i));
		its = bmapd.headMap(fkey);
		System.out.println("KV Battery1AR13 derived");
		i = min;
		while(its.hasNext()) {
			Based nex = (Based) its.next();
			if(Integer.parseInt(nex.basedKey) != i) {
			// Map.Entry
				System.out.println("KV RANGE 1AR13 KEY MISMATCH derived:"+i+" - "+nex);
				throw new Exception("KV RANGE 1AR13 KEY MISMATCH derived:"+i+" - "+nex);
			}
			++i;
		}
		if(i > max/2)
			throw new Exception("KV RANGE 1AR13 KEY MISMATCH derived index:"+i+" exceeded:"+(max/2));
		 System.out.println("BATTERY1AR13 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * headMapKV
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR14(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		int i = max;
		Based fkey = new Based(String.format(uniqKeyFmt, i));
		Iterator<?> its = bmap.headMapKV(fkey);
		System.out.println("KV Battery1AR14");
		i = min;
		while(its.hasNext()) {
			Comparable nex = (Comparable) its.next();
			Map.Entry<Based, Long> nexe = (Map.Entry<Based,Long>)nex;
			if(Integer.parseInt(nexe.getKey().basedKey) != i) {
			// Map.Entry
				System.out.println("KV RANGE KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE KEY MISMATCH:"+i+" - "+nex);
			}
			++i;
		}
		i = max/2;
		fkey = new Based(String.format(uniqKeyFmt, i));
		its = bmapd.headMapKV(fkey);
		System.out.println("KV Battery1AR14 derived");
		i = min;
		while(its.hasNext()) {
			Comparable nex = (Comparable) its.next();
			Map.Entry<Based, Long> nexe = (Map.Entry<Based,Long>)nex;
			if(Integer.parseInt(nexe.getKey().basedKey) != i) {
			// Map.Entry
				System.out.println("KV RANGE KEY MISMATCH derived:"+i+" - "+nex);
				throw new Exception("KV RANGE KEY MISMATCH derived:"+i+" - "+nex);
			}
			++i;
		}
		if(i > max/2)
			throw new Exception("KV RANGE 1AR14 KEY MISMATCH derived index:"+i+" exceeded:"+(max/2));
		 System.out.println("BATTERY1AR14 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * subMap - Returns a view of the portion of this map whose keys range from fromKey, inclusive, to toKey, exclusive.
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR15(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		int i = min;
		int j = max;
		Based fkey = new Based(String.format(uniqKeyFmt, i));
		// with j at max, should get them all since we stored to max -1
		Based tkey = new Based(String.format(uniqKeyFmt, j));
		Iterator<?> its = bmap.subMap(fkey, tkey);
		System.out.println("KV Battery1AR15");
		// with i at max, should catch them all
		while(its.hasNext()) {
			Based nex = (Based) its.next();
			if(Integer.parseInt(nex.basedKey) != i) {
			// Map.Entry
				System.out.println("KV RANGE 1AR15 KEY MISMATCH:"+i+" - "+nex);
				throw new Exception("KV RANGE 1AR15 KEY MISMATCH:"+i+" - "+nex);
			}
			++i;
		}
		//leave the types as base class, let the cast fall
		i = min;
		its = bmapd.subMap(fkey, tkey);
		System.out.println("KV Battery1AR15 derived");
		// with i at max, should catch them all
		while(its.hasNext()) {
			Based nex = (Based) its.next();
			if(Integer.parseInt(nex.basedKey) != i) {
			// Map.Entry
				System.out.println("KV RANGE 1AR15 KEY MISMATCH derived:"+i+" - "+nex);
				throw new Exception("KV RANGE 1AR15 KEY MISMATCH derived:"+i+" - "+nex);
			}
			++i;
		}
		 System.out.println("BATTERY1AR15 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms.");
	}
	
	/**
	 * subMapKV - Returns a view of the portion of this map whose keys range from fromKey, inclusive, to toKey, exclusive.
	 * @param argv
	 * @throws Exception
	 */
	public static void battery1AR16(String[] argv) throws Exception {
		long tims = System.currentTimeMillis();
		int i = min;
		int j = max;
		Based fkey = new Based(String.format(uniqKeyFmt, i));
		// with j at max, should get them all since we stored to max -1
		Based tkey = new Based(String.format(uniqKeyFmt, j));
		Iterator<?> its = bmap.subMapKV(fkey, tkey);
		System.out.println("KV Battery1AR16");
		// with i at max, should catch them all
		while(its.hasNext()) {
			Comparable nex = (Comparable) its.next();
			Map.Entry<Based, Long> nexe = (Map.Entry<Based,Long>)nex;
			if(Integer.parseInt(nexe.getKey().basedKey) != i) {
			// Map.Entry
				System.out.println("KV RANGE 1AR16 KEY MISMATCH:"+i+" - "+nexe);
				throw new Exception("KV RANGE 1AR16 KEY MISMATCH:"+i+" - "+nexe);
			}
			++i;
		}
		// leave the base class, let the cast
		i = min;
		its = bmapd.subMapKV(fkey, tkey);
		System.out.println("KV Battery1AR16 derived");
		// with i at max, should catch them all
		while(its.hasNext()) {
			Comparable nex = (Comparable) its.next();
			Map.Entry<Based, Long> nexe = (Map.Entry<Based,Long>)nex;
			if(Integer.parseInt(nexe.getKey().basedKey) != i) {
			// Map.Entry
				System.out.println("KV RANGE 1AR16 derived KEY MISMATCH derived:"+i+" - "+nexe);
				throw new Exception("KV RANGE 1AR16 derived KEY MISMATCH derived:"+i+" - "+nexe);
			}
			++i;
		}
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
		long j = (int) bmap.size();
		if(j > 0) {
			System.out.println("Cleaning DB of "+j+" elements.");	
			// with i at max, should catch them all
			for(int i = 0; i < j; i++) {
				Based fkey = new Based(String.format(uniqKeyFmt, i));
				bmap.remove(fkey);
				bmapd.remove(fkey);
				// Map.Entry
				if(bmap.contains(fkey)) { 
					System.out.println("KV RANGE 1AR17 KEY MISMATCH:"+i);
					throw new Exception("KV RANGE 1AR17 KEY MISMATCH:"+i);
				}
				if(bmapd.contains(fkey)) { 
					System.out.println("KV RANGE 1AR17 KEY MISMATCH:"+i);
					throw new Exception("KV RANGE 1AR17 KEY MISMATCH:"+i);
				}
			}
		}
		long siz = bmap.size();
		long sizd = bmapd.size();
		if(siz > 0) {
			Iterator<?> its = bmap.entrySet();
			while(its.hasNext()) {
				Comparable nex = (Comparable) its.next();
				//System.out.println(i+"="+nex);
				System.out.println("KV RANGE 1AR17 KEY SHOULD BE DELETED:"+nex);
				System.out.println("KV RANGE 1AR17 KEY MISMATCH:"+siz+" > 0 after all deleted and committed");
				throw new Exception("KV RANGE 1AR17 KEY MISMATCH:"+siz+" > 0 after delete/commit");
			}
		}
		if(sizd > 0) {
			Iterator<?> its = bmapd.entrySet();
			while(its.hasNext()) {
				Comparable nex = (Comparable) its.next();
				//System.out.println(i+"="+nex);
				System.out.println("KV RANGE 1AR17 derived KEY SHOULD BE DELETED:"+nex);
			}
			System.out.println("KV RANGE 1AR17 derived KEY MISMATCH:"+sizd+" > 0 after all deleted and committed");
			throw new Exception("KV RANGE 1AR17 derived KEY MISMATCH:"+sizd+" > 0 after delete/commit");
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
		int max1 = (max/2);
		long tims = System.currentTimeMillis();
		int dupes = 0;
		int recs = 0;
		Based fkey = null;
		for(int i = min; i < max1; i++) {
			fkey = new Based(String.format(uniqKeyFmt, i));
			bmap.put(fkey, new Long(i));
			++recs;
		}
		long s = bmap.size();
		if(s != max1)
			System.out.println("Size at halway point of restore incorrect:"+s+" should be "+max1);
		for(int i = max1; i < max; i++) {
			fkey = new Derived(String.format(uniqKeyFmt, i));
			bmapd.put(fkey, new Long(i));
			++recs;
		}
		System.out.println("KV BATTERY18 SUCCESS in "+(System.currentTimeMillis()-tims)+" ms. Stored "+recs+" records, rejected "+dupes+" dupes.");
	}
	
	public static class Based implements Serializable,Comparable{
		public String basedKey;
		public Based() {}
		public Based(String key) {
			basedKey = key;
		}
		@Override
		public int compareTo(Object o) {
			return basedKey.compareTo(((Based)o).basedKey);
		}
		@Override
		public String toString() {
			return basedKey;
		}
	}
	/*
	 * Store derived class in base class tablespace, since we declare no 'tablespace' attribute, it will select superclass tablespace
	 * Store derived class in base class column, since we declare 'column' attribe to be base tablespace, otherwise it would store in column named for derived class
	@DatabaseClass(column="com.neocoretechs.rocksack.test.BatteryKVDerived$Based")
	public static class Derived extends Based implements Serializable,Comparable {
		public Derived() {}
		public Derived(String key) {
			super(key);
		}	
	}
	*/
	@DatabaseClass(column="com.neocoretechs.rocksack.test.BatteryKVDerived$Based")
	public static class Derived extends Based implements Serializable,Comparable {
		public Derived() {}
		public Derived(String key) {
			super(key);
		}	
	}
	
}
