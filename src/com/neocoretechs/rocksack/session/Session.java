package com.neocoretechs.rocksack.session;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;

import com.neocoretechs.rocksack.KeyValue;
import com.neocoretechs.rocksack.SerializedComparator;
import com.neocoretechs.rocksack.iterator.EntrySetIterator;
import com.neocoretechs.rocksack.iterator.HeadSetIterator;
import com.neocoretechs.rocksack.iterator.HeadSetKVIterator;
import com.neocoretechs.rocksack.iterator.KeySetIterator;
import com.neocoretechs.rocksack.iterator.SubSetIterator;
import com.neocoretechs.rocksack.iterator.SubSetKVIterator;
import com.neocoretechs.rocksack.iterator.TailSetIterator;
import com.neocoretechs.rocksack.iterator.TailSetKVIterator;
import com.neocoretechs.rocksack.stream.EntrySetStream;
import com.neocoretechs.rocksack.stream.HeadSetKVStream;
import com.neocoretechs.rocksack.stream.HeadSetStream;
import com.neocoretechs.rocksack.stream.KeySetStream;
import com.neocoretechs.rocksack.stream.SubSetKVStream;
import com.neocoretechs.rocksack.stream.SubSetStream;
import com.neocoretechs.rocksack.stream.TailSetKVStream;
import com.neocoretechs.rocksack.stream.TailSetStream;
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
* Session object. Returned by SessionManager.Connect().
* Responsible for providing access to Deep Store key/value interface implementations
* Operations include
* handing out iterators, inserting and deleting objects, size, navigation, clearing,
* and handling commit and rollback.<p/>
* Session is the universal collection of transaction and non-transaction methods, subclasses encapsulate
* specific transaction and non-transaction semantics. 
* @author Jonathan Groff (C) NeoCoreTechs 2003, 2017, 2021, 2022
*/
public class Session {
	private boolean DEBUG = false;
	protected RocksDB kvStore;
	private Options options;
	private boolean dbOpen = false;

	/**
	* Create a new session
	* @param kvMain The {@link KeyValueMainInterface} Main object than handles the key pages indexing the objects in the deep store.
	* @param tuid The user
	* @param tgis The group
	* @exception IOException If global IO problem
	*/
	protected Session(RocksDB kvStore, Options options)  {
		this.kvStore = kvStore;
		this.options = options;
		if( DEBUG )
			System.out.println("RockSackSession constructed with db:"+getDBname());
	}
	
	protected String getDBname() {
		return kvStore.getName();
	}
	
	@Override
	public String toString() {
		return this.getClass().getName()+" using DB:"+getDBname();
	}
	
	protected Object getMutexObject() {
		return kvStore;
	}
	/**
	 * Wait for rocksdb.stats to report uptime > 0 to ensure DB is open.
	 * Not sure how necessary this is, and hope to find a better method if it in fact is.
	 * @throws IOException
	 */
	protected void waitOpen() throws IOException {
		if(dbOpen)
			return;
		while(true) {
			String str;
			try {
				str = kvStore.getProperty("rocksdb.stats");
			} catch (RocksDBException e1) {
				throw new IOException(e1);
			}
			int s = str.indexOf("Uptime");
			int t = str.indexOf("total",s);
			String tline = str.substring(s, t);
			String[] tp = tline.split(" ");
			float usecs = Float.parseFloat(tp[1]);
			if(usecs > 0.0)
				break;
			if(DEBUG)
				System.out.println("wait for uptime..."+usecs);
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {}
		}
		dbOpen = true;
	}
	/**
	 * Call the put method of KeyValueMain.
	 * @param key The key value to attempt add
	 * @param o The value for the key to add
	 * @return true
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected boolean put(Comparable key, Object o) throws IOException {
		try {
			kvStore.put(SerializedComparator.serializeObject(key),SerializedComparator.serializeObject(o));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
		return true;
	}
	/**
	 * Call the put method of KeyValueMain.
	 * @param txn The transaction context
	 * @param key The key value to attempt add
	 * @param o The value for the key to add
	 * @return true
	 * @throws IOException
	 */
	protected boolean put(Transaction txn, Comparable key, Object o) throws IOException {
		try {
			txn.put(SerializedComparator.serializeObject(key),SerializedComparator.serializeObject(o));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
		return true;
	}
	/**
	 * Call the put method of KeyValueMain.
	 * @param key The key value to attempt add, raw bytes which will not be serialized beforehand
	 * @param o The value for the key to add
	 * @return true
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected boolean putViaBytes(byte[] key, Object o) throws IOException {
		try {
			kvStore.put(key,SerializedComparator.serializeObject(o));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
		return true;
	}
	/**
	 * Call the put method of KeyValueMain.
	 * @param txn The transaction context
	 * @param key The key value to attempt add, raw bytes unserialized beforehand
	 * @param o The value for the key to add
	 * @return true
	 * @throws IOException
	 */
	protected boolean putViaBytes(Transaction txn, byte[] key, Object o) throws IOException {
		try {
			txn.put(key,SerializedComparator.serializeObject(o));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
		return true;
	}
	/**
	 * Cause the KvStore to seekKey for the Comparable type.
	 * @param o the Comparable object to seek.
	 * @return the Key/Value object from the retrieved node
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Object get(Comparable o) throws IOException {
		if(DEBUG)
			System.out.printf("%s.get(%s)%n", this.getClass().getName(), o);
		   try {
			   byte[] b = kvStore.get(SerializedComparator.serializeObject(o));
			   if(b == null)
				   return null;
			   return new KeyValue(o,SerializedComparator.deserializeObject(b));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
	}
	/**
	 * Cause the KvStore to seekKey for the Comparable type.
	 * @param txn Transaction context
	 * @param ro The ReadOptions
	 * @param o the Comparable object to seek.
	 * @return the Key/Value object from the retrieved node
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Object get(Transaction txn, ReadOptions ro, Comparable o) throws IOException {
		if(DEBUG)
			System.out.printf("%s.get(%s, %s, %s)%n", this.getClass().getName(), txn, ro, o);
		   try {
			   byte[] b = txn.get(ro,SerializedComparator.serializeObject(o));
			   if(b == null)
				   return null;
			   return new KeyValue(o,SerializedComparator.deserializeObject(b));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
	}
	/**
	 * Cause the KvStore to seekKey for the raw byte array.
	 * @param o the byte array to seek.
	 * @return the Value object from the retrieved node
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Object getViaBytes(byte[] o) throws IOException {
		if(DEBUG)
			System.out.printf("%s.get(%s)%n", this.getClass().getName(), o);
		   try {
			   byte[] b = kvStore.get(o);
			   if(b == null)
				   return null;
			   return SerializedComparator.deserializeObject(b);
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
	}
	/**
	 * Cause the KvStore to seekKey for the Comparable type.
	 * @param txn Transaction context
	 * @param ro The ReadOptions
	 * @param o the byte array to seek.
	 * @return the Value object from the retrieved node
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Object getViaBytes(Transaction txn, ReadOptions ro, byte[] o) throws IOException {
		if(DEBUG)
			System.out.printf("%s.get(%s, %s, %s)%n", this.getClass().getName(), txn, ro, o);
		   try {
			   byte[] b = txn.get(ro,o);
			   if(b == null)
				   return null;
			   return SerializedComparator.deserializeObject(b);
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
	}
	/**
	 * Retrieve an object with this value for first key found to have it.
	 * @param o the object value to seek
	 * @return element for the key, null if not found
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Object getValue(Object o) throws IOException {
		   try {
			return SerializedComparator.deserializeObject(kvStore.get(SerializedComparator.serializeObject(o)));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
	}
	
	/**
	 * Retrieve an object with this value for first key found to have it.
	 * @param txn Transaction context
	 * @param ro The ReadOptions
	 * @param o the object value to seek
	 * @return element for the key, null if not found
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Object getValue(Transaction txn, ReadOptions ro, Object o) throws IOException {
		   try {
			return SerializedComparator.deserializeObject(txn.get(ro, SerializedComparator.serializeObject(o)));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
	}
	/**
	* Returns iterator vs actual subset.
	* 'from' element inclusive, 'to' element exclusive
	* @param fkey Return from fkey
	* @param tkey Return from fkey to strictly less than tkey
	* @return The Iterator over the subSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> subSet(Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetIterator(fkey, tkey, kvStore);
	}
	/**
	* Returns iterator vs actual subset.
	* 'from' element inclusive, 'to' element exclusive
	* @param txn Transaction
	* @param fkey Return from fkey
	* @param tkey Return from fkey to strictly less than tkey
	* @return The Iterator over the subSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> subSet(Transaction txn, Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetIterator(fkey, tkey, txn);
	}
	/**
	 * Return a Stream that delivers the subset of fkey to tkey
	 * @param fkey the from key
	 * @param tkey the to key
	 * @return the stream from which the lambda expression can be utilized
	 * @throws IOException
	 */
	protected Stream<?> subSetStream(Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetStream(new SubSetIterator(fkey, tkey, kvStore));
	}
	/**
	 * Return a Stream that delivers the subset of fkey to tkey
	 * @param txn Transaction
	 * @param fkey the from key
	 * @param tkey the to key
	 * @return the stream from which the lambda expression can be utilized
	 * @throws IOException
	 */
	protected Stream<?> subSetStream(Transaction txn, Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetStream(new SubSetIterator(fkey, tkey, txn));
	}
	/**
	* Not a real subset, returns iterator vs set.
	* 'from' element inclusive, 'to' element exclusive
	* @param fkey Return from fkey
	* @param tkey Return from fkey to strictly less than tkey
	* @return The KeyValuePair Iterator over the subSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> subSetKV(Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetKVIterator(fkey, tkey, kvStore);
	}
	/**
	* Not a real subset, returns iterator vs set.
	* 'from' element inclusive, 'to' element exclusive
	* @param txn Transaction
	* @param fkey Return from fkey
	* @param tkey Return from fkey to strictly less than tkey
	* @return The KeyValuePair Iterator over the subSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> subSetKV(Transaction txn, Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetKVIterator(fkey, tkey, txn);
	}
	/**
	 * Return a Streamof key/value pairs that delivers the subset of fkey to tkey
	 * @param fkey the from key
	 * @param tkey the to key
	 * @return the stream from which the lambda expression can be utilized
	 * @throws IOException
	 */
	protected Stream<?> subSetKVStream(Comparable fkey, Comparable tkey) throws IOException {
			return new SubSetKVStream(fkey, tkey, kvStore);
	}
	/**
	 * Return a Streamof key/value pairs that delivers the subset of fkey to tkey
	 * @param txn Transaction
	 * @param fkey the from key
	 * @param tkey the to key
	 * @return the stream from which the lambda expression can be utilized
	 * @throws IOException
	 */
	protected Stream<?> subSetKVStream(Transaction txn, Comparable fkey, Comparable tkey) throws IOException {
			return new SubSetKVStream(fkey, tkey, txn);
	}
	/**
	* Not a real subset, returns iterator
	* @return The Iterator over the entrySet
	* @exception IOException If we cannot obtain the iterator
	*/
	protected Iterator<?> entrySet() throws IOException {
		return new EntrySetIterator(kvStore);
	}
	/**
	* Not a real subset, returns iterator
	* @param txn Transaction
	* @return The Iterator over the entrySet
	* @exception IOException If we cannot obtain the iterator
	*/
	protected Iterator<?> entrySet(Transaction txn) throws IOException {
		return new EntrySetIterator(txn);
	}
	/**
	 * Get a stream of entry set
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> entrySetStream() throws IOException {
		return new EntrySetStream(kvStore);
	}
	/**
	 * Get a stream of entry set
	 * @param txn Transaction
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> entrySetStream(Transaction txn) throws IOException {
		return new EntrySetStream(txn);
	}
	/**
	* Not a real subset, returns Iterator
	* @param tkey return from head to strictly less than tkey
	* @return The Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> headSet(Comparable tkey) throws IOException {
		return new HeadSetIterator(tkey, kvStore);
	}
	/**
	* Not a real subset, returns Iterator
	* @param Transaction txn
	* @param tkey return from head to strictly less than tkey
	* @return The Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> headSet(Transaction txn, Comparable tkey) throws IOException {
		return new HeadSetIterator(tkey, txn);
	}
	/**
	 * Get a stream of headset
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> headSetStream(Comparable tkey) throws IOException {
		return new HeadSetStream(tkey, kvStore);
	}
	/**
	 * Get a stream of headset
	 * @param txn Transaction
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> headSetStream(Transaction txn, Comparable tkey) throws IOException {
		return new HeadSetStream(tkey, txn);
	}
	/**
	* Not a real subset, returns Iterator
	* @param tkey return from head to strictly less than tkey
	* @return The KeyValuePair Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> headSetKV(Comparable tkey) throws IOException {
		return new HeadSetKVIterator(tkey, kvStore);
	}
	/**
	* Not a real subset, returns Iterator
	* @param txn Transaction
	* @param tkey return from head to strictly less than tkey
	* @return The KeyValuePair Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> headSetKV(Transaction txn, Comparable tkey) throws IOException {
		return new HeadSetKVIterator(tkey, txn);
	}
	/**
	 * Get a stream of head set
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> headSetKVStream(Comparable tkey) throws IOException {
		return new HeadSetKVStream(tkey, kvStore);
	}
	/**
	 * Get a stream of head set
	 * @param txn Transaction
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> headSetKVStream(Transaction txn, Comparable tkey) throws IOException {
		return new HeadSetKVStream(tkey, txn);
	}
	/**
	* Return the keyset Iterator over all elements
	* @return The Iterator over the keySet
	* @exception IOException If we cannot obtain the iterator
	*/
	protected Iterator<?> keySet() throws IOException {
		return new KeySetIterator(kvStore);
	}
	/**
	* Return the keyset Iterator over all elements
	* @param txn Transaction
	* @return The Iterator over the keySet
	* @exception IOException If we cannot obtain the iterator
	*/
	protected Iterator<?> keySet(Transaction txn) throws IOException {
		return new KeySetIterator(txn);
	}
	/**
	 * Get a keyset stream
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> keySetStream() throws IOException {
		return new KeySetStream(kvStore);
	}
	/**
	 * Get a keyset stream
	 * @param txn Transaction
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> keySetStream(Transaction txn) throws IOException {
		return new KeySetStream(txn);
	}
	/**
	* Not a real subset, returns Iterator
	* @param fkey return from value to end
	* @return The Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> tailSet(Comparable fkey) throws IOException {
		return new TailSetIterator(fkey, kvStore);
	}
	/**
	* Not a real subset, returns Iterator
	* @param txn Transaction
	* @param fkey return from value to end
	* @return The Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> tailSet(Transaction txn, Comparable fkey) throws IOException {
		return new TailSetIterator(fkey, txn);
	}
	/**
	 * Return a tail set stream
	 * @param fkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> tailSetStream(Comparable fkey) throws IOException {
		return new TailSetStream(fkey, kvStore);
	}
	/**
	 * Return a tail set stream
	 * @param txn Transaction
	 * @param fkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> tailSetStream(Transaction txn, Comparable fkey) throws IOException {
		return new TailSetStream(fkey, txn);
	}
	/**
	* Not a real subset, returns Iterator
	* @param fkey return from value to end
	* @return The KeyValuePair Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> tailSetKV(Comparable fkey) throws IOException {
		return new TailSetKVIterator(fkey, kvStore);
	}
	/**
	* Not a real subset, returns Iterator
	* @param txn Transaction
	* @param fkey return from value to end
	* @return The KeyValuePair Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> tailSetKV(Transaction txn, Comparable fkey) throws IOException {
		return new TailSetKVIterator(fkey, txn);
	}
	/**
	 * Return a tail set key/value stream
	 * @param fkey from key of tailset
	 * @return the stream from which the lambda can be utilized
	 * @throws IOException
	 */
	protected Stream<?> tailSetKVStream(Comparable fkey) throws IOException {
		return new TailSetKVStream(fkey, kvStore);
	}
	/**
	 * Return a tail set key/value stream
	 * @param txn Transaction
	 * @param fkey from key of tailset
	 * @return the stream from which the lambda can be utilized
	 * @throws IOException
	 */
	protected Stream<?> tailSetKVStream(Transaction txn, Comparable fkey) throws IOException {
		return new TailSetKVStream(fkey, txn);
	}
	/**
	 * Contains a value object
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected boolean containsValue(Object o) throws IOException {
		Iterator it = new KeySetIterator(kvStore);
		while(it.hasNext()) {
			try {
				Object o2 = SerializedComparator.deserializeObject(kvStore.get(SerializedComparator.serializeObject(it.next())));
				if(o.equals(o2))
					return true;
			} catch (RocksDBException | IOException e) {
				throw new IOException(e);
			}
		}
		return false;
	}
	/**
	 * Contains a value object
	 * @param txn Transaction
	 * @param ro ReadOptions
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected boolean containsValue(Transaction txn, ReadOptions ro, Object o) throws IOException {
		Iterator it = new KeySetIterator(txn);
		while(it.hasNext()) {
			try {
				Object o2 = SerializedComparator.deserializeObject(txn.get(ro, SerializedComparator.serializeObject(it.next())));
				if(o.equals(o2))
					return true;
			} catch (RocksDBException | IOException e) {
				throw new IOException(e);
			}
		}
		return false;
	}
	/**
	 * Contains a value object
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected boolean contains(Comparable o) throws IOException {
		Object o2 = get(o);
		return o2 != null;
	}
	/**
	 * Contains a value object. Does a get since RocksDB doesnt seem to have keymayexist in trans context
	 * @param txn Transaction
	 * @param ro readOptions
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected boolean contains(Transaction txn, ReadOptions ro, Comparable o) throws IOException {
		Object o2 = get(txn,ro,o);
		return o2 != null;
	}
	/**
	* Remove the key and value of the parameter.
	* @return null or previous object
	*/
	@SuppressWarnings("rawtypes")
	protected Object remove(Comparable o) throws IOException {
		try {
			byte[] b2 = SerializedComparator.serializeObject(o); // key
			byte[] b = kvStore.get(b2); // b = value
			kvStore.delete(b2); // serial bytes of key, call to delete
			if(b != null) {
				return SerializedComparator.deserializeObject(b); // serialize previous value from retrieved bytes
			}
		} catch (RocksDBException | IOException e) {
			return new IOException(e);
		}
		return null; // no previous key
	}
	/**
	* Remove the key and value of the parameter.
	* @return null or previous object
	*/
	@SuppressWarnings("rawtypes")
	protected Object remove(Transaction txn, ReadOptions ro, Comparable o) throws IOException {
		try {
			byte[] b2 = SerializedComparator.serializeObject(o); // key
			byte[] b = txn.get(ro, b2); // b = value
			txn.delete(b2); // serial bytes of key, call to delete
			if(b != null) {
				return SerializedComparator.deserializeObject(b); // serialize previous value from retrieved bytes
			}
		} catch (RocksDBException | IOException e) {
			return new IOException(e);
		}
		return null; // no previous key
	}
	/**
	 * Get the value of the object associated with first key
	 * @return Object from first key
	 * @throws IOException
	 */
	protected Object first() throws IOException {
		Iterator it = new EntrySetIterator(kvStore);
		if(it.hasNext()) {
			return ((Map.Entry)it.next()).getValue();
		}
		return null;
	}
	/**
	 * Get the value of the object associated with first key
	 * @param txn Transaction
	 * @return Object from first key
	 * @throws IOException
	 */
	protected Object first(Transaction txn) throws IOException {
		Iterator it = new EntrySetIterator(txn);
		if(it.hasNext()) {
			return ((Map.Entry)it.next()).getValue();
		}
		return null;
	}
	/**
	 * Get the first key
	 * @return The Comparable first key in the KVStore
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Comparable firstKey() throws IOException {
		if(DEBUG)
			System.out.printf("%s.firstKey for kvStore %s%n", this.getClass().getName(),kvStore);
		Iterator it = new EntrySetIterator(kvStore);
		if(it.hasNext()) {
			return (Comparable) ((Map.Entry)it.next()).getKey();
		}
		return null;
	}
	/**
	 * Get the first key
	 * @param txn Transaction
	 * @return The Comparable first key in the KVStore
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Comparable firstKey(Transaction txn) throws IOException {
		if(DEBUG)
			System.out.printf("%s.firstKey for kvStore %s%n", this.getClass().getName(),txn);
		Iterator it = new EntrySetIterator(txn);
		if(it.hasNext()) {
			return (Comparable) ((Map.Entry)it.next()).getKey();
		}
		return null;
	}
	/**
	 * Get the last object associated with greatest valued key in the KVStore
	 * @return The value of the Object of the greatest key
	 * @throws IOException
	 */
	protected Object last() throws IOException {
		if(DEBUG)
			System.out.printf("%s.last for kvStore %s%n", this.getClass().getName(),kvStore);
		EntrySetIterator it = new EntrySetIterator(kvStore);
		RocksIterator ri = it.getIterator();
		ri.seekToLast();
		if(ri.isValid()) {
			return SerializedComparator.deserializeObject(ri.value());
		}
		return null;
	}
	/**
	 * Get the last object associated with greatest valued key in the KVStore
	 * @param txn Transaction
	 * @return The value of the Object of the greatest key
	 * @throws IOException
	 */
	protected Object last(Transaction txn) throws IOException {
		if(DEBUG)
			System.out.printf("%s.last for kvStore %s%n", this.getClass().getName(),txn);
		EntrySetIterator it = new EntrySetIterator(txn);
		RocksIterator ri = it.getIterator();
		ri.seekToLast();
		if(ri.isValid()) {
			return SerializedComparator.deserializeObject(ri.value());
		}
		return null;
	}
	/**
	 * Get the last key in the KVStore
	 * @return The last, greatest valued key in the KVStore.
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Comparable lastKey() throws IOException {
		if(DEBUG)
			System.out.printf("%s.lastKey for kvStore %s%n", this.getClass().getName(),kvStore);
		EntrySetIterator it = new EntrySetIterator(kvStore);
		RocksIterator ri = it.getIterator();
		ri.seekToLast();
		if(ri.isValid()) {
			return (Comparable) SerializedComparator.deserializeObject(ri.key());
		}
		return null;
	}
	/**
	 * Get the last key in the KVStore
	 * @param txn Transaction
	 * @return The last, greatest valued key in the KVStore.
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Comparable lastKey(Transaction txn) throws IOException {
		if(DEBUG)
			System.out.printf("%s.lastKey for kvStore %s%n", this.getClass().getName(),txn);
		EntrySetIterator it = new EntrySetIterator(txn);
		RocksIterator ri = it.getIterator();
		ri.seekToLast();
		if(ri.isValid()) {
			return (Comparable) SerializedComparator.deserializeObject(ri.key());
		}
		return null;
	}
	/**
	 * Get the number of keys total.
	 * @return The size of the KVStore.
	 * @throws IOException
	 */
	protected long size() throws IOException {
		Iterator it = new KeySetIterator(kvStore);
		long cnt = 0;
		while(it.hasNext()) {
			it.next();
			++cnt;
		}
		return cnt;
	}
	/**
	 * Get the number of keys total.
	 * @param txn Transaction
	 * @return The size of the KVStore.
	 * @throws IOException
	 */
	protected long size(Transaction txn) throws IOException {
		Iterator it = new KeySetIterator(txn);
		long cnt = 0;
		while(it.hasNext()) {
			it.next();
			++cnt;
		}
		return cnt;
	}
	/**
	 * Is the KVStore empty?
	 * @return true if it is empty.
	 * @throws IOException
	 */
	protected boolean isEmpty() throws IOException {
		Iterator it = new KeySetIterator(kvStore);
		if(it.hasNext()) {
			return true;
		}
		return false;
	}
	
	protected boolean isEmpty(Transaction txn) throws IOException {
		Iterator it = new KeySetIterator(txn);
		if(it.hasNext()) {
			return true;
		}
		return false;
	}
	/**
	* Close this session.
	* @param rollback true to roll back, false to commit
	* @exception IOException For low level failure
	*/
	public void Close() throws IOException {
		kvStore.close();
	}
	
	protected RocksDB getKVStore() { return kvStore; }
	
}
