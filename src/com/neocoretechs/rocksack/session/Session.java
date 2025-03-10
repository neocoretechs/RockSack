package com.neocoretechs.rocksack.session;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;

import com.neocoretechs.rocksack.KeyValue;
import com.neocoretechs.rocksack.SerializedComparator;
import com.neocoretechs.rocksack.iterator.Entry;
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
* The user will access these methods through the subclassed implementations such as {@link BufferedMap} and {@link TransactionalMap}.
* Responsible for providing access to Deep Store key/value interface implementations
* Operations include
* handing out iterators, inserting and deleting objects, size, navigation, clearing,
* and handling commit and rollback.<p>
* Session is the universal collection of transaction and non-transaction methods, subclasses encapsulate
* specific transaction and non-transaction semantics. 
* @author Jonathan Groff (C) NeoCoreTechs 2003, 2017, 2021, 2022
*/
public class Session {
	private boolean DEBUG = false;
	protected RocksDB kvStore;
	protected Options options;
	private boolean dbOpen = false;
	//
	List<ColumnFamilyDescriptor> columnFamilyDescriptor = null;
	List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
	/**
	 * 
	 * @param kvStore
	 * @param options
	 * @param columnFamilyDescriptor
	 * @param columnFamilyHandles
	 */
	public Session(RocksDB kvStore, Options options, ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptor, List<ColumnFamilyHandle> columnFamilyHandles) {
		this.kvStore = kvStore;
		this.options = options;
		this.columnFamilyDescriptor = columnFamilyDescriptor;
		this.columnFamilyHandles = columnFamilyHandles;
		if( DEBUG )
			System.out.println("RockSackSession constructed with db:"+getDBname()+" desc:"+Arrays.toString(columnFamilyDescriptor.toArray())+" handle:"+Arrays.toString(columnFamilyHandles.toArray()));
	}

	protected String getDBname() {
		return kvStore.getName();
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
	 * @param cfh ColumnFamilyHandle
	 * @param key The key value to attempt add
	 * @param o The value for the key to add
	 * @return true
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected boolean put(ColumnFamilyHandle cfh, Comparable key, Object o) throws IOException {
		try {
			kvStore.put(cfh,SerializedComparator.serializeObject(key),SerializedComparator.serializeObject(o));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
		return true;
	}
	/**
	 * Call the put method of KeyValueMain.
	 * @param txn The transaction context
	 * @param cfh ColumnFamilyHandle
	 * @param key The key value to attempt add
	 * @param o The value for the key to add
	 * @return true
	 * @throws IOException
	 */
	protected boolean put(Transaction txn, ColumnFamilyHandle cfh, Comparable key, Object o) throws IOException {
		try {
			txn.put(cfh,SerializedComparator.serializeObject(key),SerializedComparator.serializeObject(o));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
		return true;
	}

	/**
	 * Call the put method of KeyValueMain.
	 * @param cfh ColumnFamilyHandle
	 * @param key The key value to attempt add, raw bytes which will not be serialized beforehand
	 * @param o The value for the key to add
	 * @return true
	 * @throws IOException
	 */
	protected boolean putViaBytes(ColumnFamilyHandle cfh, byte[] key, Object o) throws IOException {
		try {
			kvStore.put(cfh,key,SerializedComparator.serializeObject(o));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
		return true;
	}
	/**
	 * Call the put method of KeyValueMain.
	 * @param txn The transaction context
	 * @param cfh ColumnFamilyHandle
	 * @param key The key value to attempt add, raw bytes unserialized beforehand
	 * @param o The value for the key to add
	 * @return true
	 * @throws IOException
	 */
	protected boolean putViaBytes(Transaction txn, ColumnFamilyHandle cfh, byte[] key, Object o) throws IOException {
		try {
			txn.put(cfh,key,SerializedComparator.serializeObject(o));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
		return true;
	}

	/**
	 * Cause the KvStore to seekKey for the Comparable type.
	 * @param cfh ColumnFamilyHandle
	 * @param o the Comparable object to seek.
	 * @return the Key/Value object from the retrieved node
	 * @throws IOException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object get(ColumnFamilyHandle cfh, Comparable o) throws IOException {
		if(DEBUG)
			System.out.printf("%s.get(%s)%n", this.getClass().getName(), o);
		   try {
			   byte[] b = kvStore.get(cfh,SerializedComparator.serializeObject(o));
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
	 * @param cfh ColumnFamilyHandle
	 * @param ro The ReadOptions
	 * @param o the Comparable object to seek.
	 * @return the Key/Value object from the retrieved node
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Object get(Transaction txn, ColumnFamilyHandle cfh, ReadOptions ro, Comparable o) throws IOException {
		if(DEBUG)
			System.out.printf("%s.get(%s, %s, %s)%n", this.getClass().getName(), txn, ro, o);
		   try {
			   byte[] b = txn.get(ro, cfh, SerializedComparator.serializeObject(o));
			   if(b == null)
				   return null;
			   return new KeyValue(o,SerializedComparator.deserializeObject(b));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
	}
	
	/**
	 * getForUpdate() to read a key and make the read value a precondition for transaction commit.
	 * Currently, GetForUpdate() is the only way to establish Read-Write conflicts, 
	 * so it can be used in combination with iterators, for example.
	 * @param txn Transaction context
	 * @param cfh ColumnFamilyHandle
	 * @param ro The ReadOptions
	 * @param o the Comparable object to seek.
	 * @param exclusive - true if the transaction should have exclusive access to the key, otherwise false for shared access.
	 * @return the Key/Value object from the retrieved node
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Object getForUpdate(Transaction txn, ColumnFamilyHandle cfh, ReadOptions ro, Comparable o, boolean exclusive) throws IOException {
		if(DEBUG)
			System.out.printf("%s.get(%s, %s, %s)%n", this.getClass().getName(), txn, ro, o);
		   try {
			   byte[] b = txn.getForUpdate(ro,cfh,SerializedComparator.serializeObject(o), exclusive);
			   if(b == null)
				   return null;
			   return new KeyValue(o,SerializedComparator.deserializeObject(b));
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
	}
	/**
	 * Tell the transaction that it no longer needs to do any conflict checking for this key.
	 * @param txn The transaction
	 * @param cfh ColumnFamilyhandle
	 * @param o key
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected void undoGetForUpdate(Transaction txn, ColumnFamilyHandle cfh, Comparable o) throws IOException {
		if(DEBUG)
			System.out.printf("%s.get(%s, %s, %s)%n", this.getClass().getName(), txn, cfh, o);
		txn.undoGetForUpdate(cfh,SerializedComparator.serializeObject(o));
	}
	/**
	 * Cause the KvStore to seekKey for the raw byte array.
	 * @param o the byte array to seek.
	 * @return the Value object from the retrieved node
	 * @throws IOException
	 */
	protected Object getViaBytes(ColumnFamilyHandle cfh, byte[] o) throws IOException {
		if(DEBUG)
			System.out.printf("%s.get(%s)%n", this.getClass().getName(), o);
		   try {
			   byte[] b = kvStore.get(cfh, o);
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
	 * @param cfh ColumnFamilyHandle
	 * @param ro The ReadOptions
	 * @param o the byte array to seek.
	 * @return the Value object from the retrieved node
	 * @throws IOException
	 */
	protected Object getViaBytes(Transaction txn, ColumnFamilyHandle cfh, ReadOptions ro, byte[] o) throws IOException {
		if(DEBUG)
			System.out.printf("%s.get(%s, %s, %s)%n", this.getClass().getName(), txn, ro, o);
		   try {
			   byte[] b = txn.get(ro, cfh, o);
			   if(b == null)
				   return null;
			   return SerializedComparator.deserializeObject(b);
		} catch (RocksDBException | IOException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Retrieve an object with this value for first key found to have it.
	 * @param cfh ColumnFamilyHandle
	 * @param o the object value to seek
	 * @return element for the key, null if not found of type {@link com.neocoretechs.rocksack.iterator.Entry}
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Object getValue(ColumnFamilyHandle cfh, Object o) throws IOException {
		Iterator it = new EntrySetIterator(kvStore, cfh);
		while(it.hasNext()) {
			Entry e = ((Entry)it.next());
			if(e.getValue().equals(o))
				return e;
		}
		return null;
	}
	
	/**
	 * Retrieve an object with this value for first key found to have it. {@link com.neocoretechs.rocksack.iterator.EntrySetIterator}
	 * @param txn Transaction context
	 * @param cfh ColumnFamilyHandle
	 * @param ro The ReadOptions
	 * @param o the object value to seek
	 * @return element for the key, null if not found of type {@link com.neocoretechs.rocksack.iterator.Entry}
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Object getValue(Transaction txn, ColumnFamilyHandle cfh, ReadOptions ro, Object o) throws IOException {
		Iterator it = new EntrySetIterator(txn, ro, cfh);
		while(it.hasNext()) {
			Entry e = ((Entry)it.next());
			if(e.getValue().equals(o))
				return e;
		}
		return null;
	}

	/**
	 * Return the key/value pair of Map.Entry implementation of the closest key to the passed key template.
	 * May be exact match Up to user. Essentially starts a tailSet iterator seeking nearest key.
	 * @param cfh ColumnFamilyHandle
	 * @param key target key template
	 * @return null if no next for initial iteration
	 * @throws IOException
	 */
	protected Object nearest(ColumnFamilyHandle cfh, Comparable key) throws IOException {
		Iterator<?> it = new TailSetKVIterator(cfh, key, kvStore);
		if(!it.hasNext())
			return null;
		return it.next();
	}
	/**
	 * Return the key/value pair of Map.Entry implementation of the closest key to the passed key template.
	 * May be exact match Up to user. Essentially starts a tailSet iterator seeking nearest key.
	 * @param txn the transaction
	 * @param cfh ColumnFamilyHandle
	 * @param key target key template
	 * @return null if no next for initial iteration
	 * @throws IOException
	 */
	protected Object nearest(Transaction txn, ColumnFamilyHandle cfh, Comparable key) throws IOException {
		Iterator<?> it = new TailSetKVIterator(cfh, key, txn);
		if(!it.hasNext())
			return null;
		return it.next();
	}

	/**
	* Returns iterator vs actual subset. {@link com.neocoretechs.rocksack.iterator.SubSetIterator}
	* 'from' element inclusive, 'to' element exclusive
	* @param cfh ColumnFamilyHandle
	* @param fkey Return from fkey
	* @param tkey Return from fkey to strictly less than tkey
	* @return The Iterator over the subSet {@link com.neocoretechs.rocksack.iterator.SubSetIterator}
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> subSet(ColumnFamilyHandle cfh, Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetIterator(cfh, fkey, tkey, kvStore);
	}
	/**
	* Returns iterator vs actual subset. {@link com.neocoretechs.rocksack.iterator.SubSetIterator}
	* 'from' element inclusive, 'to' element exclusive
	* @param txn Transaction
	* @param cfh ColumnFamilyHandle
	* @param fkey Return from fkey
	* @param tkey Return from fkey to strictly less than tkey
	* @return The Iterator over the subSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> subSet(Transaction txn, ColumnFamilyHandle cfh, Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetIterator(cfh, fkey, tkey, txn);
	}

	/**
	 * Return a Stream that delivers the subset of fkey to tkey. {@link com.neocoretechs.rocksack.stream.SubSetStream}
	 * @param cfh ColumnFamilyHandle
	 * @param fkey the from key
	 * @param tkey the to key
	 * @return the stream from which the lambda expression can be utilized
	 * @throws IOException
	 */
	protected Stream<?> subSetStream(ColumnFamilyHandle cfh, Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetStream(new SubSetIterator(cfh, fkey, tkey, kvStore));
	}
	/**
	 * Return a Stream that delivers the subset of fkey to tkey. {@link com.neocoretechs.rocksack.stream.SubSetStream}
	 * @param txn Transaction
	 * @param cfh ColumnFamilyHandle
	 * @param fkey the from key
	 * @param tkey the to key
	 * @return the stream from which the lambda expression can be utilized
	 * @throws IOException
	 */
	protected Stream<?> subSetStream(Transaction txn, ColumnFamilyHandle cfh, Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetStream(new SubSetIterator(cfh, fkey, tkey, txn));
	}

	/**
	* Not a real subset, returns iterator vs set. {@link com.neocoretechs.rocksack.iterator.SubSetKVIterator}
	* 'from' element inclusive, 'to' element exclusive
	* @param cfh ColumnFamilyHandle
	* @param fkey Return from fkey
	* @param tkey Return from fkey to strictly less than tkey
	* @return The KeyValuePair Iterator over the subSet, implementation of Map.Entry
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> subSetKV(ColumnFamilyHandle cfh, Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetKVIterator(cfh, fkey, tkey, kvStore);
	}
	
	/**
	* Not a real subset, returns iterator vs set. {@link com.neocoretechs.rocksack.iterator.SubSetKVIterator}
	* 'from' element inclusive, 'to' element exclusive
	* @param txn Transaction
	* @param cfh ColumnFamilyHandle
	* @param fkey Return from fkey
	* @param tkey Return from fkey to strictly less than tkey
	* @return The KeyValuePair Iterator over the subSet, implementation of Map.Entry
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> subSetKV(Transaction txn, ColumnFamilyHandle cfh, Comparable fkey, Comparable tkey) throws IOException {
		return new SubSetKVIterator(cfh, fkey, tkey, txn);
	}

	/**
	 * Return a Streamof key/value pairs that delivers the subset of fkey to tkey. {@link com.neocoretechs.rocksack.stream.SubSetKVStream}
	 * @param cfh ColumnFamilyHandle
	 * @param fkey the from key
	 * @param tkey the to key
	 * @return the stream from which the lambda expression can be utilized
	 * @throws IOException
	 */
	protected Stream<?> subSetKVStream(ColumnFamilyHandle cfh, Comparable fkey, Comparable tkey) throws IOException {
			return new SubSetKVStream(cfh, fkey, tkey, kvStore);
	}
	/**
	 * Return a Streamof key/value pairs that delivers the subset of fkey to tkey. {@link com.neocoretechs.rocksack.stream.SubSetKVStream}
	 * @param txn Transaction
	 * @param cfh ColumnFamilyHandle
	 * @param fkey the from key
	 * @param tkey the to key
	 * @return the stream from which the lambda expression can be utilized
	 * @throws IOException
	 */
	protected Stream<?> subSetKVStream(Transaction txn, ColumnFamilyHandle cfh, Comparable fkey, Comparable tkey) throws IOException {
			return new SubSetKVStream(cfh, fkey, tkey, txn);
	}

	/**
	* Not a real subset, returns iterator. {@link com.neocoretechs.rocksack.iterator.EntrySetIterator}
	* @param cfh ColumnFamilyHandle
	* @return The Iterator over the entrySet
	* @exception IOException If we cannot obtain the iterator
	*/
	protected Iterator<?> entrySet(ColumnFamilyHandle cfh) throws IOException {
		return new EntrySetIterator(kvStore, cfh);
	}
	/**
	* Not a real subset, returns iterator. {@link com.neocoretechs.rocksack.iterator.EntrySetIterator}
	* @param txn Transaction
	* @param cfh ColumnFamilyHandle
	* @return The Iterator over the entrySet
	* @exception IOException If we cannot obtain the iterator
	*/
	protected Iterator<?> entrySet(Transaction txn, ColumnFamilyHandle cfh) throws IOException {
		return new EntrySetIterator(txn, new ReadOptions(), cfh);
	}

	/**
	 * Get a stream of entry set. {@link com.neocoretechs.rocksack.stream.EntrySetStream} consisting of Map.Entry
	 * @param cfh ColumnFamilyHandle
	 * @return The Map.Entry Stream of EntrySet
	 * @throws IOException
	 */
	protected Stream<?> entrySetStream(ColumnFamilyHandle cfh) throws IOException {
		return new EntrySetStream(kvStore, cfh);
	}
	/**
	 * Get a Map.Entry stream of entry set. {@link com.neocoretechs.rocksack.stream.EntrySetStream}
	 * @param txn Transaction
	 * @param cfh ColumnFamilyHandle
	 * @return Map.Entry stream over EntrySet
	 * @throws IOException
	 */
	protected Stream<?> entrySetStream(Transaction txn, ColumnFamilyHandle cfh) throws IOException {
		return new EntrySetStream(txn, cfh);
	}

	/**
	* Not a real subset, returns Iterator. {@link com.neocoretechs.rocksack.iterator.HeadSetIterator}
	* @param cfh ColumnFamilyHandle
	* @param tkey return from head to strictly less than tkey
	* @return The Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> headSet(ColumnFamilyHandle cfh, Comparable tkey) throws IOException {
		return new HeadSetIterator(tkey, kvStore, cfh);
	}
	/**
	* Not a real subset, returns Iterator. {@link com.neocoretechs.rocksack.iterator.HeadSetIterator}
	* @param Transaction txn
	* @param cfh ColumnFamilyHandle
	* @param tkey return from head to strictly less than tkey
	* @return The Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> headSet(Transaction txn, ColumnFamilyHandle cfh, Comparable tkey) throws IOException {
		return new HeadSetIterator(tkey, txn, cfh);
	}

	/**
	 * Get a stream of headset. {@link com.neocoretechs.rocksack.stream.HeadSetStream}
	 * @param cfh ColumnFamilyHandle
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> headSetStream(ColumnFamilyHandle cfh, Comparable tkey) throws IOException {
		return new HeadSetStream(tkey, kvStore, cfh);
	}
	/**
	 * Get a stream of headset. {@link com.neocoretechs.rocksack.stream.HeadSetStream}
	 * @param txn Transaction
	 * @param cfh ColumnFamilyHandle
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> headSetStream(Transaction txn, ColumnFamilyHandle cfh, Comparable tkey) throws IOException {
		return new HeadSetStream(tkey, txn, cfh);
	}

	/**
	* Not a real subset, returns Iterator. {@link com.neocoretechs.rocksack.iterator.HeadSetKVIterator}
	* @param cfh ColumnFamilyHandle
	* @param tkey return from head to strictly less than tkey
	* @return The KeyValuePair Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> headSetKV(ColumnFamilyHandle cfh, Comparable tkey) throws IOException {
		return new HeadSetKVIterator(tkey, kvStore, cfh);
	}
	/**
	* Not a real subset, returns Iterator. {@link com.neocoretechs.rocksack.iterator.HeadSetKVIterator}
	* @param txn Transaction
	* @param cfh ColumnFamilyHandle
	* @param tkey return from head to strictly less than tkey
	* @return The KeyValuePair Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> headSetKV(Transaction txn, ColumnFamilyHandle cfh, Comparable tkey) throws IOException {
		return new HeadSetKVIterator(tkey, txn, cfh);
	}

	/**
	 * Get a stream of head set. {@link com.neocoretechs.rocksack.stream.HeadSetKVStream}
	 * @param cfh ColumnFamilyHandle
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> headSetKVStream(ColumnFamilyHandle cfh, Comparable tkey) throws IOException {
		return new HeadSetKVStream(tkey, kvStore, cfh);
	}
	/**
	 * Get a stream of head set. {@link com.neocoretechs.rocksack.stream.HeadSetKVStream}
	 * @param txn Transaction
	 * @param cfh ColumnFamilyHandle
	 * @param tkey
	 * @return The Stream over KV elements
	 * @throws IOException
	 */
	protected Stream<?> headSetKVStream(Transaction txn, ColumnFamilyHandle cfh, Comparable tkey) throws IOException {
		return new HeadSetKVStream(tkey, txn);
	}

	/**
	* Return the keyset Iterator over all elements. {@link com.neocoretechs.rocksack.iterator.KeySetIterator}
	* @param cfh ColumnFamilyHandle
	* @return The Iterator over the keySet
	* @exception IOException If we cannot obtain the iterator
	*/
	protected Iterator<?> keySet(ColumnFamilyHandle cfh) throws IOException {
		return new KeySetIterator(kvStore, cfh);
	}
	/**
	* Return the keyset Iterator over all elements. {@link com.neocoretechs.rocksack.iterator.KeySetIterator}
	* @param txn Transaction
	* @param cfh ColumnFamilyHandle
	* @return The Iterator over the keySet
	* @exception IOException If we cannot obtain the iterator
	*/
	protected Iterator<?> keySet(Transaction txn, ColumnFamilyHandle cfh) throws IOException {
		return new KeySetIterator(txn, cfh);
	}

	/**
	 * Get a keyset stream. {@link com.neocoretechs.rocksack.stream.KeySetStream}
	 * @param cfh ColumnFamilyHandle
	 * @return Stream over KeySet
	 * @throws IOException
	 */
	protected Stream<?> keySetStream(ColumnFamilyHandle cfh) throws IOException {
		return new KeySetStream(kvStore, cfh);
	}
	/**
	 * Get a keyset stream.  {@link com.neocoretechs.rocksack.stream.KeySetStream}
	 * @param txn Transaction
	 * @param cfh ColumnFamilyHandle
	 * @return Stream over KeySet
	 * @throws IOException
	 */
	protected Stream<?> keySetStream(Transaction txn, ColumnFamilyHandle cfh) throws IOException {
		return new KeySetStream(txn, cfh);
	}

	/**
	* Not a real subset, returns Iterator, {@link com.neocoretechs.rocksack.iterator.TailSetIterator}
	* @param cfh ColumnFamilyHandle
	* @param fkey return from value to end
	* @return The Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> tailSet(ColumnFamilyHandle cfh, Comparable fkey) throws IOException {
		return new TailSetIterator(cfh, fkey, kvStore);
	}
	/**
	* Not a real subset, returns Iterator. {@link com.neocoretechs.rocksack.iterator.TailSetIterator}
	* @param txn Transaction
	* @param cfh ColumnFamilyHandle
	* @param fkey return from value to end
	* @return The Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> tailSet(Transaction txn, ColumnFamilyHandle cfh, Comparable fkey) throws IOException {
		return new TailSetIterator(cfh, fkey, txn);
	}

	/**
	 * Return a tail set stream. {@link com.neocoretechs.rocksack.stream.TailSetStream}
	 * @param fkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> tailSetStream(ColumnFamilyHandle cfh, Comparable fkey) throws IOException {
		return new TailSetStream(fkey, kvStore, cfh);
	}
	/**
	 * Return a tail set stream. {@link com.neocoretechs.rocksack.stream.TailSetStream}
	 * @param txn Transaction
	 * @param cfh ColumnFamilyHandle
	 * @param fkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> tailSetStream(Transaction txn, ColumnFamilyHandle cfh, Comparable fkey) throws IOException {
		return new TailSetStream(fkey, txn, cfh);
	}

	/**
	* Not a real subset, returns Iterator. {@link com.neocoretechs.rocksack.iterator.TailSetKVIterator}
	* @param cfh ColumnFamilyHandle
	* @param fkey return from value to end
	* @return The KeyValuePair Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> tailSetKV(ColumnFamilyHandle cfh, Comparable fkey) throws IOException {
		return new TailSetKVIterator(cfh, fkey, kvStore);
	}
	/**
	* Not a real subset, returns Iterator. {@link com.neocoretechs.rocksack.iterator.TailSetKVIterator}
	* @param txn Transaction
	* @param cfh ColumnFamilyHandle
	* @param fkey return from value to end
	* @return The KeyValuePair Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	@SuppressWarnings("rawtypes")
	protected Iterator<?> tailSetKV(Transaction txn, ColumnFamilyHandle cfh, Comparable fkey) throws IOException {
		return new TailSetKVIterator(cfh, fkey, txn);
	}

	/**
	 * Return a tail set key/value stream. {@link com.neocoretechs.rocksack.stream.TailSetKVStream}
	 * @param cfh ColumnFamilyHandle
	 * @param fkey from key of tailset
	 * @return the stream from which the lambda can be utilized
	 * @throws IOException
	 */
	protected Stream<?> tailSetKVStream(ColumnFamilyHandle cfh, Comparable fkey) throws IOException {
		return new TailSetKVStream(fkey, kvStore, cfh);
	}

	/**
	 * Return a tail set key/value stream. {@link com.neocoretechs.rocksack.stream.TailSetKVStream}
	 * @param txn Transaction
	 * @param cfh ColumnFamilyHandle
	 * @param fkey from key of tailset
	 * @return the stream from which the lambda can be utilized
	 * @throws IOException
	 */
	protected Stream<?> tailSetKVStream(Transaction txn, ColumnFamilyHandle cfh, Comparable fkey) throws IOException {
		return new TailSetKVStream(fkey, txn);
	}
	/**
	 * Contains a value object
	 * @param cfh ColumnFamilyHandle
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	protected boolean containsValue(ColumnFamilyHandle cfh, Object o) throws IOException {
		return (getValue(cfh, o) != null);
	}
	/**
	 * Contains a value object
	 * @param txn Transaction
	 * @param cfh ColumnFamilyHandle
	 * @param ro ReadOptions
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	protected boolean containsValue(Transaction txn, ColumnFamilyHandle cfh, ReadOptions ro, Object o) throws IOException {
		return (getValue(txn, cfh, ro, o) != null);
	}


	/**
	 * Contains a value object
	 * @param cfh ColumnFamilyHandle
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected boolean contains(ColumnFamilyHandle cfh, Comparable o) throws IOException {
		Object o2 = get(cfh, o);
		return o2 != null;
	}
	/**
	 * Contains a value object. Does a get since RocksDB doesnt seem to have keymayexist in trans context
	 * @param txn Transaction
	 * @param cfh ColumnFamilyHandle
	 * @param ro readOptions
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected boolean contains(Transaction txn, ColumnFamilyHandle cfh, ReadOptions ro, Comparable o) throws IOException {
		Object o2 = get(txn,cfh,ro,o);
		return o2 != null;
	}

	/**
	* Remove the key and value of the parameter.
	* @param cfh ColumnFamilyHandle
	* @param o The object to remove
	* @return null or previous value object
	*/
	@SuppressWarnings("rawtypes")
	protected Object remove(ColumnFamilyHandle cfh, Comparable o) throws IOException {
		try {
			byte[] b2 = SerializedComparator.serializeObject(o); // key
			byte[] b = kvStore.get(cfh, b2); // b = value
			kvStore.delete(cfh, b2); // serial bytes of key, call to delete
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
	* @param txn The Transaction
	* @param cfh ColumnFamilyHandle
	* @return null or previous object
	*/
	@SuppressWarnings("rawtypes")
	protected Object remove(Transaction txn, ColumnFamilyHandle cfh, ReadOptions ro, Comparable o) throws IOException {
		try {
			byte[] b2 = SerializedComparator.serializeObject(o); // key
			byte[] b = txn.get(ro, cfh, b2); // b = value
			txn.delete(cfh, b2); // serial bytes of key, call to delete
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
	 * @param cfh ColumnFamilyHandle
	 * @return Object from first key
	 * @throws IOException
	 */
	protected Object first(ColumnFamilyHandle cfh) throws IOException {
		Iterator<?> it = new EntrySetIterator(kvStore, cfh);
		if(it.hasNext()) {
			return ((Entry)it.next()).getValue();
		}
		return null;
	}
	/**
	 * Get the value of the object associated with first key
	 * @param txn Transaction
	 * @param cfh ColumnFamilyHandle
	 * @return Object from first key
	 * @throws IOException
	 */
	protected Object first(Transaction txn, ColumnFamilyHandle cfh) throws IOException {
		Iterator<?> it = new EntrySetIterator(txn, new ReadOptions(), cfh);
		if(it.hasNext()) {
			return ((Entry)it.next()).getValue();
		}
		return null;
	}

	/**
	 * Get the first key
	 * @param cfh ColumnFamilyHandle
	 * @return The Comparable first key in the KVStore
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Comparable firstKey(ColumnFamilyHandle cfh) throws IOException {
		if(DEBUG)
			System.out.printf("%s.firstKey for kvStore %s%n", this.getClass().getName(),kvStore);
		Iterator it = new EntrySetIterator(kvStore, cfh);
		if(it.hasNext()) {
			return (Comparable) ((Entry)it.next()).getKey();
		}
		return null;
	}
	/**
	 * Get the first key
	 * @param txn Transaction
	 * @param cfh ColumnFamilyHandle
	 * @return The Comparable first key in the KVStore
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Comparable firstKey(Transaction txn, ColumnFamilyHandle cfh) throws IOException {
		if(DEBUG)
			System.out.printf("%s.firstKey for kvStore %s%n", this.getClass().getName(),txn);
		Iterator it = new EntrySetIterator(txn, new ReadOptions(), cfh);
		if(it.hasNext()) {
			return (Comparable) ((Entry)it.next()).getKey();
		}
		return null;
	}

	/**
	 * Get the last object associated with greatest valued key in the KVStore
	 * @param cfh ColumnFamilyHandle
	 * @return The value of the Object of the greatest key
	 * @throws IOException
	 */
	protected Object last(ColumnFamilyHandle cfh) throws IOException {
		if(DEBUG)
			System.out.printf("%s.last for kvStore %s%n", this.getClass().getName(),kvStore);
		EntrySetIterator it = new EntrySetIterator(kvStore, cfh);
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
	 * @param cfh ColumnFamilyHandle
	 * @return The value of the Object of the greatest key
	 * @throws IOException
	 */
	protected Object last(Transaction txn, ColumnFamilyHandle cfh) throws IOException {
		if(DEBUG)
			System.out.printf("%s.last for kvStore %s%n", this.getClass().getName(),txn);
		EntrySetIterator it = new EntrySetIterator(txn, new ReadOptions(), cfh);
		RocksIterator ri = it.getIterator();
		ri.seekToLast();
		if(ri.isValid()) {
			return SerializedComparator.deserializeObject(ri.value());
		}
		return null;
	}

	/**
	 * Get the last key in the KVStore
	 * @param cfh ColumnFamilyHandle
	 * @return The last, greatest valued key in the KVStore.
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Comparable lastKey(ColumnFamilyHandle cfh) throws IOException {
		if(DEBUG)
			System.out.printf("%s.lastKey for kvStore %s%n", this.getClass().getName(),kvStore);
		EntrySetIterator it = new EntrySetIterator(kvStore, cfh);
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
	 * @param cfh ColumnFamilyHandle
	 * @return The last, greatest valued key in the KVStore.
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Comparable lastKey(Transaction txn, ColumnFamilyHandle cfh) throws IOException {
		if(DEBUG)
			System.out.printf("%s.lastKey for kvStore %s%n", this.getClass().getName(),txn);
		EntrySetIterator it = new EntrySetIterator(txn, new ReadOptions(), cfh);
		RocksIterator ri = it.getIterator();
		ri.seekToLast();
		if(ri.isValid()) {
			return (Comparable) SerializedComparator.deserializeObject(ri.key());
		}
		return null;
	}

	/**
	 * Get the number of keys total.
	 * @param cfh ColumnFamilyHandle
	 * @return The size of the KVStore.
	 * @throws IOException
	 */
	protected long size(ColumnFamilyHandle cfh) throws IOException {
		Iterator<?> it = new KeySetIterator(kvStore, cfh);
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
	 * @param cfh ColumnFamilyHandle
	 * @return The size of the KVStore.
	 * @throws IOException
	 */
	protected long size(Transaction txn, ColumnFamilyHandle cfh) throws IOException {
		Iterator<?> it = new KeySetIterator(txn, cfh);
		long cnt = 0;
		while(it.hasNext()) {
			it.next();
			++cnt;
		}
		return cnt;
	}

	/**
	 * Is the KVStore empty?
	 * @param cfh ColumnFamilyHandle
	 * @return true if it is empty.
	 * @throws IOException
	 */
	protected boolean isEmpty(ColumnFamilyHandle cfh) throws IOException {
		Iterator<?> it = new KeySetIterator(kvStore, cfh);
		if(it.hasNext()) {
			return true;
		}
		return false;
	}
	
	/**
	 * Is map empty?
	 * @param txn the Transaction
	 * @param cfh ColumnFamilyHandle
	 * @return
	 * @throws IOException
	 */
	protected boolean isEmpty(Transaction txn, ColumnFamilyHandle cfh) throws IOException {
		Iterator<?> it = new KeySetIterator(txn, cfh);
		if(it.hasNext()) {
			return true;
		}
		return false;
	}
	
	/**
	 * Drop the column family
	 * @param cfh ColumnFamilyHandle
	 * @throws IOException
	 */
	protected void dropColumn(ColumnFamilyHandle cfh) throws IOException {
		try {
			kvStore.dropColumnFamily(cfh);
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}
	
	/**
	* Close this session.
	* @exception IOException For low level failure
	*/
	public void Close() throws IOException {
		kvStore.close();
	}
	/**
	* Close this session.
	* @param db The database transaction
	* @exception IOException For low level failure
	*/
	public void Close(Transaction db) throws IOException {
		db.close();
	}
	
	/**
	 * 
	 * @return The RocksDB kvStore
	 */
	protected RocksDB getKVStore() { return kvStore; }
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName()+" using DB:"+getDBname();
	}
	
}
