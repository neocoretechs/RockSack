package com.neocoretechs.rocksack.session;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Stream;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

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
* such as BTree and HMap..  Operations include
* handing out iterators, inserting and deleting objects, size, navigation, clearing,
* and handling commit and rollback.
* @author Jonathan Groff (C) NeoCoreTechs 2003, 2017, 2021
*/
final class RockSackSession implements TransactionInterface {
	private boolean DEBUG = false;
	private int uid;
	private int gid;
	private RocksDB kvStore;
	private Options options;

	/**
	* Create a new session
	* @param kvMain The {@link KeyValueMainInterface} Main object than handles the key pages indexing the objects in the deep store.
	* @param tuid The user
	* @param tgis The group
	* @exception IOException If global IO problem
	*/
	protected RockSackSession(RocksDB kvStore, Options options, int uid, int gid)  {
		this.kvStore = kvStore;
		this.options = options;
		this.uid = uid;
		this.gid = gid;
		if( DEBUG )
			System.out.println("RockSackSession constructed with db:"+getDBPath());
	}

	public long getTransactionId() { return -1L; }
	
	protected String getDBname() {
		return kvStore.getName();
	}
	
	protected String getDBPath() {
		return options.dbPaths().get(0).toString(); // primary path?
	}

	@Override
	public String toString() {
		return "RockSackSession using DB:"+getDBname()+" path:"+getDBPath();
	}
	
	@Override
	public int hashCode() {
		return Integer.hashCode((int)getTransactionId());
	}
	
	@Override
	public boolean equals(Object o) {
		return( getTransactionId() == ((Long)o).longValue());
	}
	
	protected int getUid() {
		return uid;
	}
	protected int getGid() {
		return gid;
	}

	protected Object getMutexObject() {
		return kvStore;
	}


	/**
	 * Call the add method of KeyValueMainInterface.
	 * @param key The key value to attempt add
	 * @param o The value for the key to add
	 * @return true if the key existed and was not added
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
	 * Cause the KvStore to seekKey for the Comparable type.
	 * @param o the Comparable object to seek.
	 * @return the Key/Value object from the retrieved node
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected Object get(Comparable o) throws IOException {
		   try {
			   return new KeyValue(o,SerializedComparator.deserializeObject(kvStore.get(SerializedComparator.serializeObject(o))));
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
	* Not a real subset, returns iterator vs set.
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
	* Not a real subset, returns iterator
	* @return The Iterator over the entrySet
	* @exception IOException If we cannot obtain the iterator
	*/
	protected Iterator<?> entrySet() throws IOException {
		return new EntrySetIterator(kvStore);
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
	 * Get a stream of headset
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> headSetStream(Comparable tkey) throws IOException {
		return new HeadSetStream(tkey, kvStore);
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
	 * Get a stream of head set
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> headSetKVStream(Comparable tkey) throws IOException {
		return new HeadSetKVStream(tkey, kvStore);
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
	 * Get a keyset stream
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> keySetStream() throws IOException {
		return new KeySetStream(kvStore);
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
	 * Return a tail set stream
	 * @param fkey
	 * @return
	 * @throws IOException
	 */
	protected Stream<?> tailSetStream(Comparable fkey) throws IOException {
		return new TailSetStream(fkey, kvStore);
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
	 * Return a tail set key/value stream
	 * @param fkey from key of tailset
	 * @return the stream from which the lambda can be utilized
	 * @throws IOException
	 */
	protected Stream<?> tailSetKVStream(Comparable fkey) throws IOException {
		return new TailSetKVStream(fkey, kvStore);
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
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	protected boolean contains(Comparable o) throws IOException {
		return kvStore.keyMayExist(ByteBuffer.wrap(SerializedComparator.serializeObject(o)));
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
			if(b != null) {
				kvStore.delete(SerializedComparator.serializeObject(b2)); // serial bytes of key, call to delete
				return SerializedComparator.serializeObject(b); // serialize previous value from retrieved bytes
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
	 * Get the number of keys total.
	 * @return The size of the KVStore.
	 * @throws IOException
	 */
	protected long size() throws IOException {
		Iterator it = new KeySetIterator(kvStore);
		long cnt = 0;
		while(it.hasNext()) {
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
		return size() == 0;
	}

	/**
	* Close this session.
	* @param rollback true to roll back, false to commit
	* @exception IOException For low level failure
	*/
	public void Close(boolean rollback) throws IOException {
		rollupSession(rollback);
	}
	/**
	 * Open the files associated with the BTree for the instances of class
	 * @throws IOException
	 */
	protected void Open() throws IOException {
	}
	
	/**
	* @exception IOException for low level failure
	*/
	public void Rollback() throws IOException {
	}
	
	/**
	* Commit the blocks.
	* @exception IOException For low level failure
	*/
	public void Commit() throws IOException {
	}
	/**
	 * Checkpoint the current transaction
	 * @throws IOException 
	 * @throws IllegalAccessException 
	 */
	public void Checkpoint() throws IllegalAccessException, IOException {

	}
	/**
	* Generic session roll up.  Data is committed based on rollback param.
	* We deallocate the outstanding block
	* We iterate the tablespaces for each db removing obsolete log files.
	* Remove the WORKER threads from KeyValueMain, then remove this session from the SessionManager
	* @param rollback true to roll back, false to commit
	* @exception IOException For low level failure
	*/
	public void rollupSession(boolean rollback) throws IOException {
		if (rollback) {
			
		} else {
	
		}
		kvStore.close();
	}
	
	/**
	* This forces a close with rollback.
	* for offlining of db's
	* @exception IOException if low level error occurs
	*/
	protected void forceClose() throws IOException {
		Close(true);
	}

	protected RocksDB getKVStore() { return kvStore; }
	
	
}
