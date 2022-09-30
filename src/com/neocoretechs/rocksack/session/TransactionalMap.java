package com.neocoretechs.rocksack.session;
import java.io.IOException;
import java.util.Iterator;
import java.util.Stack;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

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
* TransactionalMap. The same underlying session objects are used here but the user has access to the transactional
* Semantics underlying the recovery protocol. Thread safety is enforced on the session at this level.
* Java Map backed by pooled serialized objects. It is the users responsibility to commit/rollback/checkpoint.
* We add an additional constructor to use a previously created session and instantiate an new Transaction instance.
* In the adapter, we retrieve an existing map, extract the session, and instantiate a new TransactionalMap.
* The assumption is that the new TransactionalMap is stored in an additional session container outside of the adapter.
* @author Jonathan Groff (C) NeoCoreTechs 2003,2014,2017,2021,2022
*/
public class TransactionalMap implements OrderedKVMapInterface {
	protected RockSackTransactionSession session;
	Transaction txn;
	ReadOptions ro = new ReadOptions();
	WriteOptions wo = new WriteOptions();
	/**
	 * @param dbname
	 * @param database Database type i.e. RocksDB
	 * @param backingStore "MMap or "File" etc.
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public TransactionalMap(String dbname, String database, String backingStore) throws IOException, IllegalAccessException {
		session = SessionManager.ConnectTransaction(dbname, database, backingStore);
	}
	
	/**
	 * @param session Existing transactional database previously opened and ready for new transaction context
	 * @throws IOException
	 * @throws IllegalAccessException
	 */
	public TransactionalMap(RockSackTransactionSession session) throws IOException, IllegalAccessException {
		this.session = session;
	}
	
	public Transaction getTransaction() {
		return txn;
	}
	/**
	* Put a  key/value pair to main cache and pool.  We may
	* toss out an old one when cache size surpasses objectCacheSize
	* @param tkey The key for the pair
	* @param tvalue The value for the pair
	* @return true if key previously existed and was not added
	* @exception IOException if put to backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public boolean put(Comparable tkey, Object tvalue) throws IOException {
		synchronized (session.getMutexObject()) {
				// now put new
				return session.put(txn, tkey, tvalue);
		}
	}
	
	/**
	* Get a value from backing store if not in cache.
	* We may toss out one to make room if size surpasses objectCacheSize
	* @param tkey The key for the value
	* @return The value for the key
	* @exception IOException if get from backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public Object get(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.get(txn, ro, tkey);
		}
	}
	
	/**
	* Get a value from backing store if not in cache.
	* We may toss out one to make room if size surpasses objectCacheSize
	* @param tkey The key for the value
	* @return The {@link Entry} from RockSack iterator Entry derived from Map.Entry for the key
	* @exception IOException if get from backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public Object getValue(Object tkey) throws IOException {
		synchronized (session.getMutexObject()) {
				return session.getValue(txn, ro, tkey);
		}
	}	

	/**
	* Return the number of elements in the backing store
 	* @return A long value of number of elements
	* @exception IOException If backing store retrieval failure
	*/
	public long size() throws IOException {
		synchronized (session.getMutexObject()) {
				return session.size();
		}
	}

	/**
	* Obtain iterator over the entrySet. Retrieve from backing store if not in cache.
	* @return The Iterator for all elements
	* @exception IOException if get from backing store fails
	*/
	public Iterator<?> entrySet() throws IOException {
		synchronized (session.getMutexObject()) {
				return session.entrySet();
		}
	}
	
	public Stream<?> entrySetStream() throws IOException {
		synchronized (session.getMutexObject()) {
				return session.entrySetStream();
		}
	}
	/**
	* Get a keySet iterator. Get from backing store if not in cache.
	* @return The iterator for the keys
	* @exception IOException if get from backing store fails
	*/
	public Iterator<?> keySet() throws IOException {
		synchronized (session.getMutexObject()) {
				return session.keySet();
		}
	}
	
	public Stream<?> keySetStream() throws IOException {
		synchronized (session.getMutexObject()) {
				return session.keySetStream();
		}
	}
	/**
	* Returns true if the collection contains the given key
	* @param tkey The key to match
	* @return true if in, or false if absent
	* @exception IOException If backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public boolean containsKey(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return  session.contains(tkey);
		}
	}
	/**
	* Returns true if the collection contains the given value object
	* @param value The value to match
	* @return true if in, false if absent
	* @exception IOException If backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public boolean containsValue(Object value) throws IOException {
		synchronized (session.getMutexObject()) {
			return  session.containsValue(value);
		}
	}
	/**
	* Remove object from cache and backing store.
	* @param tkey The key to match
	* @return The removed object
	* @exception IOException If backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public Object remove(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.remove(tkey);
		}
	}
	/**
	* @return First key in set
	* @exception IOException If backing store retrieval failure
	*/
	public Comparable firstKey() throws IOException {
		synchronized (session.getMutexObject()) {
			return session.firstKey();
		}
	}
	/**
	* @return Last key in set
	* @exception IOException If backing store retrieval failure
	*/
	public Comparable lastKey() throws IOException {
		synchronized (session.getMutexObject()) {
			return session.lastKey();
		}
	}
	/**
	* Return the last value in the set
	* @return The last element in the set
	* @exception IOException If backing store retrieval failure
	*/
	public Object last() throws IOException {
		synchronized (session.getMutexObject()) {
			return session.last();
		}
	}
	/**
	* Return the first element, we have to bypass cache for this because
	* of our random throwouts
	* @return A long value of number of elements
	* @exception IOException If backing store retrieval failure
	*/
	public Object first() throws IOException {
		synchronized (session.getMutexObject()) {
			return session.first();
		}
	}
	/**
	* @param tkey Strictly less than 'to' this element
	* @return Iterator of first to tkey
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> headMap(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.headSet(tkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> headMapStream(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.headSetStream(tkey);
		}
	}
	/**
	* @param tkey Strictly less than 'to' this element
	* @return Iterator of first to tkey returning KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> headMapKV(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.headSetKV(tkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> headMapKVStream(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.headSetKVStream(tkey);
		}
	}
	/**
	* @param fkey Greater or equal to 'from' element
	* @return Iterator of objects from fkey to end
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> tailMap(Comparable fkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.tailSet(fkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> tailMapStream(Comparable fkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.tailSetStream(fkey);
		}
	}
	/**
	* @param fkey Greater or equal to 'from' element
	* @return Iterator of objects from fkey to end which are KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> tailMapKV(Comparable fkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.tailSetKV(fkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> tailMapKVStream(Comparable fkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.tailSetKVStream(fkey);
		}
	}
	/**
	* @param fkey 'from' element inclusive 
	* @param tkey 'to' element exclusive
	* @return Iterator of objects in subset from fkey to tkey
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> subMap(Comparable fkey, Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.subSet(fkey, tkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> subMapStream(Comparable fkey, Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.subSetStream(fkey, tkey);
		}
	}
	/**
	* @param fkey 'from' element inclusive 
	* @param tkey 'to' element exclusive
	* @return Iterator of objects in subset from fkey to tkey composed of KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> subMapKV(Comparable fkey, Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.subSetKV(fkey, tkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> subMapKVStream(Comparable fkey, Comparable tkey)
		throws IOException {
		synchronized (session.getMutexObject()) {
			return session.subSetKVStream(fkey, tkey);
		}
	}
	
	/**
	* Return boolean value indicating whether the map is empty
	* @return true if empty
	* @exception IOException If backing store retrieval failure
	*/
	public boolean isEmpty() throws IOException {
		synchronized (session.getMutexObject()) {
				return session.isEmpty();
		}
	}
	
	/**
	 * Commit the outstanding transaction
	 * @throws IOException
	 */
	public void Commit() throws IOException {
		synchronized (session.getMutexObject()) {
			session.Commit(txn);
		}
	}
	
	/**
	 * Checkpoint the current database transaction state for roll forward recovery in event of crash
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public void Checkpoint() throws IllegalAccessException, IOException {
		synchronized (session.getMutexObject()) {
			session.Checkpoint(txn);
		}
	}
	
	/**
	 * Roll back the outstanding transactions
	 * @throws IOException
	 */
	public void Rollback() throws IOException {
		synchronized (session.getMutexObject()) {
			session.Rollback(txn);
		}
	}

	public long getTransactionId() {
		synchronized (session.getMutexObject()) {
			return session.getTransactionId();
		}
	}

	public void Close(boolean rollback) throws IOException {
		rollupSession(rollback);
	}

	public void rollupSession(boolean rollback) throws IOException {
		synchronized (session.getMutexObject()) {
			if(rollback)
				session.Rollback(txn);
			else
				session.Commit(txn);
		}
	}

	@Override
	public String getDBName() {
		return session.getDBname();
	}

	@Override
	public String getDBPath() {
		return session.getDBPath();
	}

	@Override
	public int getUid() {
		return session.getUid();
	}

	@Override
	public int getGid() {
		return session.getGid();
	}

	@Override
	public Object getMutexObject() {
		return session.getMutexObject();
	}
	

	@Override
	public Iterator<?> iterator() throws IOException {
		synchronized (session.getMutexObject()) {
			return session.keySet();
		}
	}

	@Override
	public boolean contains(Comparable o) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.contains(o);
		}
	}

	@Override
	public void Open() throws IOException {
		synchronized (session.getMutexObject()) {
			
		}
	}

	@Override
	public void forceClose() throws IOException {
		synchronized (session.getMutexObject()) {
			session.Close();
		}
		
	}

	@Override
	public RocksDB getKVStore() {
		synchronized (session.getMutexObject()) {
			return session.getKVStore();
		}
	}

	@Override
	public Iterator<?> subSet(Comparable fkey, Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.subSet(fkey, tkey);
		}
	}

	@Override
	public Stream<?> subSetStream(Comparable fkey, Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.subSetStream(fkey, tkey);
		}
	}

	@Override
	public Iterator<?> headSet(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.headSet(tkey);
		}
	}

	@Override
	public Stream<?> headSetStream(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.headSetStream(tkey);
		}
	}

	@Override
	public Iterator<?> tailSet(Comparable fkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.tailSet(fkey);
		}
	}

	@Override
	public Stream<?> tailSetStream(Comparable fkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.tailSetStream(fkey);
		}
	}

	@Override
	public Iterator<?> subSetKV(Comparable fkey, Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.subSetKV(fkey, tkey);
		}
	}

	@Override
	public Stream<?> subSetKVStream(Comparable fkey, Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.subSetKVStream(fkey, tkey);
		}
	}

	@Override
	public Iterator<?> headSetKV(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.headSetKV(tkey);
		}
	}

	@Override
	public Stream<?> headSetKVStream(Comparable tkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.headSetKVStream(tkey);
		}
	}

	@Override
	public Iterator<?> tailSetKV(Comparable fkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.tailSetKV(fkey);
		}
	}

	@Override
	public Stream<?> tailSetKVStream(Comparable fkey) throws IOException {
		synchronized (session.getMutexObject()) {
			return session.tailSetKVStream(fkey);
		}
	}

	public void BeginTransaction() {
		txn = session.getKVStore().beginTransaction(wo);	
	}

}
