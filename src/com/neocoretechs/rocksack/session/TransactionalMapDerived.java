package com.neocoretechs.rocksack.session;
import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.WriteOptions;

/*
* Copyright (c) 2024, NeoCoreTechs
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
* TransactionalMapDerived. The same underlying session objects are used here but the user has access to the transactional
* Semantics underlying the recovery protocol. Thread safety is enforced on the session at this level.
* We add an additional constructor to use a previously created {@link TransactionSession} and instantiate an new Transaction instance.
* In the adapter, we retrieve an existing map, extract the session, and instantiate a new {@link TransactionalMap}
* or {@link TransactionalMapDerived}.
* @author Jonathan Groff (C) NeoCoreTechs 2024
*/
public class TransactionalMapDerived extends TransactionalMap {
	private static boolean DEBUG = false;
	ColumnFamilyHandle columnFamilyHandle = null;
	ColumnFamilyDescriptor columnFamilyDescriptor = null;
	/**
	 * Calls processColumnFamily with session.derivedClassFound and derivedClassName
	 * @param session
	 * @param txn
	 * @param derivedClassName
	 * @throws IOException
	 * @throws IllegalAccessException
	 * @throws RocksDBException
	 */
	public TransactionalMapDerived(TransactionSession session, Transaction txn) throws IOException, IllegalAccessException, RocksDBException {
		super(session, txn);
		ro = new ReadOptions();
		wo = new WriteOptions();
		this.session = session;
		this.txn = txn;
	    processColumnFamily();
	}
	/**
	 * Calls processColumnFamily with session.derivedClassFound and derivedClassName
	 * @param session
	 * @param xid
	 * @param derivedClassName
	 * @throws IOException
	 * @throws IllegalAccessException
	 * @throws RocksDBException
	 */
	public TransactionalMapDerived(TransactionSession session, String xid) throws IOException, IllegalAccessException, RocksDBException {
		super(session, xid);
		ro = new ReadOptions();
		wo = new WriteOptions();
		this.session = session;
		BeginTransaction();
		txn.setName(xid);
		processColumnFamily();
	}
	/**
	 * Calls processColumnFamily with session.derivedClassFound and derivedClassName
	 * @param session
	 * @param txn
	 * @param derivedClassName
	 * @throws IOException
	 * @throws IllegalAccessException
	 * @throws RocksDBException
	 */
	public TransactionalMapDerived(TransactionSession session, Transaction txn, String derivedClassName) throws IOException, IllegalAccessException, RocksDBException {
		super(session, txn);
		ro = new ReadOptions();
		wo = new WriteOptions();
		this.session = session;
		this.txn = txn;
	    processColumnFamily(derivedClassName);
	}
	/**
	 * Calls processColumnFamily with session.derivedClassFound and derivedClassName
	 * @param session
	 * @param xid
	 * @param derivedClassName
	 * @throws IOException
	 * @throws IllegalAccessException
	 * @throws RocksDBException
	 */
	public TransactionalMapDerived(TransactionSession session, String xid, String derivedClassName) throws IOException, IllegalAccessException, RocksDBException {
		super(session, xid);
		ro = new ReadOptions();
		wo = new WriteOptions();
		this.session = session;
		BeginTransaction();
		txn.setName(xid);
		processColumnFamily(derivedClassName);
	}
	/**
	 * Generates columnFamilyHandle and columnFamilydescriptor
	 * calls createColumnFamily for database if found is false
	 * @param found
	 * @param derivedClassName
	 * @throws RocksDBException
	 */
	private void processColumnFamily() throws RocksDBException {
		boolean found = false;
		int index = 0;
		for(ColumnFamilyHandle cfh: this.session.columnFamilyHandles) {
			this.columnFamilyHandle = this.session.columnFamilyHandles.get(index++);
			if(new String(cfh.getName()).equals(new String(TransactionDB.DEFAULT_COLUMN_FAMILY))) {
				found = true;
				break;
			}
		}
		index = 0;
		for(ColumnFamilyDescriptor cfd: this.session.columnFamilyDescriptor) {
			this.columnFamilyDescriptor = this.session.columnFamilyDescriptor.get(index++);
			if(new String(cfd.getName()).equals(new String(TransactionDB.DEFAULT_COLUMN_FAMILY))) {
				break;
			}
		}
		// make sure it's legit
		if(!found)
			throw new RocksDBException("columnFamilyHandle name default not found");
	}
	
	/**
	 * Generates columnFamilyHandle and columnFamilydescriptor
	 * calls createColumnFamily for database if found is false
	 * @param found
	 * @param derivedClassName
	 * @throws RocksDBException
	 */
	private void processColumnFamily(String derivedClassName) throws RocksDBException {
		if(DEBUG)
			System.out.printf("%s.processColumnFamily derived:%s%n",this.getClass().getName(),derivedClassName);
		boolean found = false;
		int index = 0;
		for(ColumnFamilyHandle cfh: this.session.columnFamilyHandles) {
			this.columnFamilyHandle = this.session.columnFamilyHandles.get(index++);
			if(new String(cfh.getName()).equals(derivedClassName)) {
				found = true;
				break;
			}
		}
		if(found) {
			index = 0;
			for(ColumnFamilyDescriptor cfd: this.session.columnFamilyDescriptor) {
				this.columnFamilyDescriptor = this.session.columnFamilyDescriptor.get(index++);
				if(new String(cfd.getName()).equals(derivedClassName)) {
					break;
				}
			}
		} else {
			this.columnFamilyDescriptor = new ColumnFamilyDescriptor(derivedClassName.getBytes(), DatabaseManager.getDefaultColumnFamilyOptions());
			this.session.columnFamilyDescriptor.add(this.columnFamilyDescriptor);
			this.columnFamilyHandle = this.session.kvStore.createColumnFamily(this.columnFamilyDescriptor);
		}
		// make sure it's legit
		if(!new String(this.columnFamilyHandle.getName()).equals(derivedClassName) ||
				!new String(this.columnFamilyDescriptor.getName()).equals(derivedClassName))
			throw new RocksDBException("columnFamilyHandle name "+(new String(this.columnFamilyHandle.getName()))+" or descriptor does not match target:"+derivedClassName);
	}
	
	
	public TransactionSession getSession() throws IOException {
		session.waitOpen();
		return session;
	}
	
	public Transaction getTransaction() {
		return txn;
	}
	
	public void setReadOptions(ReadOptions ro) {
		this.ro = ro;
	}
	public void setWriteOptions(WriteOptions wo) {
		this.wo = wo;
	}
	/**
	* Put a  key/value pair to main cache and pool.
	* @param tkey The key for the pair
	* @param tvalue The value for the pair
	* @return true if key previously existed and was not added
	* @exception IOException if put to backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public boolean put(Comparable tkey, Object tvalue) throws IOException {
		synchronized (getSession().getMutexObject()) {
				// now put new
				return session.put(txn, tkey, tvalue);
		}
	}
	/**
	* Put a  key/value pair to main cache and pool. 
	* @param tkey The key for the pair, will not be serialized
	* @param tvalue The value for the pair
	* @return true if key previously existed and was not added
	* @exception IOException if put to backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public boolean putViaBytes(byte[] tkey, Object tvalue) throws IOException {
		synchronized (getSession().getMutexObject()) {
				// now put new
				return session.putViaBytes(txn, tkey, tvalue);
		}
	}
	/**
	* Get a value from backing store if not in cache.
	* @param tkey The key for the value
	* @return The value for the key
	* @exception IOException if get from backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public Object get(Comparable tkey) throws IOException {
		//synchronized (session.getMutexObject()) {
			return getSession().get(txn, ro, tkey);
		//}
	}
	/**
	* Get a value from backing store if not in cache.
	* @param tkey The key for the value
	* @return The value for the key
	* @exception IOException if get from backing store fails
	*/
	public Object getViaBytes(byte[] tkey) throws IOException {
		//synchronized (session.getMutexObject()) {
			return getSession().getViaBytes(txn, ro, tkey);
		//}
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
		synchronized (getSession().getMutexObject()) {
				return session.getValue(txn, ro, tkey);
		}
	}	

	/**
	* Return the number of elements in the backing store
 	* @return A long value of number of elements
	* @exception IOException If backing store retrieval failure
	*/
	public long size() throws IOException {
		synchronized (getSession().getMutexObject()) {
				return session.size(txn);
		}
	}

	/**
	* Obtain iterator over the entrySet. Retrieve from backing store if not in cache.
	* @return The Iterator for all elements
	* @exception IOException if get from backing store fails
	*/
	public Iterator<?> entrySet() throws IOException {
		synchronized (getSession().getMutexObject()) {
				return session.entrySet(txn);
		}
	}
	
	public Stream<?> entrySetStream() throws IOException {
		synchronized (getSession().getMutexObject()) {
				return session.entrySetStream(txn);
		}
	}
	/**
	* Get a keySet iterator. Get from backing store if not in cache.
	* @return The iterator for the keys
	* @exception IOException if get from backing store fails
	*/
	public Iterator<?> keySet() throws IOException {
		synchronized (getSession().getMutexObject()) {
				return session.keySet(txn);
		}
	}
	
	public Stream<?> keySetStream() throws IOException {
		synchronized (getSession().getMutexObject()) {
				return session.keySetStream(txn);
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
		synchronized (getSession().getMutexObject()) {
			return  session.contains(txn, ro, tkey);
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
		synchronized (getSession().getMutexObject()) {
			return  session.containsValue(txn, ro, value);
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
		synchronized (getSession().getMutexObject()) {
			return session.remove(txn, ro, tkey);
		}
	}
	/**
	* @return First key in set
	* @exception IOException If backing store retrieval failure
	*/
	public Comparable firstKey() throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.firstKey(txn);
		}
	}
	/**
	* @return Last key in set
	* @exception IOException If backing store retrieval failure
	*/
	public Comparable lastKey() throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.lastKey(txn);
		}
	}
	/**
	* Return the last value in the set
	* @return The last element in the set
	* @exception IOException If backing store retrieval failure
	*/
	public Object last() throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.last(txn);
		}
	}
	/**
	* Return the first element
	* @return A long value of number of elements
	* @exception IOException If backing store retrieval failure
	*/
	public Object first() throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.first(txn);
		}
	}
	/**
	 * Return the element nearest to given key
	 * @param tkey the key to sear for
	 * @return the key/value of closest element to tkey
	 * @throws IOException
	 */
	public Object nearest(Comparable tkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.nearest(txn, tkey);
		}
	}
	/**
	* @param tkey Strictly less than 'to' this element
	* @return Iterator of first to tkey
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> headMap(Comparable tkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.headSet(txn, tkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> headMapStream(Comparable tkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.headSetStream(txn, tkey);
		}
	}
	/**
	* @param tkey Strictly less than 'to' this element
	* @return Iterator of first to tkey returning KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> headMapKV(Comparable tkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.headSetKV(txn, tkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> headMapKVStream(Comparable tkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.headSetKVStream(txn, tkey);
		}
	}
	/**
	* @param fkey Greater or equal to 'from' element
	* @return Iterator of objects from fkey to end
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> tailMap(Comparable fkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.tailSet(txn, fkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> tailMapStream(Comparable fkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.tailSetStream(txn, fkey);
		}
	}
	/**
	* @param fkey Greater or equal to 'from' element
	* @return Iterator of objects from fkey to end which are KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	@SuppressWarnings("rawtypes")
	public Iterator<?> tailMapKV(Comparable fkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.tailSetKV(txn, fkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> tailMapKVStream(Comparable fkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.tailSetKVStream(txn, fkey);
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
		synchronized (getSession().getMutexObject()) {
			return session.subSet(txn, fkey, tkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> subMapStream(Comparable fkey, Comparable tkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.subSetStream(txn, fkey, tkey);
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
		synchronized (getSession().getMutexObject()) {
			return session.subSetKV(txn, fkey, tkey);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public Stream<?> subMapKVStream(Comparable fkey, Comparable tkey)
		throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.subSetKVStream(txn, fkey, tkey);
		}
	}
	
	/**
	* Return boolean value indicating whether the map is empty
	* @return true if empty
	* @exception IOException If backing store retrieval failure
	*/
	public boolean isEmpty() throws IOException {
		synchronized (getSession().getMutexObject()) {
				return session.isEmpty(txn);
		}
	}
	
	public String getTransactionId() throws IOException {
		synchronized (getSession().getMutexObject()) {
			return txn.getName();
		}
	}

	@Override
	public String getDBName() {
		return session.getDBname();
	}

	@Override
	public Object getMutexObject() {
		return session.getMutexObject();
	}
	

	@Override
	public Iterator<?> iterator() throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.keySet(txn);
		}
	}

	@Override
	public boolean contains(Comparable o) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.contains(txn, ro, o);
		}
	}

	@Override
	public void Open() throws IOException {
		synchronized (session.getMutexObject()) {
			
		}
	}

	@Override
	public void Close() throws IOException {		
		synchronized (getSession().getMutexObject()) {
			session.getKVStore().close();
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
		synchronized (getSession().getMutexObject()) {
			return session.subSet(txn, fkey, tkey);
		}
	}

	@Override
	public Stream<?> subSetStream(Comparable fkey, Comparable tkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.subSetStream(txn, fkey, tkey);
		}
	}

	@Override
	public Iterator<?> headSet(Comparable tkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.headSet(txn, tkey);
		}
	}

	@Override
	public Stream<?> headSetStream(Comparable tkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.headSetStream(txn, tkey);
		}
	}

	@Override
	public Iterator<?> tailSet(Comparable fkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.tailSet(txn, fkey);
		}
	}

	@Override
	public Stream<?> tailSetStream(Comparable fkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.tailSetStream(txn, fkey);
		}
	}

	@Override
	public Iterator<?> subSetKV(Comparable fkey, Comparable tkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.subSetKV(txn, fkey, tkey);
		}
	}

	@Override
	public Stream<?> subSetKVStream(Comparable fkey, Comparable tkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.subSetKVStream(txn, fkey, tkey);
		}
	}

	@Override
	public Iterator<?> headSetKV(Comparable tkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.headSetKV(txn, tkey);
		}
	}

	@Override
	public Stream<?> headSetKVStream(Comparable tkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.headSetKVStream(txn, tkey);
		}
	}

	@Override
	public Iterator<?> tailSetKV(Comparable fkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.tailSetKV(txn, fkey);
		}
	}

	@Override
	public Stream<?> tailSetKVStream(Comparable fkey) throws IOException {
		synchronized (getSession().getMutexObject()) {
			return session.tailSetKVStream(txn, fkey);
		}
	}

	public void BeginTransaction() throws IOException {
		txn = getSession().getKVStore().beginTransaction(wo);	
	}

	@Override
	public String toString() {
		return (session == null ? "TransactionalMap Session NULL" : session.toString())+" Transaction:"+
				(txn == null ? "TransactionalMap transaction NULL" : txn.getName());
	}

}
