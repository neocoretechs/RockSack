package com.neocoretechs.rocksack.session;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.WriteOptions;

import com.neocoretechs.rocksack.Alias;
import com.neocoretechs.rocksack.TransactionId;

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
* or {@link TransactionalMap}.
* @author Jonathan Groff (C) NeoCoreTechs 2024
*/
public class TransactionalMap implements TransactionOrderedKVMapInterface {
	private static boolean DEBUG = false;
	protected TransactionSession session;
	private String className;
	ColumnFamilyHandle columnFamilyHandle = null;
	ColumnFamilyDescriptor columnFamilyDescriptor = null;

	/**
	 * Calls processColumnFamily with derivedClassName if derived is true, no args otherwise.
	 * @param session The TransactionSession that manages this instance
	 * @param className The class name this map represents. Used to form transaction name and derived column family info
	 * @param isDerived true if this is to represent a derived class, such that the column family handle and descriptor can be properly identified
	 * @throws IOException
	 * @throws IllegalAccessException
	 * @throws RocksDBException
	 */
	public TransactionalMap(TransactionSession session, String className, boolean isDerived) throws IOException, IllegalAccessException, RocksDBException {
		this.session = session;
		this.className = className;
		if(DEBUG)
			System.out.printf("%s %s %b%n", this.getClass().getName(), className, isDerived);
		if(isDerived)
			processColumnFamily(className);
		else
			processColumnFamily();
	}

	/**
	 * Generates columnFamilyHandle and columnFamilydescriptor
	 * calls createColumnFamily for database if found is false
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

	public String getClassName() {
		return className;
	}
	
	@Override
	public void Open() throws IOException {
	}

	@Override
	public void Close() throws IOException {		
		session.getKVStore().close();
	}
	
	@Override
	public TransactionDB getKVStore() {
		return session.getKVStore();
	}
	
	/**
	* Put a  key/value pair to main cache and pool.
	* @param tkey The key for the pair
	* @param tvalue The value for the pair
	* @return true if key previously existed and was not added
	* @exception IOException if put to backing store fails
	*/
	@SuppressWarnings("rawtypes")
	@Override
	public boolean put(TransactionId transactionId, Comparable tkey, Object tvalue) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.put(txn, columnFamilyHandle, tkey, tvalue);
	}
	/**
	* Put a  key/value pair to main cache and pool. 
	* @param tkey The key for the pair, will not be serialized
	* @param tvalue The value for the pair
	* @return true if key previously existed and was not added
	* @exception IOException if put to backing store fails
	*/
	@SuppressWarnings("rawtypes")
	public boolean putViaBytes(TransactionId transactionId, byte[] tkey, Object tvalue) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.putViaBytes(txn, columnFamilyHandle, tkey, tvalue);
	}
	/**
	* Get a value from backing store if not in cache.
	* @param tkey The key for the value
	* @return The value for the key
	* @exception IOException if get from backing store fails
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public Object get(TransactionId transactionId, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return getSession().get(txn, columnFamilyHandle, session.ro, tkey);
	}
	/**
	* Get a value from backing store if not in cache.
	* @param tkey The key for the value
	* @return The value for the key
	* @exception IOException if get from backing store fails
	*/
	public Object getViaBytes(TransactionId transactionId, byte[] tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return getSession().getViaBytes(txn, columnFamilyHandle, session.ro, tkey);
	}
	/**
	* Get a value from backing store if not in cache.
	* We may toss out one to make room if size surpasses objectCacheSize
	* @param tkey The key for the value
	* @return The {@link Entry} from RockSack iterator Entry derived from Map.Entry for the key
	* @exception IOException if get from backing store fails
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public Object getValue(TransactionId transactionId, Object tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.getValue(txn, columnFamilyHandle, session.ro, tkey);
	}	

	/**
	* Return the number of elements in the backing store
 	* @return A long value of number of elements
	* @exception IOException If backing store retrieval failure
	*/
	@Override
	public long size(TransactionId transactionId) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.size(txn, columnFamilyHandle);
	}

	/**
	* Obtain iterator over the entrySet. Retrieve from backing store if not in cache.
	* @return The Iterator for all elements
	* @exception IOException if get from backing store fails
	*/
	@Override
	public Iterator<?> entrySet(TransactionId transactionId) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.entrySet(txn, columnFamilyHandle);
	}
	
	@Override
	public Stream<?> entrySetStream(TransactionId transactionId) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.entrySetStream(txn, columnFamilyHandle);
	}
	
	/**
	* Get a keySet iterator. Get from backing store if not in cache.
	* @return The iterator for the keys
	* @exception IOException if get from backing store fails
	*/
	@Override
	public Iterator<?> keySet(TransactionId transactionId) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.keySet(txn, columnFamilyHandle);
	}
	
	@Override
	public Stream<?> keySetStream(TransactionId transactionId) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.keySetStream(txn, columnFamilyHandle);
	}
	
	/**
	* Returns true if the collection contains the given key
	* @param tkey The key to match
	* @return true if in, or false if absent
	* @exception IOException If backing store fails
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public boolean containsKey(TransactionId transactionId, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.contains(txn, columnFamilyHandle, session.ro, tkey);
	}
	/**
	* Returns true if the collection contains the given value object
	* @param value The value to match
	* @return true if in, false if absent
	* @exception IOException If backing store fails
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public boolean containsValue(TransactionId transactionId, Object value) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.containsValue(txn, columnFamilyHandle, session.ro, value);
	}
	/**
	* Remove object from cache and backing store.
	* @param tkey The key to match
	* @return The removed object
	* @exception IOException If backing store fails
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public Object remove(TransactionId transactionId, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.remove(txn, columnFamilyHandle, session.ro, tkey);
	}
	/**
	* @return First key in set
	* @exception IOException If backing store retrieval failure
	*/
			@Override
	public Comparable firstKey(TransactionId transactionId) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.firstKey(txn, columnFamilyHandle);
	}
	/**
	* @return Last key in set
	* @exception IOException If backing store retrieval failure
	*/
	@Override
	public Comparable lastKey(TransactionId transactionId) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.lastKey(txn, columnFamilyHandle);

	}
	/**
	* Return the last value in the set
	* @return The last element in the set
	* @exception IOException If backing store retrieval failure
	*/
	@Override
	public Object last(TransactionId transactionId) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.last(txn, columnFamilyHandle);
	}
	/**
	* Return the first element
	* @return The value of the Object of the first key
	* @exception IOException If backing store retrieval failure
	*/
	@Override
	public Object first(TransactionId transactionId) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.first(txn, columnFamilyHandle);
	}
	/**
	 * Return the element nearest to given key
	 * @param tkey the key to sear for
	 * @return the key/value of closest element to tkey
	 * @throws IOException
	 */
	public Object nearest(TransactionId transactionId, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.nearest(txn, columnFamilyHandle, tkey);
	}
	/**
	* @param tkey Strictly less than 'to' this element
	* @return Iterator of first to tkey
	* @exception IOException If backing store retrieval failure
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public Iterator<?> headMap(TransactionId transactionId, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.headSet(txn, columnFamilyHandle, tkey);
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public Stream<?> headMapStream(TransactionId transactionId, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.headSetStream(txn, columnFamilyHandle, tkey);
	}
	/**
	* @param tkey Strictly less than 'to' this element
	* @return Iterator of first to tkey returning KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public Iterator<?> headMapKV(TransactionId transactionId, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.headSetKV(txn, columnFamilyHandle, tkey);
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public Stream<?> headMapKVStream(TransactionId transactionId, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.headSetKVStream(txn, columnFamilyHandle, tkey);
	}
	/**
	* @param fkey Greater or equal to 'from' element
	* @return Iterator of objects from fkey to end
	* @exception IOException If backing store retrieval failure
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public Iterator<?> tailMap(TransactionId transactionId, Comparable fkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.tailSet(txn, columnFamilyHandle, fkey);
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public Stream<?> tailMapStream(TransactionId transactionId, Comparable fkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.tailSetStream(txn, columnFamilyHandle, fkey);
	}
	/**
	* @param fkey Greater or equal to 'from' element
	* @return Iterator of objects from fkey to end which are KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public Iterator<?> tailMapKV(TransactionId transactionId, Comparable fkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.tailSetKV(txn, columnFamilyHandle, fkey);
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public Stream<?> tailMapKVStream(TransactionId transactionId, Comparable fkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.tailSetKVStream(txn, columnFamilyHandle, fkey);
	}
	/**
	* @param fkey 'from' element inclusive 
	* @param tkey 'to' element exclusive
	* @return Iterator of objects in subset from fkey to tkey
	* @exception IOException If backing store retrieval failure
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public Iterator<?> subMap(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.subSet(txn, columnFamilyHandle, fkey, tkey);
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public Stream<?> subMapStream(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.subSetStream(txn, columnFamilyHandle, fkey, tkey);
	}
	
	/**
	* @param fkey 'from' element inclusive 
	* @param tkey 'to' element exclusive
	* @return Iterator of objects in subset from fkey to tkey composed of KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	@Override
	@SuppressWarnings("rawtypes")
	public Iterator<?> subMapKV(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.subSetKV(txn, columnFamilyHandle, fkey, tkey);
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public Stream<?> subMapKVStream(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.subSetKVStream(txn, columnFamilyHandle, fkey, tkey);
	}
	
	/**
	* Return boolean value indicating whether the map is empty
	* @return true if empty
	* @exception IOException If backing store retrieval failure
	*/
	@Override
	public boolean isEmpty(TransactionId transactionId) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.isEmpty(txn, columnFamilyHandle);
	}
	
	public void dropColumn() throws IOException {
		session.dropColumn(columnFamilyHandle);
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
	public Iterator<?> iterator(TransactionId transactionId) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.keySet(txn, columnFamilyHandle);
	}

	@Override
	public boolean contains(TransactionId transactionId, Comparable o) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.contains(txn, columnFamilyHandle, session.ro, o);
	}

	@Override
	public Iterator<?> subSet(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.subSet(txn, columnFamilyHandle, fkey, tkey);
	}

	@Override
	public Stream<?> subSetStream(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.subSetStream(txn, columnFamilyHandle, fkey, tkey);
	}

	@Override
	public Iterator<?> headSet(TransactionId transactionId, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.headSet(txn, columnFamilyHandle, tkey);
	}

	@Override
	public Stream<?> headSetStream(TransactionId transactionId, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.headSetStream(txn, columnFamilyHandle, tkey);
	}

	@Override
	public Iterator<?> tailSet(TransactionId transactionId, Comparable fkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.tailSet(txn, columnFamilyHandle, fkey);
	}

	@Override
	public Stream<?> tailSetStream(TransactionId transactionId, Comparable fkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.tailSetStream(txn, columnFamilyHandle, fkey);
	}

	@Override
	public Iterator<?> subSetKV(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.subSetKV(txn, columnFamilyHandle, fkey, tkey);
	}

	@Override
	public Stream<?> subSetKVStream(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.subSetKVStream(txn, columnFamilyHandle, fkey, tkey);
	}

	@Override
	public Iterator<?> headSetKV(TransactionId transactionId, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.headSetKV(txn, columnFamilyHandle, tkey);
	}

	@Override
	public Stream<?> headSetKVStream(TransactionId transactionId, Comparable tkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.headSetKVStream(txn, columnFamilyHandle, tkey);
	}

	@Override
	public Iterator<?> tailSetKV(TransactionId transactionId, Comparable fkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.tailSetKV(txn, columnFamilyHandle, fkey);
	}

	@Override
	public Stream<?> tailSetKVStream(TransactionId transactionId, Comparable fkey) throws IOException {
		Transaction txn = session.getTransaction(transactionId, className, false);
		if(txn == null)
			throw new IOException("Transaction "+transactionId+" not found.");
		return session.tailSetKVStream(txn, columnFamilyHandle, fkey);
	}


	@Override
	public String toString() {
		return (session == null ? "TransactionalMap Session NULL" : session.toString()+" ClassName:"+className+" ColumnFamily:"+columnFamilyDescriptor.toString());
	}

}
