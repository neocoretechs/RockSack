package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

import com.neocoretechs.rocksack.TransactionId;


interface TransactionMapInterface extends TransactionSetInterface {


	boolean put(TransactionId transactionId, Comparable key, Object o) throws IOException;

	/**
	 * Retrieve an object with this value for first key found to have it.
	 * @param o the object value to seek
	 * @return the object, null if not found
	 * @throws IOException
	 */
	Object getValue(TransactionId transactionId, Object o) throws IOException;
	
	/**
	* Not a real subset, returns iterator
	* @return The Iterator over the entrySet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> entrySet(TransactionId transactionId) throws IOException;
	
	/**
	 * Get a stream of entry set
	 * @return
	 * @throws IOException
	 */
	Stream<?> entrySetStream(TransactionId transactionId) throws IOException;
	
	/**
	* Return the keyset Iterator over all elements
	* @return The Iterator over the keySet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> keySet(TransactionId transactionId) throws IOException;
	
	/**
	 * Get a keyset stream
	 * @return
	 * @throws IOException
	 */
	Stream<?> keySetStream(TransactionId transactionId) throws IOException;
		
	/**
	 * Contains a value object
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	boolean containsValue(TransactionId transactionId, Object o) throws IOException;
	
	/**
	 * Get the first key
	 * @return The Comparable first key in the KVStore
	 * @throws IOException
	 */
	Comparable firstKey(TransactionId transactionId) throws IOException;

	/**
	 * Get the last key in the KVStore
	 * @return The last, greatest valued key in the KVStore.
	 * @throws IOException
	 */
	Comparable lastKey(TransactionId transactionId) throws IOException;

	/**
	* Returns true if the collection contains the given key
	* @param tkey The key to match
	* @return true if in, or false if absent
	* @exception IOException If backing store fails
	*/
	boolean containsKey(TransactionId transactionId, Comparable tkey) throws IOException;

	
}