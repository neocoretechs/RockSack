package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

import org.rocksdb.RocksDB;
import org.rocksdb.TransactionDB;

import com.neocoretechs.rocksack.TransactionId;


public interface TransactionSetInterface {

	String getDBName();

	Object getMutexObject();

	/**
	 * Cause the b seekKey for the Comparable type.
	 * @param o the Comparable object to seek.
	 * @return the value of object associated with the key, null if key was not found
	 * @throws IOException
	 */
	Object get(TransactionId transactionId, Comparable o) throws IOException;

	
	/**
	* Returns iterator
	* @return The Iterator over the entrySet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> iterator(TransactionId transactionId) throws IOException ;

	/**
	 * Contains a value object
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	boolean contains(TransactionId transactionId, Comparable o) throws IOException;
	
	/**
	* Remove the key and value of the parameter.
	* @return null or previous object
	*/
	Object remove(TransactionId transactionId, Comparable o) throws IOException;
	/**
	 * Get the value of the object associated with first key
	 * @return Object from first key
	 * @throws IOException
	 */
	Object first(TransactionId transactionId) throws IOException;

	/**
	 * Get the last object associated with greatest valued key in the KVStore
	 * @return The Object of the greatest key
	 * @throws IOException
	 */
	Object last(TransactionId transactionId) throws IOException;
	
	/**
	 * Get the number of keys total.
	 * @return The size of the KVStore.
	 * @throws IOException
	 */
	long size(TransactionId transactionId) throws IOException ;
	/**
	 * Is the KVStore empty?
	 * @return true if it is empty.
	 * @throws IOException
	 */
	boolean isEmpty(TransactionId transactionId) throws IOException;


	/**
	 * Open the files associated with the BTree for the instances of class
	 * @throws IOException
	 */
	void Open() throws IOException;
	

	TransactionDB getKVStore();

	void Close() throws IOException;
	
}