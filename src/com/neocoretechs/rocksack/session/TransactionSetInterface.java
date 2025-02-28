package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.Iterator;

import org.rocksdb.TransactionDB;

import com.neocoretechs.rocksack.TransactionId;
/**
 * Interface contract for the Set abstraction in a transaction context.<p>
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2025
 *
 */
public interface TransactionSetInterface {
	/**
	 * @return the name of the associated database
	 */
	String getDBName();

	Object getMutexObject();

	/**
	 * Cause the b seekKey for the Comparable type.
	 * @param transactionId the transaction Id
	 * @param o the Comparable object to seek.
	 * @return the value of object associated with the key, null if key was not found
	 * @throws IOException
	 */
	Object get(TransactionId transactionId, Comparable o) throws IOException;
	
	/**
	 * Read a key and make the key value a precondition for commit. Used for optimistic concurrency control
	 * @param transactionId Transaction Id
	 * @param o key to get
	 * @param exclusive true to get exclusive access
	 * @return the key/value object retrieved
	 * @throws IOException
	 */
	Object getForUpdate(TransactionId transactionId, Comparable o, boolean exclusive) throws IOException;
	
	/**
	 * Inform the transaction that it no longer needs to do any conflict checking for this key.
	 * @param transactionId Transaction Id
	 * @param o key to undo
	 * @throws IOException
	 */
	void undoGetForUpdate(TransactionId transactionId, Comparable o) throws IOException;
	
	/**
	* Returns iterator
	* @param transactionId Transaction Id
	* @return The Iterator over the entrySet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> iterator(TransactionId transactionId) throws IOException ;

	/**
	 * Contains a value object
	 * @param transactionId Transaction Id
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	boolean contains(TransactionId transactionId, Comparable o) throws IOException;
	
	/**
	* Remove the key and value of the parameter.
	* @param transactionId Transaction Id
	* @param o The object to remove
	* @return null or previous object
	*/
	Object remove(TransactionId transactionId, Comparable o) throws IOException;
	
	/**
	 * Get the value of the object associated with first key
	 * @param transactionId Transaction Id
	 * @return Object from first key
	 * @throws IOException
	 */
	Object first(TransactionId transactionId) throws IOException;

	/**
	 * Get the last object associated with greatest valued key in the KVStore
	 * @param transactionId Transaction Id
	 * @return The Object of the greatest key
	 * @throws IOException
	 */
	Object last(TransactionId transactionId) throws IOException;
	
	/**
	 * Get the number of keys total.
	 * @param transactionId Transaction Id
	 * @return The size of the KVStore.
	 * @throws IOException
	 */
	long size(TransactionId transactionId) throws IOException;
	
	/**
	 * Is the KVStore empty?
	 * @param transactionId Transaction Id
	 * @return true if it is empty.
	 * @throws IOException
	 */
	boolean isEmpty(TransactionId transactionId) throws IOException;

	/**
	 * Open the files associated with the BTree for the instances of class
	 * @throws IOException
	 */
	void Open() throws IOException;

	
	/**
	 * Close the seesion
	 * @throws IOException
	 */
	void Close() throws IOException;
	
	/**
	 * @return The session instance
	 * @throws IOException
	 */
	Session getSession() throws IOException;
	
}