package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

import com.neocoretechs.rocksack.TransactionId;

/**
 * Interface contract for the ordered Key/Value Map abstraction in a transaction context.<p>
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2025
 *
 */
interface TransactionOrderedKVMapInterface extends TransactionMapInterface, TransactionOrderedKVSetInterface {
	
	/**
	* Not a real subset, returns iterator vs set.
	* 'from' element inclusive, 'to' element exclusive
	* @param fkey Return from fkey
	* @param tkey Return from fkey to strictly less than tkey
	* @return The KeyValuePair Iterator over the subSet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> subSetKV(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException;
	
	/**
	 * Return a Streamof key/value pairs that delivers the subset of fkey to tkey
	 * @param fkey the from key
	 * @param tkey the to key
	 * @return the stream from which the lambda expression can be utilized
	 * @throws IOException
	 */
	Stream<?> subSetKVStream(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException;
	
	
	/**
	* Not a real subset, returns Iterator
	* @param tkey return from head to strictly less than tkey
	* @return The KeyValuePair Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> headSetKV(TransactionId transactionId, Comparable tkey) throws IOException;
	
	/**
	 * Get a stream of head set
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	Stream<?> headSetKVStream(TransactionId transactionId, Comparable tkey) throws IOException;
	
	
	/**
	* Not a real subset, returns Iterator
	* @param fkey return from value to end
	* @return The KeyValuePair Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> tailSetKV(TransactionId transactionId, Comparable fkey) throws IOException;
	
	/**
	 * Return a tail set key/value stream
	 * @param fkey from key of tailset
	 * @return the stream from which the lambda can be utilized
	 * @throws IOException
	 */
	Stream<?> tailSetKVStream(TransactionId transactionId, Comparable fkey) throws IOException;

	/**
	* @param tkey Strictly less than 'to' this element
	* @return Iterator of first to tkey
	* @exception IOException If backing store retrieval failure
	*/
	Iterator<?> headMap(TransactionId transactionId, Comparable tkey) throws IOException;

	Stream<?> headMapStream(TransactionId transactionId, Comparable tkey) throws IOException;

	/**
	* @param tkey Strictly less than 'to' this element
	* @return Iterator of first to tkey returning KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	Iterator<?> headMapKV(TransactionId transactionId, Comparable tkey) throws IOException;

	Stream<?> headMapKVStream(TransactionId transactionId, Comparable tkey) throws IOException;

	/**
	* @param fkey Greater or equal to 'from' element
	* @return Iterator of objects from fkey to end
	* @exception IOException If backing store retrieval failure
	*/
	Iterator<?> tailMap(TransactionId transactionId, Comparable fkey) throws IOException;

	Stream<?> tailMapStream(TransactionId transactionId, Comparable fkey) throws IOException;

	/**
	* @param fkey Greater or equal to 'from' element
	* @return Iterator of objects from fkey to end which are KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	Iterator<?> tailMapKV(TransactionId transactionId, Comparable fkey) throws IOException;

	Stream<?> tailMapKVStream(TransactionId transactionId, Comparable fkey) throws IOException;

	/**
	* @param fkey 'from' element inclusive 
	* @param tkey 'to' element exclusive
	* @return Iterator of objects in subset from fkey to tkey
	* @exception IOException If backing store retrieval failure
	*/
	Iterator<?> subMap(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException;

	Stream<?> subMapStream(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException;

	/**
	* @param fkey 'from' element inclusive 
	* @param tkey 'to' element exclusive
	* @return Iterator of objects in subset from fkey to tkey composed of KeyValuePairs
	* @exception IOException If backing store retrieval failure
	*/
	Iterator<?> subMapKV(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException;

	Stream<?> subMapKVStream(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException;
	

	
}