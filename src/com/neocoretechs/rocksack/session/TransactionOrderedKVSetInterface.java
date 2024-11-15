package com.neocoretechs.rocksack.session;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

import com.neocoretechs.rocksack.TransactionId;


interface TransactionOrderedKVSetInterface extends TransactionSetInterface {

	/**
	* Not a real subset, returns iterator vs set.
	* 'from' element inclusive, 'to' element exclusive
	* @param fkey Return from fkey
	* @param tkey Return from fkey to strictly less than tkey
	* @return The Iterator over the subSet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> subSet(TransactionId transactionId, Comparable fkey, Comparable tkey) throws IOException;
		
	/**
	 * Return a Stream that delivers the subset of fkey to tkey
	 * @param fkey the from key
	 * @param tkey the to key
	 * @return the stream from which the lambda expression can be utilized
	 * @throws IOException
	 */
	Stream<?> subSetStream(TransactionId transactionId, Comparable fkey, Comparable tkey)throws IOException;
	
	/**
	* Not a real subset, returns Iterator
	* @param tkey return from head to strictly less than tkey
	* @return The Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> headSet(TransactionId transactionId, Comparable tkey) throws IOException ;
	/**
	 * Get a stream of headset
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	Stream<?> headSetStream(TransactionId transactionId, Comparable tkey) throws IOException;

	/**
	* Not a real subset, returns Iterator
	* @param fkey return from value to end
	* @return The Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> tailSet(TransactionId transactionId, Comparable fkey) throws IOException;
	/**
	 * Return a tail set stream
	 * @param fkey
	 * @return
	 * @throws IOException
	 */
	Stream<?> tailSetStream(TransactionId transactionId, Comparable fkey) throws IOException ;
	
	
}