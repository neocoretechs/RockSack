package com.neocoretechs.rocksack.iterator;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Slice;
import org.rocksdb.Transaction;

import com.neocoretechs.rocksack.SerializedComparator;
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
* Provides a persistent collection iterator of keys 'from' element inclusive, 'to' element exclusive of the keys specified
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
*/
public class SubSetIterator extends AbstractIterator {
	private static boolean DEBUG = false;
	@SuppressWarnings("rawtypes")
	Comparable fromKey, toKey;
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public SubSetIterator(Comparable fromKey, Comparable toKey, RocksDB db) throws IOException {
		super(db.newIterator(), fromKey);//new ReadOptions()));//.
				//setIterateUpperBound(new Slice(SerializedComparator.serializeObject(toKey)))));//.
				//setIterateLowerBound(new Slice(SerializedComparator.serializeObject(fromKey)))));
		//
		while(kvMain.isValid()) {
			// set our lower bound
			nextKey = (Comparable) SerializedComparator.deserializeObject(kvMain.key());
			if(nextKey.compareTo(fromKey) >= 0)
				break;
			if(nextKey.compareTo(toKey) >= 0)  {
				nextKey = null;
				break;
			}
			kvMain.next();
		}
		if(DEBUG) {
			System.out.printf("%s fromKey=%s toKey=%s%n", this.getClass().getName(),fromKey,toKey);
			System.out.printf("%s start key=%s value=%s%n", this.getClass().getName(),nextKey,SerializedComparator.deserializeObject(kvMain.value()));
		}
		this.fromKey = fromKey;
		this.toKey = toKey;
	}
	public SubSetIterator(Comparable fromKey, Comparable toKey, Transaction db) throws IOException {
		super(db.getIterator(new ReadOptions()));//.
				//setIterateUpperBound(new Slice(SerializedComparator.serializeObject(toKey)))));//.
				//setIterateLowerBound(new Slice(SerializedComparator.serializeObject(fromKey)))));
		while(kvMain.isValid()) {
			// set our lower bound
			nextKey = (Comparable) SerializedComparator.deserializeObject(kvMain.key());
			if(nextKey.compareTo(fromKey) >= 0)
				break;
			if (nextKey.compareTo(toKey) >= 0)  {
				nextKey = null;
				break;
			}
			kvMain.next();
		}
		if(DEBUG) {
			System.out.printf("%s fromKey=%s toKey=%s%n", this.getClass().getName(),fromKey,toKey);
			System.out.printf("%s start key=%s value=%s%n", this.getClass().getName(),nextKey,SerializedComparator.deserializeObject(kvMain.value()));
		}
		this.fromKey = fromKey;
		this.toKey = toKey;
	}
	public boolean hasNext() {
		return nextKey != null && kvMain.isValid();
	}

	/**
	 * Standard Iterator next method. To make it resistant to deletes and insertions, we re-traverse
	 * the key on each iteration. A somewhat inefficient yet failsafe approach.
	 */
	@SuppressWarnings("unchecked")
	public Object next() {
		try {
			if (!kvMain.isValid() || nextKey == null)
				throw new NoSuchElementException("No next iterator element");
			retKey = nextKey;
			kvMain.next();
			if(kvMain.isValid()) {
				nextKey = (Comparable) SerializedComparator.deserializeObject(kvMain.key());
				if (nextKey.compareTo(toKey) >= 0 || nextKey.compareTo(fromKey) < 0) {
					nextKey = null;
				}
			} else {
				nextKey = null;
			}
			return retKey;
		} catch (IOException ioe) {
			throw new RuntimeException(ioe.toString());
		}
	}

	public void remove() {
		throw new UnsupportedOperationException("No provision to remove from Iterator");
	}
}
