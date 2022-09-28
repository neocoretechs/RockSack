package com.neocoretechs.rocksack.iterator;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Slice;

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
* Provides a persistent collection iterator 'from' element inclusive, 'to' element exclusive
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
*/
public class SubSetIterator extends AbstractIterator {
	private static boolean DEBUG = false;
	@SuppressWarnings("rawtypes")
	Comparable fromKey, toKey;
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public SubSetIterator(Comparable fromKey, Comparable toKey, RocksDB db) throws IOException {
		super(db.newIterator(new ReadOptions().
				setIterateUpperBound(new Slice(SerializedComparator.serializeObject(toKey))).
				setIterateLowerBound(new Slice(SerializedComparator.serializeObject(fromKey)))));
		this.fromKey = fromKey;
		this.toKey = toKey;
	}
	public boolean hasNext() {
		return kvMain.isValid();
	}

	/**
	 * Standard Iterator next method. To make it resistant to deletes and insertions, we re-traverse
	 * the key on each iteration. A somewhat inefficient yet failsafe approach.
	 */
	@SuppressWarnings("unchecked")
	public Object next() {
		try {
			if (!kvMain.isValid())
				throw new NoSuchElementException("No next iterator element");
			retKey = nextKey;
			kvMain.next();
			if(kvMain.isValid()) {
				nextKey = (Comparable) SerializedComparator.deserializeObject(kvMain.key());
			} else {
				nextKey = null;
			}
			// move nextelem to retelem, search nextelem, get nextelem
			synchronized (kvMain) {
				if (nextKey == null || nextKey.compareTo(toKey) >= 0 || nextKey.compareTo(fromKey) < 0) {
						if( DEBUG ) {
							if( nextKey != null)
								System.out.println("SubSetIterator init nextKey non-null toKey:"+nextKey.compareTo(toKey)+" fromKey:"+nextKey.compareTo(fromKey));
							else
								System.out.println("SubSetIterator init nextKey null toKey:"+toKey+" fromKey:"+fromKey);
						}
						nextKey = null; //exclusive
						System.out.println("WARNING: NON exclusive subset iterator");
				}
				return retKey;
			}
		} catch (IOException ioe) {
			throw new RuntimeException(ioe.toString());
		}
	}

	public void remove() {
		throw new UnsupportedOperationException("No provision to remove from Iterator");
	}
}
