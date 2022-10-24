package com.neocoretechs.rocksack.iterator;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.Stack;

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
* Provides a persistent collection iterator greater or equal to 'from' element
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2021,2022
*/
public class TailSetIterator extends AbstractIterator {
	@SuppressWarnings("rawtypes")
	private static boolean DEBUG = false;
	Comparable fromKey;
	public TailSetIterator(Comparable fromKey, RocksDB db) throws IOException {
		super(db.newIterator());//new ReadOptions().setIterateLowerBound(new Slice(SerializedComparator.serializeObject(fromKey)))));
	    //kvMain.seek(SerializedComparator.serializeObject(fromKey));
		//if(kvMain.isValid()) {
		//	nextKey = (Comparable) SerializedComparator.deserializeObject(kvMain.key());
		//}
		while(kvMain.isValid()) {
			// set our lower bound
			nextKey = (Comparable) SerializedComparator.deserializeObject(kvMain.key());
			if(nextKey.compareTo(fromKey) >= 0)
				break;
			kvMain.next();
		}
		this.fromKey = fromKey;
	}
	public TailSetIterator(Comparable fromKey, Transaction db) throws IOException {
		super(db.getIterator(new ReadOptions()));//.setIterateLowerBound(new Slice(SerializedComparator.serializeObject(fromKey)))));
		while(kvMain.isValid()) {
			// set our lower bound
			nextKey = (Comparable) SerializedComparator.deserializeObject(kvMain.key());
			if(nextKey.compareTo(fromKey) >= 0)
				break;
			kvMain.next();
		}
		this.fromKey = fromKey;
	}
	public boolean hasNext() {
		return kvMain.isValid();
	}
	public Object next() {
			try {
				// move nextelem to retelem, search nextelem, get nextelem
				if (!kvMain.isValid())
					throw new NoSuchElementException("No next iterator element");
				retKey = nextKey;
				kvMain.next();
				if(kvMain.isValid()) {
					nextKey = (Comparable) SerializedComparator.deserializeObject(kvMain.key());
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
