package com.neocoretechs.rocksack.iterator;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
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
* Provides a persistent collection iterator 'from' element inclusive, 'to' element exclusive of {@link com.neocoretechs.rocksack.iterator.KeyValuePair} of Map.Entry
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2021,2022
*/
public class SubSetKVIterator extends SubSetIterator {
	private boolean DEBUG = false;
	Object nextElem, retElem;
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public SubSetKVIterator(Comparable fromKey, Comparable toKey, RocksDB db) throws IOException {
		super(fromKey, toKey, db);
		if(kvMain.isValid() && nextKey != null) {
			nextElem = SerializedComparator.deserializeObject(kvMain.value());
		}
	}
	
	public SubSetKVIterator(Comparable fromKey, Comparable toKey, Transaction db) throws IOException {
		super(fromKey, toKey, db);
		if(kvMain.isValid() && nextKey != null) {
			nextElem = SerializedComparator.deserializeObject(kvMain.value());
		}
	}
	
	public SubSetKVIterator(ColumnFamilyHandle cfh, Comparable fromKey, Comparable toKey, RocksDB db) throws IOException {
		super(cfh, fromKey, toKey, db);
		if(kvMain.isValid() && nextKey != null) {
			nextElem = SerializedComparator.deserializeObject(kvMain.value());
		}
	}
	
	public SubSetKVIterator(ColumnFamilyHandle cfh, Comparable fromKey, Comparable toKey, Transaction db) throws IOException {
		super(cfh, fromKey, toKey, db);
		if(kvMain.isValid() && nextKey != null) {
			nextElem = SerializedComparator.deserializeObject(kvMain.value());
		}
	}
	
	@SuppressWarnings("unchecked")
	public Object next() {
		try {
			if (!kvMain.isValid() || nextKey == null)
				throw new NoSuchElementException("No next iterator element");
			retKey = nextKey;
			retElem = nextElem;
			kvMain.next();
			if(kvMain.isValid()) {
				nextKey = (Comparable) SerializedComparator.deserializeObject(kvMain.key());
				nextElem = SerializedComparator.deserializeObject(kvMain.value());
				if (nextKey.compareTo(toKey) >= 0 || nextKey.compareTo(fromKey) < 0) {
					nextKey = null;
				}
			} else {
				nextKey = null;
			}
			return new KeyValuePair(retKey, retElem);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe.toString());
		}
	}

}
