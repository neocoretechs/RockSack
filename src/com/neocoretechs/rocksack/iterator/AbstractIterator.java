package com.neocoretechs.rocksack.iterator;

import java.io.IOException;
import java.util.Iterator;

import org.rocksdb.RocksIterator;

import com.neocoretechs.rocksack.SerializedComparator;

/**
 * Provides the superclass for out iterators and drop-in compatibility for java.util.Iterator<> contracts
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public abstract class AbstractIterator implements Iterator<Object> {
	RocksIterator kvMain;
	Comparable nextKey, retKey;
	public AbstractIterator(RocksIterator kvMain) throws IOException {
		this.kvMain = kvMain;
	    kvMain.seekToFirst();
		if(kvMain.isValid()) {
			nextKey = (Comparable) SerializedComparator.deserializeObject(kvMain.key());
		}
	}
	public abstract boolean hasNext();
	public abstract Object next();
}
