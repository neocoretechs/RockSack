package com.neocoretechs.rocksack.stream;

import java.io.IOException;

import org.rocksdb.RocksDB;

import com.neocoretechs.rocksack.iterator.SubSetIterator;

/**
 * Java 8 stream extensions for BigSack delivery of ordered persistent datasets.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class SubSetStream extends SackStream {

	public SubSetStream(SubSetIterator esi) {
		super(esi);
	}
	
	public SubSetStream(Comparable fkey, Comparable tkey, RocksDB kvMain) throws IOException {
		this(new SubSetIterator(fkey, tkey, kvMain));
	}
}
