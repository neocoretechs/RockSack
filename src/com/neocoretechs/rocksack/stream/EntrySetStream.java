package com.neocoretechs.rocksack.stream;

import java.io.IOException;

import org.rocksdb.RocksDB;

import com.neocoretechs.rocksack.iterator.EntrySetIterator;

/**
 * Java 8 stream extensions for BigSack delivery of ordered persistent datasets.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class EntrySetStream extends SackStream {

	public EntrySetStream(EntrySetIterator esi) {
		super(esi);
	}

	public EntrySetStream(RocksDB kvMain) throws IOException {
		this(new EntrySetIterator(kvMain));
	}

}
