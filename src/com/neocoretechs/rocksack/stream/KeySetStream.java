package com.neocoretechs.rocksack.stream;

import java.io.IOException;

import org.rocksdb.RocksDB;

import com.neocoretechs.rocksack.iterator.KeySetIterator;

/**
 * Java 8 stream extensions for RockSack delivery of ordered persistent datasets.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class KeySetStream extends SackStream {

	public KeySetStream(KeySetIterator esi) {
		super(esi);
	}

	public KeySetStream(RocksDB kvMain) throws IOException {
		this(new KeySetIterator(kvMain));
	}


}
