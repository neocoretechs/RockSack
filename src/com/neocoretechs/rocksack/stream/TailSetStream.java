package com.neocoretechs.rocksack.stream;

import java.io.IOException;

import org.rocksdb.RocksDB;

import com.neocoretechs.rocksack.iterator.TailSetIterator;

/**
 * Java 8 stream extensions for RockSack delivery of ordered persistent datasets.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class TailSetStream extends SackStream {

	public TailSetStream(TailSetIterator esi) {
		super(esi);
	}

	public TailSetStream(Comparable fkey, RocksDB kvMain) throws IOException {
		this(new TailSetIterator(fkey, kvMain));
	}


}
