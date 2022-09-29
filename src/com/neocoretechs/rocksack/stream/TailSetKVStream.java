package com.neocoretechs.rocksack.stream;

import java.io.IOException;

import org.rocksdb.RocksDB;

import com.neocoretechs.rocksack.iterator.TailSetKVIterator;

/**
 * Java 8 stream extensions for RockSack delivery of ordered persistent datasets.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class TailSetKVStream extends SackStream {

	public TailSetKVStream(TailSetKVIterator esi) {
		super(esi);
	}

	public TailSetKVStream(Comparable fkey, RocksDB kvMain) throws IOException {
		this(new TailSetKVIterator(fkey, kvMain));
	}


}
