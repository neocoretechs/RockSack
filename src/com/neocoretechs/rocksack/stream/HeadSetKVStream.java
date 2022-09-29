package com.neocoretechs.rocksack.stream;

import java.io.IOException;

import org.rocksdb.RocksDB;

import com.neocoretechs.rocksack.iterator.HeadSetKVIterator;
/**
 * Java 8 stream extensions for RockSack delivery of ordered persistent datasets.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class HeadSetKVStream extends SackStream {

	public HeadSetKVStream(HeadSetKVIterator esi) {
		super(esi);
	}

	public HeadSetKVStream(Comparable tkey, RocksDB kvMain) throws IOException {
		this(new HeadSetKVIterator(tkey, kvMain));
	}


}
