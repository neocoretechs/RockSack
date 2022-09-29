package com.neocoretechs.rocksack.stream;

import java.io.IOException;

import org.rocksdb.RocksDB;

import com.neocoretechs.rocksack.iterator.SubSetKVIterator;

/**
 * Java 8 stream extensions for RockSack delivery of ordered persistent datasets.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class SubSetKVStream extends SackStream {

	public SubSetKVStream(SubSetKVIterator esi) {
		super(esi);
	}

	public SubSetKVStream(Comparable fkey, Comparable tkey, RocksDB kvMain) throws IOException {
		this(new SubSetKVIterator(fkey, tkey, kvMain));
	}

}
