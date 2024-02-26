package com.neocoretechs.rocksack.stream;

import java.io.IOException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Transaction;

import com.neocoretechs.rocksack.iterator.EntrySetIterator;

/**
 * Java 8 stream extensions for RockSack delivery of ordered persistent datasets.
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
	public EntrySetStream(Transaction kvMain) throws IOException {
		this(new EntrySetIterator(kvMain, new ReadOptions()));
	}

	public EntrySetStream(RocksDB kvMain, ColumnFamilyHandle cfh) throws IOException {
		this(new EntrySetIterator(kvMain, cfh));
	}

	public EntrySetStream(Transaction kvMain, ColumnFamilyHandle cfh) throws IOException {
		this(new EntrySetIterator(kvMain, new ReadOptions(), cfh));
	}

}
