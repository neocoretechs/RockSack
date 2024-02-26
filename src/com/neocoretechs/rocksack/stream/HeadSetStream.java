package com.neocoretechs.rocksack.stream;

import java.io.IOException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.Transaction;

import com.neocoretechs.rocksack.iterator.HeadSetIterator;

/**
 * Java 8 stream extensions for RockSack delivery of ordered persistent datasets.
 * for items of persistent collection strictly less than 'to' element
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class HeadSetStream extends SackStream {

	public HeadSetStream(HeadSetIterator esi) {
		super(esi);
	}

	public HeadSetStream(Comparable tkey, RocksDB kvMain) throws IOException {
		this(new HeadSetIterator(tkey, kvMain));
	}
	
	public HeadSetStream(Comparable tkey, Transaction kvMain) throws IOException {
		this(new HeadSetIterator(tkey, kvMain));
	}

	public HeadSetStream(Comparable tkey, RocksDB kvMain, ColumnFamilyHandle cfh) throws IOException {
		this(new HeadSetIterator(tkey, kvMain, cfh));
	}
	
	public HeadSetStream(Comparable tkey, Transaction kvMain, ColumnFamilyHandle cfh) throws IOException {
		this(new HeadSetIterator(tkey, kvMain, cfh));
	}

}
