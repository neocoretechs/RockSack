package com.neocoretechs.rocksack.stream;

import java.io.IOException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.Transaction;

import com.neocoretechs.rocksack.iterator.HeadSetKVIterator;
/**
 * Java 8 stream extensions for RockSack delivery of ordered persistent datasets.
 * for items of persistent collection strictly less than 'to' element
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
	
	public HeadSetKVStream(Comparable tkey, Transaction kvMain) throws IOException {
		this(new HeadSetKVIterator(tkey, kvMain));
	}

	public HeadSetKVStream(Comparable tkey, RocksDB kvMain, ColumnFamilyHandle cfh) throws IOException {
		this(new HeadSetKVIterator(tkey, kvMain, cfh));
	}
	
	public HeadSetKVStream(Comparable tkey, Transaction kvMain, ColumnFamilyHandle cfh) throws IOException {
		this(new HeadSetKVIterator(tkey, kvMain, cfh));
	}

}
