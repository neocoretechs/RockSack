package com.neocoretechs.rocksack;

import java.io.IOException;
import java.util.Map;

/**
 * Class representing a key/value pair.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 */
public class KeyValue<K extends Comparable, V> implements Map.Entry{
	private K mKey;
    private V mValue;
    
    /**
     * Constructor that creates a blank key to prepare to receive pointers
     * to facilitate retrieval.
     */
    public KeyValue() {
    }

    public KeyValue(K key, V value) {
        mKey = key;
        mValue = value;
    }
    
    public K getmKey() throws IOException {
		return mKey;
	}

	public void setmKey(K mKey) {
		this.mKey = mKey;
	}

	public V getmValue() throws IOException {
		return mValue;
	}

	public void setmValue(V mValue) {
		this.mValue = mValue;
	}


    @Override
    public String toString() {
    	return String.format("Key=%s%n Value=%s%n", (mKey == null ? "null" : mKey), (mValue == null ? "null" : mValue));
    }

	@Override
	public Object getKey() {
		try {
			return getmKey();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object getValue() {
		try {
			return getmValue();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object setValue(Object value) {
		Object o;
		try {
			o = getmValue();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		setmValue((V) value);
		return o;
	}
}
