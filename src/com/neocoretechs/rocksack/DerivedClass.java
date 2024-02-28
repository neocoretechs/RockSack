package com.neocoretechs.rocksack;

public interface DerivedClass<T> {
	default Class<?> superclass() { return this.getClass().getSuperclass(); }
}
