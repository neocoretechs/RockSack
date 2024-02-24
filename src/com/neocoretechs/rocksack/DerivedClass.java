package com.neocoretechs.rocksack;

public interface DerivedClass {
	default Class<?> superclass() { return this.getClass().getSuperclass(); }
}
