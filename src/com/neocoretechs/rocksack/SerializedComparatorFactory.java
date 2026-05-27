package com.neocoretechs.rocksack;
/**
 * Class to generate new instances of SerializedComparator using the given ClassLoader.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2026
 */
public class SerializedComparatorFactory {
	private static ClassLoader loader;
	static {
		loader = ClassLoader.getSystemClassLoader();
	}
	public static void setClassLoader(ClassLoader cl) {
		loader = cl;
	}
	public static SerializedComparator newComparator() {
		return new SerializedComparator(loader);
	}
}
