package com.neocoretechs.rocksack.test;

import com.neocoretechs.rocksack.CompareAndSerialize;
import com.neocoretechs.rocksack.ComparisonOrderField;
import com.neocoretechs.rocksack.ComparisonOrderMethod;
/**
* Basic annotation tooling for RockSack to generate the necessary fields and methods for
* storage and retrieval under the java.lang.Comparable interface as used throughout the language.
* The ordering of the keys is defined here as by the annotation order field: j,i, and l. We
* demonstrate method and field access and generate compareTo method and Serializable interface
* implementation with SerialUID. We also show how to wrap a custom object to give Comparable
* functionality to any class. No modifications will affect the operation of the original class.
* The original class will be backed up as TestTooling1.bak before modification.
*/
@CompareAndSerialize
public class TestTooling1{
	@ComparisonOrderField(order=2)
	private int i;
	@ComparisonOrderField(order=1)
	private String j;
	@ComparisonOrderField(order=3)
	ByteObject l;
	@ComparisonOrderMethod
	public ByteObject getL() {
		return l;
	}
	static class ByteObject implements Comparable {
		byte[] bytes = new byte[] {10,9,8,7,6,5,4,3,2,1};
		@Override
		public int compareTo(Object o) {
			ByteObject b = (ByteObject)o;
			for(int i = 0; i < b.bytes.length; i++) {
				if(bytes[i] > b.bytes[i])
					return 1;
				if(bytes[i] < b.bytes[i])
					return -1;
			}
			return 0;
		}
		
	}
}
