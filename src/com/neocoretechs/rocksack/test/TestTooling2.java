package com.neocoretechs.rocksack.test;

import com.neocoretechs.rocksack.CompareAndSerialize;
import com.neocoretechs.rocksack.ComparisonOrderField;
import com.neocoretechs.rocksack.ComparisonOrderMethod;
/**
 * Basic annotation tooling for RockSack to generate the necessary fields and methods for
 * storage and retrieval under the java.lang.Comparable interface as used throughout the language.
 * The ordering of the keys is defined here as the order in which they appear: i,j, and l. We
 * demonstrate method and field access and generate compareTo method and Serializable interface
 * implementation with SerialUID. No modifications will affect the operation of the original class.
 * The original class will be backed up as TestTooling2.bak before modification.
 * {@link CompareAndSerialize} annotation to designate the class as toolable. The {@link ComparisonOrderField} and
 * {@link ComparisonOrderMethod}. {@link com.neocoretechs.rocksack.ClassTool}
 */
@CompareAndSerialize
public class TestTooling2{
	@ComparisonOrderField
	private int i;
	@ComparisonOrderField
	private String j;
	@ComparisonOrderField
	private String l;
	@ComparisonOrderMethod
	public String getL() {
		return l;
	}
}
