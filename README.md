<h1>RockSack</h1>
<dd>
<li/> Makes it easier to use Facebook/Meta key/value database RocksDB/RocksJava in Java applications.
<li/> Allows Java objects to control database behavior.
<li/> Expands RocksDB low level byte-oriented API to use native Java object serialization.
<li/> Build Java database applications more quickly.
<li/> Gives RocksDB the power of a true object oriented database.
<li/> Makes it easier to manage transactions and organize databases by Java class.
<li/> Supports transaction and transactionless models.
<li/> Provides tooling to rapidly adapt existing classes to use Java serialization and inherent language Comparable compatability!
</dd>
<h2>RocksDB</h2>
https://github.com/facebook/rocksdb <br/>
http://rocksdb.org/					<br/>
https://github.com/facebook/rocksdb/tree/main/java/src/main/java/org/rocksdb <br/>
<h3>Why RockSack?</h3>
A bag or sack is a computer science term for a structure to hold a large amount of data that is usually unstructured.
RockSack can store Java objects so that they can be efficiently indexed, preserved, and retrieved in a manner that mirrors the java.util.Hashmap, java.util.TreeMap and java.util.TreeSet classes while providing the benefits of a full blown database.
The amount of data the system can maintain far exceeds resident and even virtual memory.
The implementation uses the RocksDB key/value store for performance and durability. The RocksDB K/V store is a Meta project
used by numerous high volume online orgs and has lots of support. What it lacks is a first class Java object method indexing to
create a 'functional' Java database.
So RockSack provides a way of using serialized first class Java objects as indexes for RocksDB.
<p/>
The assumption is that classes are obviously serializable, and for indexing implement the java.lang.Comparable.compareTo method, which most do by default, including String.
So in addition to RockSack having the means to store a large number of objects, it adds the properties of recoverability,
isolation, durability, atomicity, and concurrency.
<h3>Technical Details:</h3>
RockSack is a Java persistence mechanism that provides key/value store functionality 
with a small footprint and native object storage capability. Just about any Java object, meaning Serializable objects implementing the 
java.lang.Comparable interface, can be stored. The Comparable interface 
is part of the standard Java Collections Framework and is implemented in the majority of built-in Java classes such as String.<p/>
Whats the advantage? Whereas an agnostic K/V store only allows you to index the raw byte values of the key, a 'functional' index
gives you the ability to index the data based on an arbitrarily complex arrangement of any of the fields in the class, or even through
a strictly computational process since we are using the result of a method call to control the order in which the data are stored.

There are methods in the class com.neocoretechs.rocksack.DatabaseManager to organize the maps and sets on the basis of type. In this way a rudimentary schema can be maintained. A non-transactional BufferedMap can be obtained by the following methods:

```

DatabaseManager.setTableSpaceDir(argv[0]);
BufferedMap map = DatabaseManager.getMap(key.getClass());
map.put(key, value);
Comparable c = map.get(key);


```

If a transaction context is desired, in other words one in which multiple operations can be committed or rolled back under the control of the application, the following methods can be used:

```

DatabaseManager.setTableSpaceDir(argv[0]);
String xid = DatabaseManager.getTransactionId();
TransactionalMap map = DatabaseManager.getTransactionalMap(key.getClass(), xid);
map.put(xid, key, value);
Comparable c = map.get(xid, key);

DatabaseManager.commitTransaction(xid); // Or
DatabaseManager.rollbackTransaction(xid); // Or
DatabaseManager.checkpointTransaction(xid); // establish intermediate checkpoint that can be rolled back to
DatabaseManager.rollbackToCheckpoint(xid);
DatabaseManager.clearAllOutstandingTransactions(); // roll back and close all outstanding transactions on all open databases

Various reporting functions:
List<String> s = DatabaseManager.getOutstandingTransactionState(); // get the status of all outstanding transactions
List<Transaction> t = DatabaseManager.getOutstandingTransactions(database);
List<Transaction> t = DatabaseManager.getOutstandingTransactionsById(xid); // gets all transactions under given transaction Id
etc..

```

So if you were to store instances of a class named 'com.you.code' you would see a directory similar to "TestDBcom.you.code".
The typing is not strongly enforced, any key type can be inserted, but a means to manage types is provided that prevents exceptions being thrown in the 'compareTo' method.

In addition to the 'get','put','remove','contains','size','keySet','entrySet','contains','containsKey','first','last','firstKey','lastKey' the full set of
iterators can be obtained to retrieve subsets of the data for sets and maps:<br>
Sets:<br/>
headSet<br/>
tailSet<br/>
subSet<br/>
Maps:<br/>
headMap<br/>
headMapKV (key and value)<br/>
tailMap<br/>
tailMapKV<br/>
subMap<br/>
subMapKV<br/>
```
		String xid = DatabaseManager.getTransactionId();
		TransactionalMap map = DatabaseManager.getTransactionalMap(Class.forName(argv[1]), xid);
		Object o;
		int i = 0;
		map.headSetStream(xid, (Comparable) map.first()).forEach(o ->System.out.println("["+(i++)+"]"+o));
		map.headSetStream(xid, (Comparable) map.first()).forEach(o -> {			
			System.out.println("["+(i++)+"]"+o);
			comparableClass.mapEntry = (Map.Entry) o;
			...
		});
		map.tailSetStream(xid, (Comparable) map.last()).forEach(o -> {
			System.out.println("["+(i++)+"]"+o);
		});
		map.subSetStream(xid, (Comparable) map.first(), (Comparable) map.last()).forEach(o -> {
			System.out.println("["+(i++)+"]"+o);
		});
```

<p/>

New capabilities include a ClassTool to rapidly adapt existing classes to use built-in Java functionality for serialization and class indexing.

Starting with this:
```

package com;

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
	private String l;
	@ComparisonOrderMethod
	public String getL() {
		return l;
	}
	public TestTooling2(int key1, String key2, String key3) {
		this.i = key1;
		this.j = key2;
		this.l = key3;
	}
}

```
The ClassTool runs in one command line to produce a fully instrumented version like this:

```
package com;

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
public class TestTooling2 implements java.io.Serializable,java.lang.Comparable{
	private static final long serialVersionUID = 1L;
	@ComparisonOrderField
	private int i;
	@ComparisonOrderField
	private String j;
	private String l;
	@ComparisonOrderMethod
	public String getL() {
		return l;
	}
	public TestTooling2(int key1, String key2, String key3) {
		this.i = key1;
		this.j = key2;
		this.l = key3;
	}
	@Override
	public int compareTo(Object o) {
		int n;
		if(i < ((TestTooling2)o).i)
			return -1;
		if(i > ((TestTooling2)o).i)
			return 1;
		n = j.compareTo(((TestTooling2)o).j);
		if(n != 0)
			return n;
		n = getL().compareTo(((TestTooling2)o).getL());
		if(n != 0)
			return n;
		return 0;
	}

	public TestTooling2() {}

}


```

And the class is easily provisioned for indexing in the RocksDB database via the key order specified!


