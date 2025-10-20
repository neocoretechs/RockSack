package com.neocoretechs.rocksack;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import org.rocksdb.CompressionType;
import org.rocksdb.CompactionStyle;
/**
 * Using this annotation on a class to be stored in the RockSack allows control over the storage of instances 
 * in a particular tablespace, and column. RocksDB uses the 'ColumnFamily' concept to represent 'columns' or collections
 * of data than can be grouped together under a common set of attributes defined in the options upon opening.<p>
 * It is analogous to separate databases stored under a unified set of files and directories.  
 * It can be considered separate columns or tablespaces or other
 * logical divisions in other systems. Here, we can store different class instances such as subclasses.<p>
 * In RockSack, the database is stored under the tablespace directory which has the database name concatenated with the class name 
 * and is used to obtain the Map. This tablespace directory will then contain the RocksDB files and logs etc.<p>
 * Using one of the methods in {@link com.neocoretechs.rocksack.session.DatabaseManager}, such as 'getMap', and transparent to
 * the user, this annotation then controls whether instances are stored in a different tablspace and internal column of that tablespace.<p>
 * Looking at the example in {@link com.neocoretechs.rocksack.test.BatteryKVDerived} we see that if we want to store subclass
 * instances with a superclass, we have just the 'column' attribute with the fully qualified name of the superclass. This will
 * ensure that sets retrieved include both subclasses and superclasses. If we want to store the subclass in a different column within the same
 * tablespace, we could have a different column name or omit the column attribute, which would then store the instances under the derived
 * class name in the tablespace of the direct superclass. So, omitting both tablespace and column attributes stores the instances in the direct
 * superclass under the column name of the subclass. So using this annotations and combinations of the attributes gives the user full
 * control over placement of the instances. <p>
 * Default options are set to write_buffer_size 64MB, max_write_buffer_number 2, compression SNAPPY, or no, compaction LEVEL, BlockSize 4k<p>
 * Additional Column overrides are present to control options on ColumnFamilies during creation of ColumnFamilyDescriptors. i.e.:<p>
 * @DatabaseClass( <br>
 *   tablespace = "metrics", <br>
 *   column = "counters", <br>
 *   cfKey = "LZ4_FIFO_32_2" (Compression_Compaction_Writebuffer_size, X is'use default':LZ4_X_64_X, LZ4_X_64_2, NO_FIFO_32_2, DEFAULT_COLUNM_FAMILY), <br>
 *   compression = CompressionType.LZ4_COMPRESSION, <br>
 *   compactionStyle = CompactionStyle.FIFO, <br>
 *   writeBufferSize = 32L * SizeUnit.MB, <br>
 *   maxWriteBufferNumber = 2 ) <br>
 * @author Jonathan N Groff (C) NeoCoreTechs 2024
 *
 */
@Retention(RUNTIME)
@Target(TYPE)
public @interface DatabaseClass {
	public String tablespace() default "";
	public String column() default "";

	// Optional overrides
	CompressionType compression() default CompressionType.NO_COMPRESSION;
	CompactionStyle compactionStyle() default CompactionStyle.UNIVERSAL;
	long writeBufferSize() default -1; // -1 means "use default"
	int maxWriteBufferNumber() default -1;

}