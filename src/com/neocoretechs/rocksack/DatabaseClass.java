package com.neocoretechs.rocksack;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
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
 * control over placement of the instances. 
 * @author Jonathan N Groff (C) NeoCoreTechs 2024
 *
 */
@Retention(RUNTIME)
@Target(TYPE)
public @interface DatabaseClass {
	public String tablespace() default "";
	public String column() default "";
}