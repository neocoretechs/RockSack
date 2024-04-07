package com.neocoretechs.rocksack;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectStreamClass;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Instrument a class for RockSack compatibility. Nothing we do changes the base functionality
 * or should affect operation of the original class. We need to ensure that it is Serializable
 * and that it implements a compareTo method to order it in the data store. Note that most
 * Java classes do this by default, but new ad-hoc classes may not. We rely on user adding
 * annotations for {@link CompareAndSerialize} and {@link ComparisonOrderField}(order=n) and {@link ComparisonOrderMethod}(order=n)
 * where n is the order of the key element in the overall collection that will order instances of the class.
 * We will create a compareTo method based on this order.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2023
 *
 */
public class InstrumentClass {
	private static boolean DEBUG = false;
	private boolean hasAtLeastOneMethod = false; // if we have at least one accessor generate an int in compareTo to hold results
	/**
	 * Does it implement Serializable and Comparable already? Does it extend a Comparable such
	 * that super() needs called on new compareTo? Do instrumented fields implement Comparable?
	 * @param object
	 * @return
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	public List<String> process(String javaFile, Class clazz, boolean callSuperCompareTo) throws IOException, IllegalArgumentException, IllegalAccessException {
		ArrayList<String> outLines = new ArrayList<String>();
        checkIfSerializable(clazz);
        //initializeObject(object);
        Map<Integer, NameAndType> elements = getFieldOrder(clazz);
        getMethodOrder(clazz, elements);
        List<String> compareToElements = generateCompareTo(clazz, elements, callSuperCompareTo);
        compareToElements = generateCompareTo(compareToElements);
        List<String> externalReads = generateReads(clazz, elements, false);
        List<String> externalWrites = generateWrites(clazz, elements, false);
        List<String> externals = generateExternal(externalReads, externalWrites);
        outLines.addAll(compareToElements);
        outLines.addAll(externals);
        if(DEBUG) {
        	elements.entrySet().stream().forEach(e -> System.out.println(e.getKey() + ":" + e.getValue()));
        	compareToElements.stream().forEach(e -> System.out.println(e));
        }
        return outLines;
	}

	private void checkIfSerializable(Class clazz) throws IOException {
        //if (Objects.isNull(object)) {
         //   throw new IOException("Can't serialize a null object");
        //}
        //Class<?> clazz = object.getClass();
        if (!clazz.isAnnotationPresent(CompareAndSerialize.class)) {
            throw new IOException("The class " + clazz.getSimpleName() + " is not annotated with CompareAndSerialize");
        }
    }

	static String resolveClass(ObjectStreamClass classname) throws ClassNotFoundException {
        if (classname != null) {
            return "\tprivate static final long serialVersionUID = " +
                classname.getSerialVersionUID() + "L;";
        } else {
			if(DEBUG)
				System.out.println("** WARNING: unable to generate serialVersionUID for class "+classname+" defaulting to 1");
			return "\tprivate static final long serialVersionUID = 1L;";
        }
    }
   /* private void initializeObject(Object object) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Class<?> clazz = object.getClass();
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(Init.class)) {
                method.setAccessible(true);
                method.invoke(object);
            }
        }
    }*/
	/**
	 * Establish field order for use in creation of compareTo method (primary key)
	 * based on annotation in class. If the user failed to supply an order use the 
	 * order of appearance of the fields as default. If we encounter a value, it will always supercede any
	 * default value present. Defaults encountered after initial ordering will increment.
	 * @param object
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
    private Map<Integer, NameAndType> getFieldOrder(Class clazz) throws IllegalArgumentException, IllegalAccessException {
    	int defaultOrder = 0;
        //Class<?> clazz = object.getClass();
        Map<Integer, NameAndType> elementsMap = new HashMap<>();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            if (field.isAnnotationPresent(ComparisonOrderField.class)) {
               	NameAndType nameAndType = new NameAndType((String) field.getName(), field.getType(), true, field);
            	int order = getKey(field);
            	if(order == 0) {
            		++defaultOrder;
            		order = defaultOrder;
            	} else {
            		defaultOrder = order;
            	}
                elementsMap.put(order, nameAndType);
            }
        }
        return elementsMap;
    }
	/**
	 * Establish method order for use in in creation of compatreTo method for primary key based on 
	 * annotation in class. If the user failed to supply an order use the 
	 * order of appearance of the fields as default. If we encounter a value, it will always supercede any
	 * default value present. Defaults encountered after initial ordering will increment.
	 * @param object
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
    private void getMethodOrder(Class clazz, Map<Integer, NameAndType> elementsMap) throws IllegalArgumentException, IllegalAccessException {
    	int defaultOrder = elementsMap.size();
        //Class<?> clazz = object.getClass();
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(ComparisonOrderMethod.class)) {
            	hasAtLeastOneMethod = true;
                method.setAccessible(true);
               	NameAndType nameAndType = new NameAndType((String) method.getName(), method.getReturnType(), false, method);
            	int order = getKey(method);
            	if(order == 0) {
            		++defaultOrder;
            		order = defaultOrder;
            	} else {
            		defaultOrder = order;
            	}
                elementsMap.put(order, nameAndType);
            }
        }
    }
    
    private int getKey(Field field) {
    	 return field.getAnnotation(ComparisonOrderField.class).order();
    }
    
    private int getKey(Method method) {
   	 return method.getAnnotation(ComparisonOrderMethod.class).order();
    }
    
	private ArrayList<String> generateCompareTo(List<String> compareToElements) {
		ArrayList<String> sb = new ArrayList<String>();
		sb.add("\t@Override\r\n");
		sb.add("\tpublic int compareTo(Object o) {\r\n");
		if(hasAtLeastOneMethod) {
			sb.add("\t\tint n;\r\n");
		}
		sb.addAll(compareToElements);
		sb.add("\t\treturn 0;\r\n");
		sb.add("\t}\r\n");
		return sb;
	}
	
    private List<String> generateCompareTo(Class clazz, Map<Integer, NameAndType> elements, boolean callSuper) {
    	ArrayList<String> s = new ArrayList<String>();
    	if(callSuper) {
    		hasAtLeastOneMethod = true;
	   		s.add("\t\tn = super.compareTo(o);\r\n");
  			s.add("\t\tif(n != 0) return n;\r\n");
    	}
    	Stream<Map.Entry<Integer, NameAndType>> sorted =
    		    elements.entrySet().stream().sorted(Map.Entry.comparingByKey());
    	sorted.forEach(e ->  {
    		NameAndType key = (NameAndType)e.getValue();
    		StringBuilder sb = new StringBuilder();
   			// primitive or object field or method?
    		if(key.isField) {
    			if(key.type.isPrimitive()) {
    				switch(key.type.getName()) {
    					case "int":
    						sb.append("\t\tn=Integer.compareUnsigned(");
    						sb.append(key.name);
    						sb.append(",");
    	    				sb.append("((");
    	    				sb.append(clazz.getSimpleName());
    	    				sb.append(")o).");
    	    				sb.append(key.name);
    	    				sb.append(");\r\n");
    						break;
    					case "long":
    						sb.append("\t\tn=Long.compareUnsigned(");
     						sb.append(key.name);
    						sb.append(",");
    	    				sb.append("((");
    	    				sb.append(clazz.getSimpleName());
    	    				sb.append(")o).");
    	    				sb.append(key.name);
    	    				sb.append(");\r\n");
    						break;
    					case "short":
    						sb.append("\t\tn=Integer.compare(Short.toUnsignedInt(");
       						sb.append(key.name);
      						sb.append(",Short.toUnsignedInt(");
      	    				sb.append("((");
    	    				sb.append(clazz.getSimpleName());
    	    				sb.append(")o).");
    	    				sb.append(key.name);
    	    				sb.append(");\r\n");
    	    				break;
    					case "byte":
       						sb.append("\t\tn=Integer.compare(Byte.toUnsignedInt(");
       						sb.append(key.name);
      						sb.append(",Byte.toUnsignedInt(");
      	    				sb.append("((");
    	    				sb.append(clazz.getSimpleName());
    	    				sb.append(")o).");
    	    				sb.append(key.name);
    	    				sb.append(");\r\n");
    	    				break;
      					case "double":
       						sb.append("\t\tn=Long.compareUnsigned(Double.doubleToRawLongBits(");
       						sb.append(key.name);
      						sb.append(",Double.doubleToRawLongBits(");
      	    				sb.append("((");
    	    				sb.append(clazz.getSimpleName());
    	    				sb.append(")o).");
    	    				sb.append(key.name);
    	    				sb.append(");\r\n");
    	    				break;
       					case "float":
       						sb.append("\t\tn=Integer.compareUnsigned(Float.floatToRawIntBits(");
       						sb.append(key.name);
      						sb.append(",Float.floatToRawIntBits(");
      	    				sb.append("((");
    	    				sb.append(clazz.getSimpleName());
    	    				sb.append(")o).");
    	    				sb.append(key.name);
    	    				sb.append(");\r\n");
    	    				break;
     					case "boolean":
       						sb.append("\t\tn=Boolean.compare(");
       						sb.append(key.name);
      						sb.append(",");
      	    				sb.append("((");
    	    				sb.append(clazz.getSimpleName());
    	    				sb.append(")o).");
    	    				sb.append(key.name);
    	    				sb.append(");\r\n");
    	    				break;	
    				}
    				s.add(sb.toString());
    				sb = new StringBuilder();
    				//sb.append(key.name);
    				//sb.append(" < ");
    				//sb.append(",");
    				//sb.append("((");
    				//sb.append(clazz.getSimpleName());
    				//sb.append(")o).");
    				//sb.append(key.name);
    				//sb.append(");");
    				//sb.append(")");
    				//sb.append("\r\n\t\t\treturn -1;\r\n");
    				s.add("\t\tif(n != 0) return n;\r\n");
    				//
    				//sb.append("\t\tif(");
    				//sb.append(key.name);
    				//sb.append(" > ");
    				//sb.append("((");
    				//sb.append(clazz.getSimpleName());
    				//sb.append(")o).");
    				//sb.append(key.name);
    				//sb.append(")");
    				//sb.append("\r\n\t\t\treturn 1;\r\n");
    			} else { // object field, not primitive, object must deliver total order bytes for readObject!
    	 			// use of hasAtLeastOneMethod to generate n temp var
    	   			if(!Comparable.class.isAssignableFrom(key.type))
        				throw new RuntimeException("Object Field "+key.name+" must implement Comparable interface");
    	   			sb.append("\t\tn = ");
    	   			sb.append(key.name);
    	   			sb.append(".compareTo(((");
    				sb.append(clazz.getSimpleName());
    				sb.append(")o).");
    				sb.append(key.name);
    				sb.append(");\r\n");
    				s.add(sb.toString());
      				s.add("\t\tif(n != 0) return n;\r\n");
    				//
    			}
    		} else { // accessor method
    			if(key.type.isPrimitive()) { //accessor method name, and type is returnType
    				switch(key.type.getName()) {
    				case "int":
    					sb.append("\t\tn=Integer.compareUnsigned(");
    					sb.append(key.name);
    					sb.append("(),");
    					sb.append("((");
    					sb.append(clazz.getSimpleName());
    					sb.append(")o).");
    					sb.append(key.name);
    					sb.append("());\r\n");
    					break;
    				case "long":
    					sb.append("\t\tn=Long.compareUnsigned(");
    					sb.append(key.name);
    					sb.append("(),");
    					sb.append("((");
    					sb.append(clazz.getSimpleName());
    					sb.append(")o).");
    					sb.append(key.name);
    					sb.append("());\r\n");
    					break;
    				case "short":
    					sb.append("\t\tn=Integer.compare(Short.toUnsignedInt(");
    					sb.append(key.name);
    					sb.append("()),Short.toUnsignedInt(");
    					sb.append("((");
    					sb.append(clazz.getSimpleName());
    					sb.append(")o).");
    					sb.append(key.name);
    					sb.append("()));\r\n");
    					break;
    				case "byte":
    					sb.append("\t\tn=Integer.compare(Byte.toUnsignedInt(");
    					sb.append(key.name);
    					sb.append("()),Byte.toUnsignedInt(");
    					sb.append("((");
    					sb.append(clazz.getSimpleName());
    					sb.append(")o).");
    					sb.append(key.name);
    					sb.append("()));\r\n");
    					break;
    				case "double":
    					sb.append("\t\tn=Long.compareUnsigned(Double.doubleToRawLongBits(");
    					sb.append(key.name);
    					sb.append("()),Double.doubleToRawLongBits(");
    					sb.append("((");
    					sb.append(clazz.getSimpleName());
    					sb.append(")o).");
    					sb.append(key.name);
    					sb.append("()));\r\n");
    					break;
    				case "float":
    					sb.append("\t\tn=Integer.compareUnsigned(Float.floatToRawIntBits(");
    					sb.append(key.name);
    					sb.append("()),Float.floatToRawIntBits(");
    					sb.append("((");
    					sb.append(clazz.getSimpleName());
    					sb.append(")o).");
    					sb.append(key.name);
    					sb.append("()));\r\n");
    					break;
					case "boolean":
   						sb.append("\t\tn=Boolean.compare(");
   						sb.append(key.name);
  						sb.append("(),");
  	    				sb.append("((");
	    				sb.append(clazz.getSimpleName());
	    				sb.append(")o).");
	    				sb.append(key.name);
	    				sb.append("());\r\n");
	    				break;	
    				}
    				s.add(sb.toString());
    				sb = new StringBuilder();
    				s.add("\t\tif(n != 0) return n;\r\n");
    			} else { // accessor returns object
    				// use of hasAtLeastOneMethod to generate n temp var
    				if(!Comparable.class.isAssignableFrom(key.type))
    					throw new RuntimeException("Accessor Object return Field "+key.name+" must implement Comparable interface");
    				sb.append("\t\tn = ");
    				sb.append(key.name);
    				sb.append("()");
    				sb.append(".compareTo(((");
    				sb.append(clazz.getSimpleName());
    				sb.append(")o).");
    				sb.append(key.name);
    				sb.append("());\r\n");
    				s.add(sb.toString());
    				sb = new StringBuilder();
    				s.add("\t\tif(n != 0) return n;\r\n");
    			}
    		}
    	});
    	return s;
    }
    
    private List<String> generateExternal(List<String> externalReads, List<String> externalWrites) {
    	ArrayList<String> sb = new ArrayList<String>();
      	sb.add("\t@Override\r\n");
    	sb.add("\tpublic void readExternal(java.io.ObjectInput in) throws java.io.IOException,ClassNotFoundException {\r\n");
    	// put external reads here
		externalReads.stream().forEach(e -> {
			sb.add(e.toString());
		});
    	sb.add("\t}\r\n");
      	sb.add("\t@Override\r\n");
    	sb.add("\tpublic void writeExternal(java.io.ObjectOutput out) throws java.io.IOException {\r\n");
    	// put external writes here
		externalWrites.stream().forEach(e -> {
			sb.add(e.toString());
		});
    	sb.add("\t}\r\n");
    	return sb;
    }
    
    private List<String> generateReads(Class clazz, Map<Integer, NameAndType> elements, boolean callSuper) {
      	ArrayList<String> readComponents = new ArrayList<String>();
    	if(callSuper) {
    		hasAtLeastOneMethod = true;
    		StringBuilder s = new StringBuilder();
	   		s.append("\t\tsuper.readExternal(in);\r\n");
			readComponents.add(s.toString());
    	}
    	Stream<Map.Entry<Integer, NameAndType>> sorted =
    		    elements.entrySet().stream().sorted(Map.Entry.comparingByKey());
    	sorted.forEach(e ->  {
    		NameAndType key = (NameAndType)e.getValue();
   			StringBuilder s = new StringBuilder();
   			// primitive or object field or method?
    		if(key.isField) {
				s.append("\t\t");
				s.append(key.name);
    			if(key.type.isPrimitive()) {
    				switch(key.type.getName()) {
    					case "int":
    						s.append("=in.readInt();\r\n");
    						break;
    					case "long":
    						s.append("=in.readLong();\r\n");    
    						break;
    					case "short":
    						s.append("=in.readShort();\r\n");    
    	    				break;
    					case "byte":
    						s.append("=in.readByte();\r\n");    
    	    				break;
      					case "double":
    						s.append("=in.readDouble();\r\n");    
    	    				break;
       					case "float":
    						s.append("=in.readFloat();\r\n");    
    	    				break;	
     					case "boolean":
    						s.append("=in.readBoolean();\r\n");    
    	    				break;	
    				}
    			} else { // object field, not primitive, object must deliver total order bytes for readObject!
					s.append("=(");
					s.append(key.type.getSimpleName());
					s.append(")in.readObject();\r\n");    
    				//
    			}
    		} else { // accessor method
    			if(key.type.isPrimitive()) { //accessor method name, and type is returnType, assume standard pattern getXXX, change to setXXX
    				String muname = key.name.replaceFirst("get", "set");
  					s.append("\t\t");
					s.append(muname);
					s.append("(");
    				switch(key.type.getName()) {
    				case "int":
  						s.append("in.readInt());\r\n");
    					break;
 					case "long":
						s.append("in.readLong());\r\n");    
						break;
					case "short":
						s.append("in.readShort());\r\n");    
	    				break;
					case "byte":
						s.append("in.readByte());\r\n");    
	    				break;
  					case "double":
						s.append("in.readDouble());\r\n");    
	    				break;
   					case "float":
						s.append("in.readFloat());\r\n");    
	    				break;	
 					case "boolean":
						s.append("in.readBoolean());\r\n");    
	    				break;	
    				}
    			} else { // accessor returns object
    				// use of hasAtLeastOneMethod to generate n temp var
     				String muname = key.name.replaceFirst("get", "set");
  					s.append("\t\t");
					s.append(muname);
					s.append("((");
					s.append(key.type.getSimpleName());
					s.append(")in.readObject());\r\n");  
    			}
    		}
    		readComponents.add(s.toString());
    	});
    	return readComponents;   	
    }
    
    private List<String> generateWrites(Class clazz, Map<Integer, NameAndType> elements, boolean callSuper) {
      	ArrayList<String> writeComponents = new ArrayList<String>();
    	if(callSuper) {
    		hasAtLeastOneMethod = true;
    		StringBuilder s = new StringBuilder();
	   		s.append("\t\tsuper.writeExternal(out);\r\n");
			writeComponents.add(s.toString());
    	}
    	Stream<Map.Entry<Integer, NameAndType>> sorted =
    		    elements.entrySet().stream().sorted(Map.Entry.comparingByKey());
    	return writeComponents;
    }
    /**
     * Defines the name of the field or method, the class of the field or class of return type of method,
     * whether field or method, and actual reflected Field or Method
     *
     */
    static class NameAndType {
		String name;
    	Class type;
    	boolean isField;
    	AccessibleObject comparor;
       	public NameAndType(String name, Class type, boolean isField, AccessibleObject comparor) {
    			this.name = name;
    			this.type = type;
    			this.isField = isField;
    			this.comparor = comparor;
    	}
       	@Override
       	public String toString() {
       		return "Field name:"+name+" type:"+type+" isField:"+isField+" element:"+comparor;
       	}
    }
    
}
