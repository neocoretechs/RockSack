package com.neocoretechs.rocksack;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectStreamClass;
import java.net.URL;
import java.net.URLClassLoader;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
/**
 * Class to use annotation tooling for RockSack to generate the necessary fields and methods for
 * storage and retrieval under the java.lang.Comparable interface as used throughout the language.
 * The ordering of the keys is defined here as by the annotation order fields. We use the
 * {@link CompareAndSerialize} annotation to designate the class as toolable. The {@link ComparisonOrderField} and
 * {@link ComparisonOrderMethod}.We generate compareTo method and Serializable interface
 * implementation with SerialUID. No modifications will affect the operation of the original class.
 * The original class will be backed up as [OriginalClassName].bak before modification.
 * {@link com.neocoretechs.rocksack.test.TestTooling1}
 * {@link com.neocoretechs.rocksack.test.TestTooling2}
 * TODO: check to see if class extends class that implements compareTo, then call superclass compareTo if we
 * need to add Comparable interface.
 * If we generate a serialVersionUID we add a default constructor as well.<p/>
 * Command line is java -cp rockcksdb.jar;rocksack.jar com.neocoretechs.rocksack.ClassTool yourjavaclass.java
 * <p/>
 * You also need the correct directory structure to match your package naming convention as the tool relies on runtime
 * compilation of the source java. Easy way is to call your package com, create com directory, then specify com/yourjavaclass.java on
 * the command line. Later, change your package name. After running the tool you should see your original
 * source as yourjavaclass.java.bak, the modified class as yourjavaclass.java, and the associated class files from runtime
 * compilation process.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2023
 *
 */
public class ClassTool {
	private static boolean DEBUG = true;
	private static int classLine = -1; // location of space after class space name decl
	private static int classPosEnd = -1; // location of curly at end of class decl

	private static int serialVerLine = -1;
	private static int serialVerPos = -1;
	private static int lineLast = -1; // location of last { line
	//static int cnt = 0;

	private static List<String> fileLines = new ArrayList<String>();
	/**
	 * Must declare extends BEFORE implements.
	 * @param args
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, IllegalArgumentException, IOException {
		classLine = -1; // location of space after class space name decl
		classPosEnd = -1; // location of curly at end of class decl
		serialVerLine = -1;
		serialVerPos = -1;
		lineLast = -1; // location of last { line
		//cnt = 0;
		//
		readLines(args[0]);
		postprocess();
		if(DEBUG)
			debugParse();
		// write backup of original
		try {
			Files.copy(Paths.get(args[0]),Paths.get(args[0]+".bak"));
		} catch(FileAlreadyExistsException fie) {
			System.out.println("Backup file already exists, will not overwrite");
			return;
		}
		Class targetClass =  compile(args[0]);
		Class<?>[] interfaces = targetClass.getInterfaces();
		boolean hasSerializable = false;
		boolean hasComparable = false;
		for(Class c: interfaces) {
			if(DEBUG)
				System.out.println("Interface:"+c);
			if(c.equals(java.lang.Comparable.class))
				hasComparable = true;
			else
				if(c.equals(java.io.Serializable.class))
					hasSerializable = true;
		}
		Class<?> superclass = targetClass.getSuperclass();
		if(DEBUG)
			System.out.println("Compiled instance:"/*"+classToTool*/+" of class:"+targetClass+" superclass:"+superclass);
		// see if we need to create Serializable interface tooling
		String newImpl;
		if(interfaces.length == 0) {
			if(DEBUG)
				System.out.println("NO detected 'implements'. class line "+classLine);
			// no implements
			// no extends? put implements directly above class line end decl
			newImpl = fileLines.get(classLine).substring(0,classPosEnd)+
					" implements java.io.Serializable,java.lang.Comparable"+
					fileLines.get(classLine).substring(classPosEnd);
			generateCompareTo(args[0], targetClass, newImpl);
		} else {
			if(DEBUG)
				System.out.println("Detected interfaces, class line "+classLine);
			if(!hasSerializable && !hasComparable) {
				// implements, but not our interfaces
				if(DEBUG)
					System.out.println("Detected interfaces, but not required interfaces.");
				newImpl = fileLines.get(classLine).substring(0,classPosEnd)+
						",java.io.Serializable,java.lang.Comparable"+
						fileLines.get(classLine).substring(classPosEnd);
				if(DEBUG)
					System.out.println("New impl:"+newImpl);
				generateCompareTo(args[0], targetClass, newImpl);
			} else {
				if(!hasSerializable) {
					// implements, but not Serializable, has compareTo
					if(DEBUG)
						System.out.println("Detected interfaces, but not with Serializable interface.");
					newImpl = fileLines.get(classLine).substring(0,classPosEnd)+
							",java.io.Serializable"+
							fileLines.get(classLine).substring(classPosEnd);
					if(DEBUG)
						System.out.println("New impl:"+newImpl);
					fileLines.set(classLine, newImpl);
				} else {
					if(!hasComparable) {
						// implements but no Comparable, add compareTo
						if(DEBUG)
							System.out.println("Detected interfaces, but not with Comparable interface.");
						// implements, but not our Comparable
						newImpl = fileLines.get(classLine).substring(0,classPosEnd)+
								",java.lang.Comparable"+
								fileLines.get(classLine).substring(classPosEnd);
						if(DEBUG)
							System.out.println("New impl:"+newImpl);
						generateCompareTo(args[0], targetClass, newImpl);
					}
					// all tooling was already complete, but check for serial version UID
				}
			}
		}
		writeLines(args[0]);

		// compile new class
		//classToTool = compile(args[0]);
		//targetClass = classToTool.getClass();
		targetClass = compile(args[0]);
		if(DEBUG)
			System.out.println("New Compiled instance:"+/*classToTool+*/" of class:"+targetClass);
		// Now we can insert serialVersionUID since we have a guaranteed serializable class
	    ObjectStreamClass objectstreamclass = ObjectStreamClass.lookup(targetClass);
		// we may have to generate default constructor
		if(objectstreamclass == null) {
			generateDefaultCtor(targetClass);
			writeLines(args[0]);
			targetClass = compile(args[0]);
			objectstreamclass = ObjectStreamClass.lookup(targetClass); // try again to generate serialversionuid with default ctor in place
		}
		if(serialVerLine == -1) {
			String serialVer = InstrumentClass.resolveClass(objectstreamclass);
			if(DEBUG)
				System.out.println("Generated Serial UID="+serialVer);
			fileLines.add(classLine+1, serialVer);
			writeLines(args[0]);
		}
		System.out.println("Tooling complete, backup of original source file is in "+args[0]+".bak");
	}
	
	private static void generateDefaultCtor(Class targetClass) {
		StringBuilder defCtor = new StringBuilder("\tpublic ");
		defCtor.append(targetClass.getSimpleName());
		defCtor.append("() {}\r\n");
		findLastLine();
		fileLines.add(lineLast, defCtor.toString());
	}

	private static void generateCompareTo(String javaFile, Class targetClass, String newImpl) throws IllegalArgumentException, IllegalAccessException, IOException {
		if(DEBUG)
			System.out.println("Setting class line "+classLine+" with newImpl:"+newImpl);
		fileLines.set(classLine, newImpl);
		// if we have method already, dont duplicate it
		try {
			targetClass.getMethod("compareTo", Object.class);
			if(DEBUG) {
				System.out.println("** WARNING compareTo method already declared");
			}
		} catch (NoSuchMethodException | SecurityException e) {
			InstrumentClass instrument = new InstrumentClass();
			// return with compareTo statement constructed
			// If we dont implement Comparable, but superclass does, call superclass compareTo in our new method
			String compareToStatement = instrument.process(javaFile, targetClass, Comparable.class.isAssignableFrom(targetClass));
			findLastLine();
			if(DEBUG)
				System.out.println("Inserting compareTo @ last line of class decl (EOF):"+lineLast);
			fileLines.add(lineLast, compareToStatement);
		}
		
	}

	private static void debugParse() {
	System.out.println(
	 classLine+"= class decl line \r\n"+
	 classPosEnd+"= end position of class decl \r\n"+
	 serialVerLine+"= serialVersionUID line\r\n");
	}
	/**
	 * Set all static vars that indicate positions of parsed entities of interest.
	 * We will parse until the first occurrence of "{" indicating end of class decl
	 * @param javaFile
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	private static void preprocess(String s) {
		if(DEBUG)
			//System.out.println(cnt+" parse:"+s);
			System.out.println(fileLines.size()+" parse:"+s);

			if(serialVerLine == -1) {
				serialVerPos = s.indexOf("serialVersionUID");
				if(serialVerPos != -1)
					serialVerLine = fileLines.size();//cnt;
			}
			if(classLine == -1) {
				classPosEnd = s.indexOf("{");
				if(classPosEnd != -1) {
					classLine = fileLines.size(); //cnt;
				}
			}
			//cnt++;
	}
	
	/**
	 * Parse array of inline comments interspersed with valid code.
	 * Maintain length of actual line, skipping comment chunks, adding offset to valid parse elements
	 * @param s
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	private static void preprocess(List<String> ss) {
		int ccnt = 0;
		for(String s: ss) {
			if(DEBUG)
				//System.out.println(cnt+" parse:"+s);
				System.out.println(fileLines.size()+" parse:"+s);
			if(s.startsWith("/*")) {
				ccnt += s.length();
				continue;
			}
			if(serialVerLine == -1) {
				serialVerPos = s.indexOf("serialVersionUID");
				if(serialVerPos != -1)
					//serialVerLine = cnt;
					serialVerLine = fileLines.size();
			}
			if(classLine == -1) {
				classPosEnd = s.indexOf("{");
				if(classPosEnd != -1) {
					classPosEnd += ccnt;
					//classLine = cnt;
					classLine = fileLines.size();
				}
			}
			ccnt += s.length();
		}
		//cnt++;
	}
	
	private static void postprocess() {
		if(classPosEnd == -1) {
			throw new IllegalArgumentException("Can't find end of class declaration");
		}

	}
	
	/**
	 * Find the last curl brace of class declaration to place our compareTo method
	 * If we cant find it, exit, if found, set lineLast variable at that line -1
	 */
	private static void findLastLine() {
		// find last curly in file for location of compareTo if necessary
		lineLast = fileLines.size()-1;
		while(lineLast > 0) {
			if(fileLines.get(lineLast).contains("}"))
				break;
			--lineLast;
		}

		if(lineLast <= 0) {
			System.out.println("System could not locate last curly brace of class declaration, will exit..");
			System.exit(1);
		}
	}
	/**
	 * Read and parse, if we detect comment embedded, send to method to parse and return array of
	 * elements interspersed with comments. Pattern is to preprocess/add to list since
	 * we use progressive size of list to designate current line position
	 * @param javaFile
	 * @return
	 * @throws IOException
	 */
	private static void readLines(String javaFile) throws IOException {
		BufferedReader reader;
		String parseLine;
		boolean commentBlk = false;

		reader = new BufferedReader(new FileReader(javaFile));
		String line = reader.readLine();
		while (line != null) {
			if(commentBlk) {
				if(line.contains("*/")) {
					commentBlk = false;
					parseLine = line.substring(line.indexOf("*/")+2);
					preprocess(parseLine);
				}
				//lines.add(line);
				fileLines.add(line);
				line = reader.readLine();
				continue;
			}
			if(line.contains("/*") && line.contains("*/")) {
				// if the number of starting comment blocks greater than ending comments blocks, commentBlk true
				// because we can end a comment on this line, then proceed to include another blocked comment
				if(occurrences(line,"/*") > occurrences(line,"*/"))
					commentBlk = true;
				//parseLine = line.substring(0,line.indexOf("/*"))+line.substring(line.indexOf("*/")+2);
				List<String> cmts = parseAllInlineComments(line);
				preprocess(cmts);
				//lines.add(line);
				fileLines.add(line);
				line = reader.readLine();
				continue;
			}
			if(line.contains("/*")) {
				parseLine = line.substring(0,line.indexOf("/*"));
				preprocess(parseLine);
				commentBlk = true;
				//lines.add(line);
				fileLines.add(line);
				line = reader.readLine();
				continue;
			}
			if(line.contains("//")) {
				parseLine = line.substring(0,line.indexOf("//"));
				preprocess(parseLine);
				//lines.add(line);
				fileLines.add(line);
				line = reader.readLine();
			}
			// read next line
			//lines.add(line);
			preprocess(line);
			fileLines.add(line);
			line = reader.readLine();
		}
		reader.close();
		//return lines;
	}
	
	private static void writeLines(String javaFile) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(javaFile));
		for(String s: fileLines){
			if(DEBUG)
				System.out.println(s+"\r\n");
			writer.write(s+"\r\n");
		}
		writer.flush();
		writer.close();
	}
	
	/**
	 * Parse all the inline comments in this line
	 * @param line
	 * @return
	 */
	private static List<String> parseAllInlineComments(String line) {
		int ccnt = 0;
		int ccnt2 = 0;
		int dcnt = 0;
		ArrayList<String> list = new ArrayList<String>();
		while(ccnt != -1) {
			ccnt = line.indexOf("/*",dcnt);
			if(ccnt != -1) {
				list.add(line.substring(dcnt,ccnt));
				ccnt2 = line.indexOf("*/",ccnt+2);
				if(ccnt2 != -1) {
					dcnt = ccnt2+2;
					list.add(line.substring(ccnt,ccnt2+2));
				}
			} else {
				if(ccnt2 > 0 && ccnt2+3 <= line.length())
					list.add(line.substring(ccnt2+2));
			}
		}
		if(DEBUG)
			Arrays.toString(list.toArray());
		return list;
	}
	
	static int occurrences(String str, String substr) {
		int occurrences = 0;
		int index = str.indexOf(substr);
		while (index != -1) {
			occurrences++;
			index = str.indexOf(substr, index + 1);
		}
		return occurrences;
	}
	
	private static Class compile(String javaFileName) {
	  try {
		File javaFile = new File(javaFileName);
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);

        // This sets up the class path that the compiler will use.
        // I've added the .jar file that contains the DoStuff interface within in it...
        List<String> optionList = new ArrayList<String>();
        optionList.add("-classpath");
        optionList.add(System.getProperty("java.class.path") + ";./");

        Iterable<? extends JavaFileObject> compilationUnit
            = fileManager.getJavaFileObjectsFromFiles(Arrays.asList(javaFile));
        JavaCompiler.CompilationTask task = compiler.getTask(
          null, 
          fileManager, 
          diagnostics, 
          optionList, 
          null, 
          compilationUnit);

        if(task.call()) {
        	// Load and execute
        	// Create a new custom class loader, pointing to the directory that contains the compiled
        	// classes, this should point to the top of the package structure!
        	URLClassLoader classLoader = new URLClassLoader(new URL[]{new File("./").toURI().toURL()});
        	// Load the class from the classloader by name....
        	String javaPackage = javaFileName.substring(0,javaFileName.indexOf(".")).replaceAll("/",".");
        	Class<?> loadedClass = classLoader.loadClass(javaPackage);
        	// Create a new instance...this requires default ctor explicit
        	//return loadedClass.newInstance();
        	fileManager.close();
        	return loadedClass;
          /************************************************************************************************* Load and execute **/
        } else {
          for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
            System.out.format("Error on line %d in %s%n",
                diagnostic.getLineNumber(),
                diagnostic.getSource().toUri());
          }
        }
        fileManager.close();
      } catch (IOException | ClassNotFoundException exp) {//| InstantiationException | IllegalAccessException exp) {
        exp.printStackTrace();
      }
	  return null;
	}
}
