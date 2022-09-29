package com.neocoretechs.rocksack;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;


public class SerializedComparator extends AbstractComparator {

	public SerializedComparator() {
		super(new ComparatorOptions());
	}

	@Override
	public int compare(ByteBuffer arg0, ByteBuffer arg1) {
		try {
			byte[] b1 = new byte[arg0.remaining()];
			arg0.get(b1);
			byte[] b2 = new byte[arg1.remaining()];
			arg1.get(b2);
			Object obj1 = deserializeObject(b1);
			Object obj2 = deserializeObject(b2);
			return ((Comparable)obj1).compareTo(obj2);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@Override
	public String name() {
		return this.getClass().getName();
	}
	/**
	* static method for serialized byte to object conversion
	* @param obuf the byte buffer containing serialized data
	* @return Object instance
	* @exception IOException cannot convert
	*/
	public static Object deserializeObject(byte[] obuf) throws IOException {
		Object Od;
		try {
			ObjectInputStream s;
			ByteArrayInputStream bais = new ByteArrayInputStream(obuf);
			ReadableByteChannel rbc = Channels.newChannel(bais);
			s = new ObjectInputStream(Channels.newInputStream(rbc));
			Od = s.readObject();
			s.close();
			bais.close();
			rbc.close();
		} catch (IOException ioe) {
			throw new IOException(
				"deserializeObject: "
					+ ioe.toString()
					+ ": Class Unreadable, may have been modified beyond version compatibility: from buffer of length "
					+ obuf.length);
		} catch (ClassNotFoundException cnf) {
			throw new IOException(
				cnf.toString()
					+ ":Class Not found, may have been modified beyond version compatibility");
		}
		return Od;
	}
	/**
	* Static method for object to serialized byte conversion.
	* Uses DirectByteArrayOutputStream, which allows underlying buffer to be retrieved without
	* copying entire backing store
	* @param Ob the user object
	* @return byte buffer containing serialized data
	* @exception IOException cannot convert
	*/
	public static byte[] serializeObject(Object Ob) throws IOException {
		byte[] retbytes;
		DirectByteArrayOutputStream baos = new DirectByteArrayOutputStream();
		ObjectOutput s = new ObjectOutputStream(baos);
		s.writeObject(Ob);
		s.flush();
		baos.flush();
		retbytes = baos.getBuf();
		s.close();
		baos.close();
		return retbytes;
	}
}
