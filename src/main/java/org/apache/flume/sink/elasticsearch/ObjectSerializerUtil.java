package org.apache.flume.sink.elasticsearch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;

import com.saic.util.gson.Obj2Json;

public class ObjectSerializerUtil {

	private final static Logger logger = Logger.getLogger(ObjectSerializerUtil.class);

	public static byte[] javaSerialize(Object msg) {
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(bo);
			oos.writeObject(msg);
			oos.flush();
			oos.close();
			bo.close();
		} catch (IOException e) {
			logger.error("message to bytes error: ", e);
		}
		return bo.toByteArray();
	}
	
	public static byte[] jsonSerialize(Object data, String encoding) {
		try {
			String jsonStr = Obj2Json.getJSONStr(data);
			return jsonStr.getBytes(encoding);
		} catch (IOException e) {
			logger.error("message to bytes error: ", e);
		}
		return null;
	}

	public static Object deJavaSerialize(byte[] data) {
		Object obj = null;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			ObjectInputStream ois = new ObjectInputStream(bis);
			obj = (ObjectSerializerUtil) ois.readObject();
			ois.close();
			bis.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
		}
		return obj;
	}

}
