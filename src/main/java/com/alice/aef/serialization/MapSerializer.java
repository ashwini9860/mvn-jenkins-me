package com.alice.aef.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class MapSerializer implements Serializer<Map> {
	@Override
	public void configure(Map<String, ?> map, boolean b) {

	}

	@Override
	public byte[] serialize(String s, Map map) {
		if (map == null) {
			return null;
		} else {
			try {
				ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutputStream out = new ObjectOutputStream(byteOut);
				out.writeObject(map);
				return byteOut.toByteArray();
			} catch (IOException e) {
				e.printStackTrace();
				throw new SerializationException("Exception to serialize Map");
			}
		}
	}

	@Override
	public void close() {

	}
}
