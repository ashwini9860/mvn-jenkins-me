package com.alice.aef.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;

public class MapDeserializer implements Deserializer<Map> {

	private static final Logger LOGGER = LoggerFactory.getLogger(MapDeserializer.class);

	@Override
	public void configure(Map<String, ?> map, boolean b) {

	}

	/**
	 * Deserialize incoming message from Kafka
	 *
	 * - added a quick fix for skipping messages that can't be deserialized (as otherwise kafka consumer infinitely
	 *  retries to read it)
	 *
	 * @param topic String
	 * @param bytes	message data
	 * @return Map containing message data
	 */
	@Override
	public Map deserialize(String topic, byte[] bytes) {
		try {
			ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
			ObjectInputStream in = new ObjectInputStream(byteIn);
			return (Map<String, Object>) in.readObject();
		} catch (Exception ex) {
			LOGGER.error("Failed to deserialize message on topic: {}", topic, ex);
		}
		// This error handling approach might solve the problem of skipping messages that can't be deserialized
		// but it can also cause some problems due to "lost" messages
		// a better approach would probably be to use a common message schema registry and to avoid having messages
		// in a "wrong" format
		return null;
	}

	@Override
	public void close() {

	}
}
