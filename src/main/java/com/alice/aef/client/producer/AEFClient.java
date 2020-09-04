package com.alice.aef.client.producer;

import static com.alice.aef.client.AEFConstants.DEFAULT_REQUEST_TIMEOUT;

import com.alice.aef.exception.AEFException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Event fabric client designed to send events to a topic.
 * Topic can be specified during construction (ctor param) or via method param.
 */
public class AEFClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(AEFClient.class);

	private static final String SENDING_DATA_ON_TOPIC_MESSAGE = "Sending data [{}] on topic [{}]";
	private static final String SENDING_REPLY_ON_TOPIC_MESSAGE = "Sending reply data [{}] on topic [{}]";
	private static final String SENT_DATA_ON_TOPIC_MESSAGE = "Sent data [{}] on topic [{}]";
	private int requestTimeout = DEFAULT_REQUEST_TIMEOUT;

	private String topic;


	public AEFClient() {
	}

	public AEFClient(String topic) {
		this.topic = topic;
	}

	@Autowired
	private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

	/**
	 * Send data to destinationTopic
	 *
	 * @param destinationTopic String
	 * @param data             {@code Map<String, Object> data}
	 */
	public void send(String destinationTopic, Map<String, Object> data) {
		LOGGER.debug(SENDING_DATA_ON_TOPIC_MESSAGE, data, destinationTopic);
		ProducerRecord<String, Map<String, Object>> record = makeRecord(destinationTopic, data);
		send(record);
		LOGGER.debug(SENT_DATA_ON_TOPIC_MESSAGE, record, destinationTopic);
	}

	/**
	 * Send rely to a received record
	 *
	 * @param destinationTopic String
	 * @param data             {@code Map<String, Object> data}
	 * @param record           {@code ConsumerRecord<String, Map<String, Object>> record}
	 */
	public void sendReply(String destinationTopic, Map<String, Object> data,
	                      ConsumerRecord<String, Map<String, Object>> record) {
		LOGGER.debug(SENDING_REPLY_ON_TOPIC_MESSAGE, data, destinationTopic);
		ProducerRecord<String, Map<String, Object>> producerRecord = makeReplyRecord(destinationTopic, data, record);
		send(producerRecord);
		LOGGER.debug(SENT_DATA_ON_TOPIC_MESSAGE, record, destinationTopic);
	}

	/**
	 * Synchronously sends ProducerRecord and waits for confirmation.
	 *
	 * @param record {@code ProducerRecord<String, Map<String, Object>> record}
	 */
	private void send(ProducerRecord<String, Map<String, Object>> record) {
		try {
			kafkaTemplate.send(record).get(requestTimeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException iex) {
			Thread.currentThread().interrupt();
			throw new AEFException("Producer thread interrupted ", iex);
		} catch (TimeoutException e) {
			throw new AEFException("Timeout while sending to Kafka ", e);
		} catch (ExecutionException e) {
			throw new AEFException("An error occurred while sending to Kafka ", e);
		}
	}

	public void send(Map<String, Object> data) {
		LOGGER.debug(SENDING_DATA_ON_TOPIC_MESSAGE, data, getTopic());
		ProducerRecord<String, Map<String, Object>> record = makeRecord(getTopic(), data);
		send(record);
		LOGGER.debug(SENT_DATA_ON_TOPIC_MESSAGE, record, topic);
	}

	/**
	 * @param topic String
	 * @param value {@code Map<String, Object> value}
	 * @return {@code ProducerRecord<String, Map<String, Object>>}
	 */
	private ProducerRecord<String, Map<String, Object>> makeRecord(String topic, Map<String, Object> value) {
		if (StringUtils.isEmpty(topic)) {
			throw new IllegalArgumentException("destination topic should not be empty");
		}
		return new ProducerRecord<>(topic, null, null, null, value, null);
	}

	/**
	 * Creates a reply record for received record.
	 * In terms of Kafka - creates ProducerRecord based on data from ConsumerRecord (headers, topics, keys)
	 * Currently only headers are reused.
	 *
	 * @param destinationTopic String
	 * @param value            {@code Map<String, Object> value}
	 * @param record           {@code ConsumerRecord<String, Map<String, Object>>}
	 * @return {@code ProducerRecord<String, Map<String, Object>>}
	 */
	private ProducerRecord<String, Map<String, Object>> makeReplyRecord(
			String destinationTopic, Map<String, Object> value, ConsumerRecord<String, Map<String, Object>> record) {
		return new ProducerRecord<>(destinationTopic, null, null, null, value, record.headers());
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public KafkaTemplate<String, Map<String, Object>> getKafkaTemplate() {
		return kafkaTemplate;
	}

	public void setKafkaTemplate(KafkaTemplate<String, Map<String, Object>> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	public int getRequestTimeout() {
		return requestTimeout;
	}

	public void setRequestTimeout(int requestTimeout) {
		this.requestTimeout = requestTimeout;
	}
}
