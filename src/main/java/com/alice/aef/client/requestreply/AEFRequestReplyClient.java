package com.alice.aef.client.requestreply;

import com.alice.aef.exception.AEFException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.alice.aef.client.AEFConstants.DEFAULT_REQUEST_REPLY_TIMEOUT;

/**
 * Event fabric client designed to send events to kafka and subscribe/await a reply
 * <p>
 * Important note is that kafka consumer group id is mandatory - this way all beans with the same
 * consumer group id on all instances will belong to the same consumer group.
 * <p>
 * Reply topics should be specified during construction.
 */
public class AEFRequestReplyClient implements InitializingBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(AEFRequestReplyClient.class);

	private String consumerGroupId;

	private List<String> replyTopics;

	private int requestReplyTimeout = DEFAULT_REQUEST_REPLY_TIMEOUT;

	@Autowired
	private ProducerFactory<String, Map<String, Object>> producerFactory;

	@Autowired
	private ConsumerFactory<String, Map<String, Object>> consumerFactory;

	private ReplyingKafkaTemplate<String, Map<String, Object>, Map<String, Object>> replyingKafkaTemplate;

	public AEFRequestReplyClient(String consumerGroupId, List<String> replyTopics) {
		this.consumerGroupId = consumerGroupId;
		this.replyTopics = replyTopics;
	}

	public ConsumerRecord<String, Map<String, Object>> sendAndReceive(String destinationTopic,
	                                                                  Map<String, Object> data) {

		try {
			ProducerRecord<String, Map<String, Object>> producerRecord =
					new ProducerRecord<>(destinationTopic, null, null, null, data, null);

			RequestReplyFuture<String, Map<String, Object>, Map<String, Object>> requestReplyFuture =
					replyingKafkaTemplate.sendAndReceive(producerRecord);

			ConsumerRecord<String, Map<String, Object>> consumerRecord =
					requestReplyFuture.get(requestReplyTimeout, TimeUnit.MILLISECONDS);

			LOGGER.debug("Received data {}", consumerRecord);

			return consumerRecord;

		} catch (Exception ex) {
			throw new AEFException("Failed to send and receive", ex);
		}
	}

	@Override
	public void afterPropertiesSet() {

		ContainerProperties containerProperties = new ContainerProperties(replyTopics.stream()
				.filter(Objects::nonNull)
				.distinct()
				.toArray(String[]::new));

		containerProperties.setGroupId(consumerGroupId);

		KafkaMessageListenerContainer<String, Map<String, Object>> kafkaMessageListenerContainer =
				new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

		replyingKafkaTemplate = new ReplyingKafkaTemplate<>(producerFactory, kafkaMessageListenerContainer);
		replyingKafkaTemplate.setReplyTimeout(requestReplyTimeout);
		// same topic can be used by multiple clients/consumer groups
		replyingKafkaTemplate.setSharedReplyTopic(true);
		replyingKafkaTemplate.start();
	}

	public String getConsumerGroupId() {
		return consumerGroupId;
	}

	public List<String> getReplyTopics() {
		return replyTopics;
	}

	public void setReplyTopics(List<String> replyTopics) {
		this.replyTopics = replyTopics;
	}

	public int getRequestReplyTimeout() {
		return requestReplyTimeout;
	}

	public void setRequestReplyTimeout(int requestReplyTimeout) {
		this.requestReplyTimeout = requestReplyTimeout;
	}

	public ProducerFactory<String, Map<String, Object>> getProducerFactory() {
		return producerFactory;
	}

	public void setProducerFactory(ProducerFactory<String, Map<String, Object>> producerFactory) {
		this.producerFactory = producerFactory;
	}

	public ConsumerFactory<String, Map<String, Object>> getConsumerFactory() {
		return consumerFactory;
	}

	public void setConsumerFactory(ConsumerFactory<String, Map<String, Object>> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}
}
