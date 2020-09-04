package com.alice.aef.client.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.RecordInterceptor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Event fabric client designed to subscribe to a topic and receive events from it.
 * <p>
 * Important note is that kafka consumer group id is mandatory - this way all beans with the same
 * consumer group id on all instances will belong to the same consumer group.
 * <p>
 * Topics and message listener should be specified during construction.
 */
public class AEFConsumerClient implements InitializingBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(AEFConsumerClient.class);

	private String consumerGroupId;

	private List<String> topics;

	// optional record interceptor (used for tracing propagation)
	@Autowired(required = false)
	private RecordInterceptor recordInterceptor;

	@Autowired
	private ConsumerFactory<String, Map<String, Object>> consumerFactory;

	// message listener
	private MessageListener<String, Map<String, Object>> messageListener;

	private KafkaMessageListenerContainer<String, Map<String, Object>> kafkaMessageListenerContainer;

	public AEFConsumerClient(String consumerGroupId,
	                         List<String> topics, MessageListener<String,
			Map<String, Object>> messageListener) {
		this.consumerGroupId = consumerGroupId;
		this.topics = topics;
		this.messageListener = messageListener;
	}

	@Override
	public void afterPropertiesSet() {

		List<String> validTopics = topics.stream()
				.filter(Objects::nonNull)
				.distinct().collect(Collectors.toList());

		ContainerProperties containerProperties = new ContainerProperties(validTopics.toArray(new String[]{}));

		// set Kafka consumer group id to bean name so that all beans with the same name from all servers
		// will be in the same consumer group
		containerProperties.setGroupId(consumerGroupId);
		// can be a performance issue as we acknowledge each record
		containerProperties.setAckMode(ContainerProperties.AckMode.RECORD);

		LOGGER.debug("Subscribe to topics [{}] with consumer group id [{}]", validTopics, consumerGroupId);
		// create kafka message container
		kafkaMessageListenerContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
		kafkaMessageListenerContainer.setupMessageListener(messageListener);
		if (recordInterceptor != null) {
			kafkaMessageListenerContainer.setRecordInterceptor(recordInterceptor);
		}
		kafkaMessageListenerContainer.start();
	}

	public String getConsumerGroupId() {
		return consumerGroupId;
	}

	public List<String> getTopics() {
		return topics;
	}

	public void setConsumerFactory(ConsumerFactory<String, Map<String, Object>> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	public KafkaMessageListenerContainer<String, Map<String, Object>> getKafkaMessageListenerContainer() {
		return kafkaMessageListenerContainer;
	}
}