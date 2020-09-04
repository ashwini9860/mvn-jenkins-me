package com.alice.aef.client;

import com.alice.aef.client.producer.AEFClient;
import com.alice.aef.dispatcher.AEFDispatcher;
import com.alice.aef.registry.ServiceRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

@Configuration
@EnableKafka
public class AEFConfiguration {

	@Autowired
	private ConsumerFactory<String, Map<String, Object>> consumerFactory;

	@Autowired
	private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

	@Bean
	public AEFClient client() {
		return new AEFClient();
	}

	@Bean
	public ServiceRegistry serviceRegistry() {
		ServiceRegistry serviceRegistry = new ServiceRegistry();
		serviceRegistry.init();
		return serviceRegistry;
	}

	@Bean
	public AEFDispatcher aefDispatcher() {
		return new AEFDispatcher();
	}
}
