package com.alice.aef.dispatcher;

import com.alice.aef.exception.AEFException;
import com.alice.aef.registry.ServiceConstants;
import com.alice.aef.transport.AEFRequest;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class AEFDispatcher {
	private Map<Class<?>, Consumer<ConsumerRecord<String, Map<String, Object>>>> consumers;

	public AEFDispatcher() {
		consumers = new HashMap<>();
	}

	public void registerConsumer(Class<?> requestClass, Consumer<ConsumerRecord<String, Map<String, Object>>> consumer) {
		consumers.put(requestClass, consumer);
	}

	public void dispatch(ConsumerRecord<String, Map<String, Object>> record) {
		AEFRequest<?> request = (AEFRequest<?>) record.value().get(ServiceConstants.REQUEST);
		if (request == null)
			throw new AEFException("Request parameter is required!");
		Consumer<ConsumerRecord<String, Map<String, Object>>> consumer = consumers.get(request.getRequestObject().getClass());
		if (consumer == null)
			throw new AEFException("Consumer for [" + request.getRequestObject().getClass() + "] not registered!");
		consumer.accept(record);
	}
}
