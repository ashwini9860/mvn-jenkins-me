package com.alice.aef.registry;

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ServiceRegistry {

	private Map<String, ServiceDescriptor> services;

	public ServiceRegistry() {
		services = new HashMap<>();
	}

	public void init() {
		YamlPropertiesFactoryBean bean = new YamlPropertiesFactoryBean();
		bean.setResources(new ClassPathResource("service-registry.yaml"));
		Properties props = bean.getObject();
		for (Map.Entry<Object, Object> entry : props.entrySet()) {
			String[] key = entry.getKey().toString().split("\\.");
			String serviceName = standardize(key[0]);
			String methodName = standardize(key[1]);
			String topicName = standardize(key[2]);
			if (!services.containsKey(serviceName))
				services.put(serviceName, new ServiceDescriptor());
			ServiceDescriptor service = services.get(serviceName);
			if (!service.containsMethod(methodName))
				service.addMethodMapping(methodName, new MethodMapping());
			MethodMapping methodMapping = service.getMethodMapping(methodName);
			String topic = entry.getValue().toString();
			switch (topicName) {
				case "requesttopic":
					methodMapping.setRequestTopic(topic);
					break;
				case "responsetopic":
					methodMapping.setResponseTopic(topic);
					break;
				case "errortopic":
					methodMapping.setErrorTopic(topic);
					break;
			}
		}
	}

	public String getRequestTopic(String service, String method) {
		return services.get(standardize(service))
				.getMethodMapping(standardize(method))
				.getRequestTopic();
	}

	public String getResponseTopic(String service, String method) {
		return services.get(standardize(service))
				.getMethodMapping(standardize(method))
				.getResponseTopic();
	}

	public String getErrorTopic(String service, String method) {
		return services.get(standardize(service))
				.getMethodMapping(standardize(method))
				.getErrorTopic();
	}

	private String standardize(String input) {
		return input.replace("-", "").toLowerCase();
	}

	private class ServiceDescriptor {
		private Map<String, MethodMapping> methods;

		private ServiceDescriptor() {
			methods = new HashMap<>();
		}

		private boolean containsMethod(String name) {
			return methods.containsKey(name);
		}

		private void addMethodMapping(String name, MethodMapping mapping) {
			methods.put(name, mapping);
		}

		private MethodMapping getMethodMapping(String name) {
			return methods.get(name);
		}

	}

	private class MethodMapping {
		private String requestTopic;
		private String responseTopic;
		private String errorTopic;

		private String getRequestTopic() {
			return requestTopic;
		}

		void setRequestTopic(String requestTopic) {
			this.requestTopic = requestTopic;
		}

		private String getResponseTopic() {
			return responseTopic;
		}

		void setResponseTopic(String responseTopic) {
			this.responseTopic = responseTopic;
		}

		private String getErrorTopic() {
			return errorTopic;
		}

		void setErrorTopic(String errorTopic) {
			this.errorTopic = errorTopic;
		}
	}

}
