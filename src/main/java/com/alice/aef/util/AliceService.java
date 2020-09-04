package com.alice.aef.util;

import com.alice.aef.client.producer.AEFClient;
import com.alice.aef.registry.ServiceRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;

/**
 * Super class for every alice service that contains common methods
 */
public abstract class AliceService {
  private static final Logger log = LoggerFactory.getLogger(AliceService.class);

  @Autowired
  protected AEFClient client;

  @Autowired
  protected ServiceRegistry serviceRegistry;

  /**
   * Helper method that checks the existence of the parameter with the given name
   * @param name the name of the parameter
   * @param record the map of record in which to look
   * @return the parameter if found, null if otherwise
   */
  protected Object getParameter(String name, ConsumerRecord<String, Map<String, Object>> record) {
    if (!record.value().containsKey(name)) {
      log.error("Parameter \"{}\" not found", name);
      return null;
    }
    return record.value().get(name);
  }

  /**
   * Helper method that creates a Map of parameters, based on the values passed.
   * @param values varargs parameter; should contain an even number of values, the odd ones being the parameter names and the even ones being the parameter
   *               values
   * @return the map containing the given parameters
   */
  protected Map<String, Object> makeParams(Object... values) {
    Map<String, Object> params = new HashMap<>();
    for (int i = 0; i < values.length; i += 2)
      params.put(values[i].toString(), values[i + 1]);
    return params;
  }
}
