package com.alice.aef.client.requestreply;

import com.alice.aef.serialization.MapDeserializer;
import com.alice.aef.serialization.MapSerializer;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka(
        topics = {
                AEFRequestReplyClientTest.REQUEST_TOPIC_1,
                AEFRequestReplyClientTest.REQUEST_TOPIC_2,
                AEFRequestReplyClientTest.REQUEST_TOPIC_3,
                AEFRequestReplyClientTest.REPLY_TOPIC_1,
                AEFRequestReplyClientTest.REPLY_TOPIC_2,
                AEFRequestReplyClientTest.REPLY_TOPIC_3,
                AEFRequestReplyClientTest.REPLY_TOPIC_4},
        ports = {AEFRequestReplyClientTest.KAFKA_PORT})
public class AEFRequestReplyClientTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AEFRequestReplyClientTest.class);

    static final int KAFKA_PORT = 9558;
    private static final String KAFKA_SERVER = "localhost:";

    static final String REQUEST_TOPIC_1 = "requestTopic1";
    static final String REQUEST_TOPIC_2 = "requestTopic2";
    static final String REQUEST_TOPIC_3 = "requestTopic3";
    static final String REPLY_TOPIC_1 = "replyTopic1";
    static final String REPLY_TOPIC_2 = "replyTopic2";
    static final String REPLY_TOPIC_3 = "replyTopic3";
    static final String REPLY_TOPIC_4 = "replyTopic4";
    private static final String CONSUMER_GROUP_ID = "requestReplyClient";

    private AEFRequestReplyClient requestReplyClient;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

    @Autowired
    private ProducerFactory<String, Map<String, Object>> producerFactory;

    @Autowired
    private ConsumerFactory<String, Map<String, Object>> consumerFactory;

    @Test
    public void testSendAndReceiveSingleMessage() {

        subscribeRequestReplyClient(Lists.list(REPLY_TOPIC_1));

        createAFakeThirdpartyService(REQUEST_TOPIC_1, REPLY_TOPIC_1);


        Map<String, Object> params = new HashMap<>();
        params.put("testKey", 123);
        ConsumerRecord<String, Map<String, Object>> replyRecord = requestReplyClient.sendAndReceive(REQUEST_TOPIC_1, params);

        assertThat(replyRecord).isNotNull();
        assertThat(replyRecord.value().containsKey("testKey")).isTrue();
        assertThat(replyRecord.value().get("testKey")).isEqualTo(123);

        assertThat(replyRecord.value().containsKey("replyData")).isTrue();
        assertThat(replyRecord.value().get("replyData")).isEqualTo("replyValue");
    }

    private void createAFakeThirdpartyService(String topic, String replyTopic) {
        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setGroupId("kafkaMessageListenerContainer");
        KafkaMessageListenerContainer kafkaMessageListenerContainer =
                new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

        kafkaMessageListenerContainer.setupMessageListener((MessageListener<String, Map<String, Object>>) record -> {
            LOGGER.info("Received message with key {} and value {}" ,record.key(), record.value());
            sendReply(replyTopic, record);
        });
        kafkaMessageListenerContainer.start();

        ContainerTestUtils.waitForAssignment(kafkaMessageListenerContainer, broker.getPartitionsPerTopic());
    }

    private void createAFakeThirdpartyServiceThatWritesToDifferentTopics(String topic) {
        ContainerProperties containerProperties = new ContainerProperties(REQUEST_TOPIC_3);
        containerProperties.setGroupId("kafkaMessageListenerContainer");
        KafkaMessageListenerContainer kafkaMessageListenerContainer  =
                new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        kafkaMessageListenerContainer.setupMessageListener(
                (MessageListener<String, Map<String, Object>>) record -> {
                    LOGGER.info("Received message with key {} and value {}" , record.key(), record.value());
                    Integer testKey = Integer.valueOf(record.value().get("testKey").toString());
                    if (testKey % 2 == 0) {
                        sendReply(REPLY_TOPIC_3, record);
                    } else {
                        sendReply(REPLY_TOPIC_4, record);
                    }
                });
        kafkaMessageListenerContainer.start();
        ContainerTestUtils.waitForAssignment(kafkaMessageListenerContainer, broker.getPartitionsPerTopic());
    }

    @Test
    public void testSendAndReceiveMultipleMessages() {

        subscribeRequestReplyClient(Lists.list(REPLY_TOPIC_2));

        createAFakeThirdpartyService(REQUEST_TOPIC_2, REPLY_TOPIC_2);

        IntStream
                .range(0, 30)
                .forEach( i -> {
                    Map<String, Object> params = new HashMap<>();
                    params.put("testKey", i);
                    ConsumerRecord<String, Map<String, Object>> replyRecord =
                            requestReplyClient.sendAndReceive(REQUEST_TOPIC_2, params);
                    assertThat(replyRecord).isNotNull();
                    assertThat(replyRecord.value().containsKey("testKey")).isTrue();
                    assertThat(replyRecord.value().get("testKey")).isEqualTo(i);

                    assertThat(replyRecord.value().containsKey("replyData")).isTrue();
                    assertThat(replyRecord.value().get("replyData")).isEqualTo("replyValue");
                });
    }

    @Test
    public void testSendAndReceiveMultipleMessagesFromTwoTopics() {

        subscribeRequestReplyClient(Lists.list(REPLY_TOPIC_3, REPLY_TOPIC_4));

        createAFakeThirdpartyServiceThatWritesToDifferentTopics(REQUEST_TOPIC_3);

        IntStream
                .range(0, 30)
                .forEach(i -> {
                    Map<String, Object> params = new HashMap<>();
                    params.put("testKey", i);
                    ConsumerRecord<String, Map<String, Object>> replyRecord =
                            requestReplyClient.sendAndReceive(REQUEST_TOPIC_3, params);
                    assertThat(replyRecord).isNotNull();
                    assertThat(replyRecord.value().containsKey("testKey")).isTrue();
                    assertThat(replyRecord.value().get("testKey")).isEqualTo(i);

                    assertThat(replyRecord.value().containsKey("replyData")).isTrue();
                    assertThat(replyRecord.value().get("replyData")).isEqualTo("replyValue");
                });
    }

    private void subscribeRequestReplyClient(List<String> topics) {
        requestReplyClient = new AEFRequestReplyClient(CONSUMER_GROUP_ID, topics);
        requestReplyClient.setConsumerFactory(consumerFactory);
        requestReplyClient.setProducerFactory(producerFactory);
        requestReplyClient.afterPropertiesSet();
    }

    private void sendReply(String topic, ConsumerRecord<String, Map<String, Object>> record) {
        Map<String, Object> value = record.value();
        value.put("replyData", "replyValue");

        ProducerRecord<String, Map<String, Object>> producerRecord =
                new ProducerRecord<>(topic, null , record.key(), value, record.headers());
        kafkaTemplate.send(producerRecord);

        ProducerRecord<String, Map<String, Object>> randomMessage = new ProducerRecord<>(topic, null ,
                null, ImmutableMap.of("randomMessageKey","Some random message"));
        kafkaTemplate.send(randomMessage);
    }

    @EnableKafka
    @TestConfiguration
    static class AEFRequestReplyClientTestConfiguration {

        @Bean
        public ConsumerFactory<String, Map<String, Object>> consumerFactory() {
            return new DefaultKafkaConsumerFactory<>(consumerConfig());
        }

        @Bean
        public Map<String, Object> consumerConfig() {
            Map<String, Object> consumerConfig = new HashMap<>();
            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER + KAFKA_PORT);
            consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MapDeserializer.class);
            consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return consumerConfig;
        }

        @Bean
        public ProducerFactory<String, Map<String, Object>> producerFactory() {
            return new DefaultKafkaProducerFactory<>(producerConfig());
        }

        @Bean
        public Map<String, Object> producerConfig() {
            Map<String, Object> producerConfig = new HashMap<>();
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER + KAFKA_PORT);
            producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MapSerializer.class);
            return producerConfig;
        }

        @Bean
        public KafkaTemplate<String, Map<String, Object>> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }
    }
}