package com.alice.aef.client.producer;

import com.alice.aef.exception.AEFException;
import com.alice.aef.serialization.MapDeserializer;
import com.alice.aef.serialization.MapSerializer;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@DirtiesContext
@RunWith(SpringRunner.class)
@EmbeddedKafka(
        topics = {AEFClientTest.REQUEST_TOPIC_1, AEFClientTest.REQUEST_TOPIC_2},
        ports = {AEFClientTest.KAFKA_PORT})
public class AEFClientTest {

    private static final String TEST_DATA = "testData";
    private static final String CONSUMER_GROUP_ID = "aefClientTestGroup";

    static final String REQUEST_TOPIC_1 = "requestTopic1";
    static final String REQUEST_TOPIC_2 = "requestTopic2";

    static final int KAFKA_PORT = 9556;
    private static final String KAFKA_SERVER = "localhost:";

    @Autowired
    private AEFClient client;

    @Autowired
    private ConsumerFactory<String, Map<String, Object>> consumerFactory;

    private Consumer<String, Map<String, Object>> consumer;

    @Before
    public void setUp() {
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(Lists.list(REQUEST_TOPIC_1));
    }

    @Test
    public void testSendSingleMessage() {
        // send single message
        client.send(createTestMessageData("SingleMessage1"));

        ConsumerRecords<String, Map<String, Object>> records = KafkaTestUtils.getRecords(consumer);

        // test that messages were sent by receiving them via a consumer
        // check that message was received
        assertThat(records).isNotNull();
        assertThat(records.count()).isEqualTo(1);
        assertThat(records.iterator().next().value().get("SingleMessage1")).isEqualTo("testDataSingleMessage1");
    }

    @Test
    public void testSendMultipleMessages() {

        // send multiple messages
        sendTestMessages(200);

        ConsumerRecords<String, Map<String, Object>> records = KafkaTestUtils.getRecords(consumer);

        // test that messages were sent by receiving them via a consumer
        // check that all messages were received
        assertThat(records).isNotNull();
        assertThat(records.count()).isEqualTo(200);
    }

    @Test
    public void testSendMultipleMessagesConcurrently() throws Exception {

        // send multiple messages from different threads
        ExecutorService executorService = Executors.newFixedThreadPool(7);
        IntStream.range(0, 30)
                .forEach(i -> executorService.submit(() -> sendTestMessages(10)));
        executorService.shutdown();
        executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);

        ConsumerRecords<String, Map<String, Object>> records = KafkaTestUtils.getRecords(consumer);

        // test that messages were sent by receiving them via a consumer
        // check that all messages were received
        assertThat(records).isNotNull();
        assertThat(records.count()).isEqualTo(300);
    }

    @Test
    public void testSendMultipleMessagesToDifferentTopics() {

        // send messages to two different topics
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(Lists.list(REQUEST_TOPIC_1, REQUEST_TOPIC_2));

        IntStream.rangeClosed(1, 111).forEach(i -> {
            Map<String, Object> data = createTestMessageData(String.valueOf(i));
            client.send(REQUEST_TOPIC_1, data);
        });

        IntStream.rangeClosed(1, 111).forEach(i -> {
            Map<String, Object> data = createTestMessageData(String.valueOf(i));
            client.send(REQUEST_TOPIC_2, data);
        });

        ConsumerRecords<String, Map<String, Object>> records = KafkaTestUtils.getRecords(consumer);

        // test that messages were sent by receiving them via a consumer
        // check that all messages were received
        assertThat(records).isNotNull();
        assertThat(records.count()).isEqualTo(222);
    }

    @Test(expected = AEFException.class)
    public void testSendMessagesWithException() {

        // try sending to messages to a non existent kafka server
        Map<String, Object> configs = KafkaTestUtils.senderProps("localhost:42000");
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10);

        ProducerFactory<String, Map<String, Object>> wrongServerProducerFactory =
                new DefaultKafkaProducerFactory<>(configs);

        AEFClient wrongEFClient = new AEFClient(AEFClientTest.REQUEST_TOPIC_1);
        wrongEFClient.setKafkaTemplate(new KafkaTemplate<>(wrongServerProducerFactory));
        wrongEFClient.send(createTestMessageData("SingleMessageWithException1"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSendMessagesWithEmptyTopic() {
        client = new AEFClient();

        client.send(createTestMessageData("SingleMessageWithException1"));
    }

    private void sendTestMessages(int numberOfMessages) {
        IntStream.rangeClosed(1, numberOfMessages).forEach(i -> {
            Map<String, Object> data = createTestMessageData(String.valueOf(i));
            client.send(data);
        });
    }

    private Map<String, Object> createTestMessageData(String key) {
        return ImmutableMap.of(
                key, TEST_DATA + key,
                "thread-id", Thread.currentThread().getId());
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    @EnableKafka
    @TestConfiguration
    static class EFClientTestConfiguration {

        @Bean
        public AEFClient client() {
            return new AEFClient(AEFClientTest.REQUEST_TOPIC_1);
        }

        @Bean
        public ConsumerFactory<String, Map<String, Object>> consumerFactory() {
            return new DefaultKafkaConsumerFactory<>(consumerConfig());
        }

        @Bean
        public Map<String, Object> consumerConfig() {
            Map<String, Object> consumerConfig = new HashMap<>();
            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER + KAFKA_PORT);
            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
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
            producerConfig.put(ProducerConfig.RETRIES_CONFIG, 1);
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