package com.alice.aef.client.consumer;


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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@DirtiesContext
@RunWith(SpringRunner.class)
@EmbeddedKafka(

        topics = {
                AEFConsumerClientTest.TOPIC_1,
                AEFConsumerClientTest.TOPIC_2,
                AEFConsumerClientTest.TOPIC_3,
                AEFConsumerClientTest.TOPIC_4,
                AEFConsumerClientTest.TOPIC_5,
                AEFConsumerClientTest.TOPIC_6,
                AEFConsumerClientTest.MULTIPLE_TOPICS_1,
                AEFConsumerClientTest.MULTIPLE_TOPICS_2},
        ports = {AEFConsumerClientTest.KAFKA_PORT},
        brokerProperties = {
                "session.timeout.ms=5000",              //making session to expire sooner/ default value of 30s is jut too long
                "group.min.session.timeout.ms=5000",    //making session to expire sooner/ default value of 30s is jut too long
                "group.max.session.timeout.ms=5500"})
public class AEFConsumerClientTest {

    static final String TOPIC_1 = "topic1";
    static final String TOPIC_2 = "topic2";
    static final String TOPIC_3 = "topic3";
    static final String TOPIC_4 = "topic4";
    static final String TOPIC_5 = "topic5";
    static final String TOPIC_6 = "topic6";
    static final String MULTIPLE_TOPICS_1 = "multipleTopics1";
    static final String MULTIPLE_TOPICS_2 = "multipleTopics2";

    static final int KAFKA_PORT = 9557;
    private static final String KAFKA_SERVER = "localhost:";

    private static final String TEST_DATA = "testData";
    private static final String CONSUMER_GROUP_ID = "consumerClientGroupId";

    @Autowired
    private KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, Map<String, Object>> consumerFactory;

    @Autowired
    private EmbeddedKafkaBroker broker;

    // latch to count messages
    private CountDownLatch latch;

    // queue to store received messages
    private BlockingQueue<ConsumerRecord<String, Map<String, Object>>> receivedMessages = new LinkedBlockingQueue<>();

    @Test
    public void testConsumerSubscriptionWithSingleMessage() throws Exception {

        AEFConsumerClient consumer1 = createConsumerClientAndSubscribeToKafka(Lists.list(TOPIC_1), CONSUMER_GROUP_ID);
        // wait so that kafka listener gets assigned kafka partitions and can receive messages
        ContainerTestUtils.waitForAssignment(consumer1.getKafkaMessageListenerContainer(),
                broker.getPartitionsPerTopic());//(Embedded kafka has 2 partitions by default)

        latch = new CountDownLatch(1);
        sendTestMessages(TOPIC_1, 1);
        latch.await(5000, TimeUnit.MILLISECONDS);

        // assert that single message was received
        assertThat(receivedMessages.size()).isEqualTo(1);
    }

    @Test
    public void testConsumerSubscriptionWithMultipleMessages() throws Exception {

        AEFConsumerClient consumer1 = createConsumerClientAndSubscribeToKafka(Lists.list(TOPIC_2), CONSUMER_GROUP_ID);
        // wait so that kafka listener gets assigned kafka partitions and can receive messages
        ContainerTestUtils.waitForAssignment(consumer1.getKafkaMessageListenerContainer(),
                broker.getPartitionsPerTopic());//(Embedded kafka has 2 partitions by default)

        latch = new CountDownLatch(42);
        sendTestMessages(TOPIC_2, 42);
        latch.await(5000, TimeUnit.MILLISECONDS);

        // assert that we received all messages
        assertThat(receivedMessages.size()).isEqualTo(42);
    }

    /**
     * Send to kafka messages and make sure that they are received by one of the consumers (same consumer group)
     *
     * @throws Exception in case of an error
     */
    @Test
    public void testConsumerSubscriptionTwoConsumersInSingleConsumerGroup() throws Exception {


        AEFConsumerClient consumer1 = createConsumerClientAndSubscribeToKafka(Lists.list(TOPIC_3), CONSUMER_GROUP_ID);
        // wait so that kafka listener gets assigned kafka partitions and can receive messages
        ContainerTestUtils.waitForAssignment(consumer1.getKafkaMessageListenerContainer(),
                broker.getPartitionsPerTopic());//(Embedded kafka has 2 partitions by default)

        // create second consumer in the same consumer group id
        AEFConsumerClient consumer2 = createConsumerClientAndSubscribeToKafka(Lists.list(TOPIC_3), CONSUMER_GROUP_ID);
        // consumer2 will be assigned only one partition as the other one will be assigned to consumer1
        //(Embedded kafka has 2 partitions by default)
        ContainerTestUtils.waitForAssignment(consumer2.getKafkaMessageListenerContainer(), 1);

        // send to kafka 42 messages and make sure that they are received by one of the consumers
        latch = new CountDownLatch(42);
        sendTestMessages(TOPIC_3,42);
        latch.await(5000, TimeUnit.MILLISECONDS);

        assertThat(receivedMessages.size()).isEqualTo(42);
    }

    /**
     * Send to kafka messages and make sure that they are received by both consumers
     * (they are in different consumer groups)
     *
     * @throws Exception in case of an error
     */
    @Test
    public void testConsumerSubscriptionTwoConsumersInDifferentConsumerGroup() throws Exception {

        AEFConsumerClient consumer1 = createConsumerClientAndSubscribeToKafka(Lists.list(TOPIC_4), CONSUMER_GROUP_ID);
        // wait so that kafka listener gets assigned kafka partitions and can receive messages
        ContainerTestUtils.waitForAssignment(consumer1.getKafkaMessageListenerContainer(),
                broker.getPartitionsPerTopic());//(Embedded kafka has 2 partitions by default)

        AEFConsumerClient consumer2 = createConsumerClientAndSubscribeToKafka(Lists.list(TOPIC_4),
                "consumerClientDifferentConsumerGroup");
        // wait so that kafka listener gets assigned kafka partitions and can receive messages
        ContainerTestUtils.waitForAssignment(consumer2.getKafkaMessageListenerContainer(),
                broker.getPartitionsPerTopic());//(Embedded kafka has 2 partitions by default)


        // send to kafka 42 messages and they should be received by
        // both consumers (duplicated)
        // thus received messages should be 42 * 2 = 84
        latch = new CountDownLatch(84);
        sendTestMessages(TOPIC_4, 42);
        latch.await(5000, TimeUnit.MILLISECONDS);

        assertThat(receivedMessages.size()).isEqualTo(84);
    }

    @Test
    public void testConsumerSubscriptionWithMultipleMessagesSentConcurrently() throws Exception {

        AEFConsumerClient consumer1 = createConsumerClientAndSubscribeToKafka(Lists.list(TOPIC_5), CONSUMER_GROUP_ID);
        // wait so that kafka listener gets assigned kafka partitions and can receive messages
        ContainerTestUtils.waitForAssignment(consumer1.getKafkaMessageListenerContainer(),
                broker.getPartitionsPerTopic());//(Embedded kafka has 2 partitions by default)

        latch = new CountDownLatch(300);
        // send messages from multiple threads
        ExecutorService executorService = Executors.newFixedThreadPool(7);
        IntStream.range(0, 30)
                .forEach(i -> executorService.submit(() -> sendTestMessages(TOPIC_5,10)));
        executorService.shutdown();
        executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);

        latch.await(20000, TimeUnit.MILLISECONDS);
        assertThat(receivedMessages.size()).isEqualTo(300);
    }

    /**
     * Test that is testing kafka re-balancing
     * 1. Create two consumers (same consumer group)
     * 2. They will get one partition each (Embedded kafka has 2 partitions by default)
     * 3. Send/receive some messages
     * 4. Stop one consumer
     * 5. Remaining consumer1 will be reassigned both partitions
     * 6. Send/receive some more messages
     *
     * @throws Exception in case of an error
     */
    @Test
    public void testConsumerSubscriptionWithKafkaRebalancing() throws Exception {

        // first consumer - gets two partitions
        AEFConsumerClient consumer1 = createConsumerClientAndSubscribeToKafka(Lists.list(TOPIC_6), CONSUMER_GROUP_ID);
        // wait so that kafka listener gets assigned kafka partitions and can receive messages
        ContainerTestUtils.waitForAssignment(consumer1.getKafkaMessageListenerContainer(),
                broker.getPartitionsPerTopic());


        // second consumer - two partitions are split to each consumer
        AEFConsumerClient consumer2 = createConsumerClientAndSubscribeToKafka(Lists.list(TOPIC_6), CONSUMER_GROUP_ID);
        // wait so that kafka listener gets assigned kafka partitions and can receive messages
        ContainerTestUtils.waitForAssignment(consumer2.getKafkaMessageListenerContainer(),1);


        // send 42 messages and they should be received once each
        latch = new CountDownLatch(42);
        sendTestMessages(TOPIC_6, 42);
        latch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(receivedMessages.size()).isEqualTo(42);

        // stop second consumer - so consumer1 should be reassigned oth partitions by kafka
        consumer2.getKafkaMessageListenerContainer().stop();
        ContainerTestUtils.waitForAssignment(consumer1.getKafkaMessageListenerContainer(),
                broker.getPartitionsPerTopic());

        // what happens here is a bit
        latch = new CountDownLatch(20);
        receivedMessages.clear();
        sendTestMessages(TOPIC_6, 20);
        latch.await(5000, TimeUnit.MILLISECONDS);
        assertThat(receivedMessages.size()).isEqualTo(20);
    }

    @Test
    public void testConsumerSubscriptionForMultipleTopics() throws Exception {

        // create consumer for two topics
        AEFConsumerClient consumer1 = createConsumerClientAndSubscribeToKafka(
                Lists.list(MULTIPLE_TOPICS_1, MULTIPLE_TOPICS_2), CONSUMER_GROUP_ID);
        // wait so that kafka listener gets assigned kafka partitions and can receive messages
        ContainerTestUtils.waitForAssignment(consumer1.getKafkaMessageListenerContainer(),
                broker.getPartitionsPerTopic() * 2);// each topic has 2 partitions

        // send messages to both topics
        latch = new CountDownLatch(150);
        sendTestMessages(MULTIPLE_TOPICS_1, 100);
        sendTestMessages(MULTIPLE_TOPICS_2, 50);
        latch.await(5000, TimeUnit.MILLISECONDS);

        assertThat(receivedMessages.size()).isEqualTo(150);
    }

    private AEFConsumerClient createConsumerClientAndSubscribeToKafka(List<String> topics, String consumerGroupId) {

        AEFConsumerClient consumerClient = new AEFConsumerClient(consumerGroupId, topics, record -> {
            System.out.println("Received message with key " + record.key() + " and value " + record.value());
            receivedMessages.add(record);
            latch.countDown();
        });

        consumerClient.setConsumerFactory(consumerFactory);
        consumerClient.afterPropertiesSet();

        return consumerClient;
    }

    private void sendTestMessages(String topic, int numberOfMessages) {
        IntStream.rangeClosed(1, numberOfMessages).forEach(i -> {
            Map<String, Object> data = createTestMessageData(String.valueOf(i));
            kafkaTemplate.send( new ProducerRecord<>(topic, null, null, null, data, null));
        });
    }

    private Map<String, Object> createTestMessageData(String key) {
        return ImmutableMap.of(
                key, TEST_DATA + key,
                "thread-id", Thread.currentThread().getId());
    }

    @EnableKafka
    @TestConfiguration
    static class AEFConsumerClientTestConfiguration {

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
            // making session to expire sooner/ default value of 30s is jut too long
            // used during re-balancing test - kafka consumer drops off, but re-balancing is done only after his
            // session is expired (after session timeout config)
            consumerConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 5000);
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
