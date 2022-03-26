package com.gamesys.test.tools;

import com.gamesys.test.tools.domain.KafkaQuery;
import com.gamesys.test.tools.domain.KafkaRecord;
import com.gamesys.test.tools.kafka.KafkaInspector;
import com.gamesys.test.tools.kafka.Konfig;
import com.gamesys.test.tools.service.KafkaInspectorService;
import com.gamesys.test.tools.utils.TestConsumer;
import com.gamesys.test.tools.utils.TestData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@SpringBootTest
@Testcontainers
@DirtiesContext
public class ApplicationTests {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

    @Autowired
    public KafkaTemplate<String, String> template;

    @Autowired
    private TestConsumer testConsumer;

    @Autowired
    private Konfig konfig;
    private KafkaInspectorService kafkaInspectorService;

    @Value("${test.topic}")
    private String topic;

    @BeforeEach
    public void setupTest() {
        konfig.setKafkaBrokerAddress(kafka.getBootstrapServers());
        kafkaInspectorService = new KafkaInspectorService(new KafkaInspector(konfig));
    }

    @Test
    public void testSearch() throws Exception {
        //put messages on topic
        String key = TestData.generateTestKeyJson("a");
        String payload = TestData.generateTestRecordJson("a");
        template.send(topic, key, payload);
        key = TestData.generateTestKeyJson("b");
        payload = TestData.generateTestRecordJson("b");
        template.send(topic, key, payload);
        key = TestData.generateTestKeyJson("c");
        payload = TestData.generateTestRecordJson("c");
        template.send(topic, key, payload);

        //consume messages before query
        testConsumer.getLatch().await(10000, TimeUnit.MILLISECONDS);
        assertThat(testConsumer.getLatch().getCount(), equalTo(0L));
        List<String> result = testConsumer.getPayload();
        assertThat(result.size(), is(3));
        assertThat(result.get(0).toString(), containsString("Joe-a"));
        assertThat(result.get(1).toString(), containsString("Joe-b"));
        assertThat(result.get(2).toString(), containsString("Joe-c"));

        //find by query
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("key1", "key1_value-b");
        queryParameters.put("key2", "key2_value-b");
        queryParameters.put("key3.keyPart", "key3_part_value-b");
        queryParameters.put("key4.keyPart", "key4_part_value-b");
        KafkaQuery query = KafkaQuery.builder()
                .topic(topic)
                .query(queryParameters)
                .build();
        Optional<KafkaRecord> record = kafkaInspectorService.findRecord(query, true);
        assertThat(record.isPresent(), is(true));
        assertThat(record.get().getValue(), containsString("Joe-b"));

        //add some more messages
        key = TestData.generateTestKeyJson("d");
        payload = TestData.generateTestRecordJson("d");
        template.send(topic, key, payload);
        key = TestData.generateTestKeyJson("e");
        payload = TestData.generateTestRecordJson("e");
        template.send(topic, key, payload);

        //find by query
        queryParameters = new HashMap<>();
        queryParameters.put("key1", "key1_value-d");
        queryParameters.put("key2", "key2_value-d");
        queryParameters.put("key3.keyPart", "key3_part_value-d");
        queryParameters.put("key4.keyPart", "key4_part_value-d");
        query = KafkaQuery.builder()
                .topic(topic)
                .query(queryParameters)
                .build();
        record = kafkaInspectorService.findRecord(query, true);
        assertThat(record.isPresent(), is(true));
        assertThat(record.get().getValue(), containsString("Joe-d"));

        //test messages have not been consumed
        result = testConsumer.getPayload();
        assertThat(result.size(), is(5));
        assertThat(result.get(0).toString(), containsString("Joe-a"));
        assertThat(result.get(1).toString(), containsString("Joe-b"));
        assertThat(result.get(2).toString(), containsString("Joe-c"));
        assertThat(result.get(3).toString(), containsString("Joe-d"));
        assertThat(result.get(4).toString(), containsString("Joe-e"));
    }

    @TestConfiguration
    static class KafkaTestContainersConfiguration {

        @Value("${test.consumer-group}")
        private String defaultGroupId;

        @Bean
        ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory());
            return factory;
        }

        @Bean
        public ConsumerFactory<Integer, String> consumerFactory() {
            return new DefaultKafkaConsumerFactory<>(consumerConfigs());
        }

        @Bean
        public Map<String, Object> consumerConfigs() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, defaultGroupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            return props;
        }

        @Bean
        public ProducerFactory<String, String> producerFactory() {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return new DefaultKafkaProducerFactory<>(configProps);
        }

        @Bean
        public KafkaTemplate<String, String> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }
    }
}
