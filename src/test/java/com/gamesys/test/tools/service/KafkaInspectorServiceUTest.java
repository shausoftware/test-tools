package com.gamesys.test.tools.service;

import com.gamesys.test.tools.utils.TestData;
import com.gamesys.test.tools.domain.KafkaQuery;
import com.gamesys.test.tools.domain.KafkaRecord;
import com.gamesys.test.tools.kafka.KafkaInspector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class KafkaInspectorServiceUTest {

    private static final String TOPIC = "test_topic";

    @Mock
    private KafkaInspector inspector;

    @InjectMocks
    private KafkaInspectorService kafkaInspectorService;

    @Test
    public void testFindRecordByKeySuccess() {
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("key1", "key1_value-b");
        queryParameters.put("key2", "key2_value-b");
        queryParameters.put("key3.keyPart", "key3_part_value-b");
        queryParameters.put("key4.keyPart", "key4_part_value-b");
        KafkaQuery query = KafkaQuery.builder()
                .topic(TOPIC)
                .query(queryParameters)
                .build();
        ConsumerRecords<String, String> consumerRecords = generateTestRecords();

        when(inspector.getRecords(TOPIC, 5000)).thenReturn(consumerRecords);
        Optional<KafkaRecord> result = kafkaInspectorService.findRecord(query, true);

        verify(inspector, times(1)).getRecords(TOPIC, 5000);
        verify(inspector, times(1)).close();
        assertTrue(result.isPresent());
        JSONObject root = new JSONObject(result.get().getValue());
        assertThat(root.getString("firstName"), is("Joe-b"));
    }

    @Test
    public void testFindRecordByValuesSuccess() {
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("firstName", "Joe-b");
        queryParameters.put("lastName", "Bloggs-b");
        queryParameters.put("address2.street", "road2-b");
        KafkaQuery query = KafkaQuery.builder()
                .topic(TOPIC)
                .query(queryParameters)
                .build();
        ConsumerRecords<String, String> consumerRecords = generateTestRecords();

        when(inspector.getRecords(TOPIC, 5000)).thenReturn(consumerRecords);
        Optional<KafkaRecord> result = kafkaInspectorService.findRecord(query, false);

        verify(inspector, times(1)).getRecords(TOPIC, 5000);
        verify(inspector, times(1)).close();
        assertTrue(result.isPresent());
        JSONObject root = new JSONObject(result.get().getValue());
        assertThat(root.getString("firstName"), is("Joe-b"));
    }

    @Test
    public void testFindRecordByValuesNotFound() {
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("firstName", "Joe-b");
        queryParameters.put("lastName", "Biggs-b");
        queryParameters.put("address2.street", "road2-b");
        KafkaQuery query = KafkaQuery.builder()
                .topic(TOPIC)
                .query(queryParameters)
                .build();
        ConsumerRecords<String, String> consumerRecords = generateTestRecords();

        when(inspector.getRecords(TOPIC, 5000)).thenReturn(consumerRecords).thenReturn(ConsumerRecords.empty());
        Optional<KafkaRecord> result = kafkaInspectorService.findRecord(query, false);

        verify(inspector, times(2)).getRecords(TOPIC, 5000);
        verify(inspector, times(1)).close();
        assertFalse(result.isPresent());
    }

    @Test
    public void testFindRecordByValuesInvalidFieldName() {
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("firstName", "Joe-b");
        queryParameters.put("lastName", "Bloggs-b");
        queryParameters.put("address2.i_do_not_exist", "road2-b");
        KafkaQuery query = KafkaQuery.builder()
                .topic(TOPIC)
                .query(queryParameters)
                .build();
        ConsumerRecords<String, String> consumerRecords = generateTestRecords();

        when(inspector.getRecords(TOPIC, 5000)).thenReturn(consumerRecords).thenReturn(ConsumerRecords.empty());
        Optional<KafkaRecord> result = kafkaInspectorService.findRecord(query, false);

        verify(inspector, times(2)).getRecords(TOPIC, 5000);
        verify(inspector, times(1)).close();
        assertFalse(result.isPresent());
    }

    private ConsumerRecords<String, String> generateTestRecords() {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> recordMap = new HashMap<>();
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        records.add(new ConsumerRecord<>(TOPIC,
                0,
                123L,
                TestData.generateTestKeyJson("a"),
                TestData.generateTestRecordJson("a")));
        records.add(new ConsumerRecord<>(TOPIC,
                0,
                124L,
                TestData.generateTestKeyJson("b"),
                TestData.generateTestRecordJson("b")));
        records.add(new ConsumerRecord<>(TOPIC,
                0,
                125L,
                TestData.generateTestKeyJson("c"),
                TestData.generateTestRecordJson("c")));
        recordMap.put(null, records);
        return new ConsumerRecords<>(recordMap);
    }
}