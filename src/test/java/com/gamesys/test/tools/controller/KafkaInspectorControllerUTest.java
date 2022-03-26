package com.gamesys.test.tools.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gamesys.test.tools.utils.TestData;
import com.gamesys.test.tools.domain.KafkaQuery;
import com.gamesys.test.tools.domain.KafkaRecord;
import com.gamesys.test.tools.service.KafkaInspectorService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(KafkaInspectorController.class)
public class KafkaInspectorControllerUTest {

    private static final String TOPIC = "test_topic";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private KafkaInspectorService kafkaInspectorService;

    @Test
    public void testFindByKeySuccess() throws Exception {
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("key1", "key1_value-b");
        KafkaQuery query = KafkaQuery.builder()
                .topic(TOPIC)
                .query(queryParameters)
                .build();

        when(kafkaInspectorService.findRecord(query, true))
                .thenReturn(buildTestRecord());

        mockMvc.perform(get("/findByKey")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(query)))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Joe-b")));
    }

    @Test
    public void testFindByKeyNotFound() throws Exception {
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("key1", "key1_value-b");
        KafkaQuery query = KafkaQuery.builder()
                .topic(TOPIC)
                .query(queryParameters)
                .build();

        when(kafkaInspectorService.findRecord(query, true))
                .thenReturn(Optional.empty());

        mockMvc.perform(get("/findByKey")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(query)))
                .andExpect(status().isNoContent());
    }

    @Test
    public void testFindByQuerySuccess() throws Exception {
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("key1", "key1_value-b");
        KafkaQuery query = KafkaQuery.builder()
                .topic(TOPIC)
                .query(queryParameters)
                .build();

        when(kafkaInspectorService.findRecord(query, false))
                .thenReturn(buildTestRecord());

        mockMvc.perform(get("/findByQuery")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(query)))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Joe-b")));
    }

    @Test
    public void testFindByQueryNotFound() throws Exception {
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("key1", "key1_value-b");
        KafkaQuery query = KafkaQuery.builder()
                .topic(TOPIC)
                .query(queryParameters)
                .build();

        System.out.println(objectMapper.writeValueAsString(query));

        when(kafkaInspectorService.findRecord(query, false))
                .thenReturn(Optional.empty());

        mockMvc.perform(get("/findByQuery")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(query)))
                .andExpect(status().isNoContent());
    }

    private Optional<KafkaRecord> buildTestRecord() {
        return Optional.of(KafkaRecord.builder()
                .key(TestData.generateTestKeyJson("b"))
                .value(TestData.generateTestRecordJson("b"))
                .offset(0L)
                .partition(0)
                .build());
    }
}