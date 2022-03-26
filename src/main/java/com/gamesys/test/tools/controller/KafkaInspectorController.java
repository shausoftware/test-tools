package com.gamesys.test.tools.controller;

import com.gamesys.test.tools.domain.KafkaQuery;
import com.gamesys.test.tools.domain.KafkaRecord;
import com.gamesys.test.tools.service.KafkaInspectorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
public class KafkaInspectorController {

    private KafkaInspectorService kafkaInspectorService;

    @Autowired
    public  KafkaInspectorController(KafkaInspectorService kafkaInspectorService) {
        this.kafkaInspectorService = kafkaInspectorService;
    }

    @GetMapping(path = "/findByKey")
    public ResponseEntity<KafkaRecord> findByKey(@RequestBody KafkaQuery key) {
        Optional<KafkaRecord> record = kafkaInspectorService.findRecord(key, true);
        if (record.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(record.get());
    }

    @GetMapping(path = "/findByQuery")
    public ResponseEntity<KafkaRecord> findByQuery(@RequestBody KafkaQuery query) {
        Optional<KafkaRecord> record = kafkaInspectorService.findRecord(query, false);
        if (record.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(record.get());
    }
}
