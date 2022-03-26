package com.gamesys.test.tools.service;

import com.gamesys.test.tools.domain.KafkaQuery;
import com.gamesys.test.tools.domain.KafkaRecord;
import com.gamesys.test.tools.kafka.KafkaInspector;
import com.gamesys.test.tools.kafka.MatchUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
public class KafkaInspectorService {

    private KafkaInspector inspector;

    @Autowired
    public KafkaInspectorService(KafkaInspector inspector) {
        this.inspector = inspector;
    }

    public Optional<KafkaRecord> findRecord(KafkaQuery query,
                                            boolean queryByKey) {
        Optional<KafkaRecord> result = Optional.empty();
        boolean poll = true;
        try {
            while (poll) {
                ConsumerRecords<String, String> records = inspector.getRecords(query.getTopic(), 5000);
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        log.debug("RECORD key:{} value:{} offset:{}", record.key(), record.value(), record.offset());
                        if (MatchUtils.match(queryByKey ? record.key() : record.value(), query.getQuery())) {
                            result = Optional.of(KafkaRecord.builder()
                                    .key(record.key())
                                    .value(record.value())
                                    .partition(record.partition())
                                    .offset(record.offset())
                                    .build());
                            poll = false;
                            break;
                        }
                    }
                } else {
                    poll = false;
                }
            }
        } finally {
            inspector.close();
        }
        return result;
    }
}