package com.gamesys.test.tools.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class KafkaInspector {

    private Konfig konfig;
    private Consumer<String, String> consumer;

    @Autowired
    public KafkaInspector(Konfig konfig) {
        this.konfig = konfig;
    }

    public ConsumerRecords<String, String> getRecords(String topic, long poll) {
        consumer = konfig.initialiseConsumer(topic);
        List<TopicPartition> topicsPartitions = consumer.partitionsFor(topic).stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .collect(Collectors.toList());
        consumer.assign(topicsPartitions);
        consumer.seekToBeginning(topicsPartitions);
        Map<Integer, Long> partitionOffsets = new HashMap<>();
        consumer.partitionsFor(topic).stream().forEach(partitionInfo -> {
            partitionOffsets.put(partitionInfo.partition(), 0L);
        });
        return consumer.poll(poll);
    }

    public void close() {
        consumer.close();
    }
}
