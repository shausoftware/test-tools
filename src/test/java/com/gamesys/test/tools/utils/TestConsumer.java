package com.gamesys.test.tools.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Component
public class TestConsumer {

    private CountDownLatch latch = new CountDownLatch(1);
    private List<String> payload = new ArrayList<>();

    @KafkaListener(topics = "${test.topic}")
    public void receive(ConsumerRecord<?, ?> consumerRecord) {
        payload.add(consumerRecord.toString());
        latch.countDown();
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public List<String> getPayload() {
        return payload;
    }
}
