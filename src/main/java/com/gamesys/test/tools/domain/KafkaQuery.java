package com.gamesys.test.tools.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
@Builder
public class KafkaQuery {
    private String topic;
    private Map<String, String> query;
}
