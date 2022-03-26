package com.gamesys.test.tools.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class KafkaRecord {
    private String key;
    private String value;
    private Integer partition;
    private Long offset;
}
