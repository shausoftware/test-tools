package com.gamesys.test.tools.kafka;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component
public class MatchUtils {

    public static boolean match(String json, Map<String, String> query) {
        Set<Boolean> match = new HashSet<>();
        query.entrySet().forEach(queryParameter -> {
            String value = findValueByKey(json, queryParameter.getKey());
            match.add(value != null && value.equals(queryParameter.getValue()));
        });
        return match.size() == 1 && match.contains(true);
    }

    public static String findValueByKey(String json, String key) {
        JSONObject root = null;
        String[] keyParts = null;
        try {
            root = new JSONObject(json);
            keyParts = key.split("\\.");
            for (int i = 0; i < keyParts.length; i++) {
                try {
                    if (i == keyParts.length - 1) {
                        return root.getString(keyParts[i]);
                    }
                    root = root.getJSONObject(keyParts[i]);
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            log.debug(e.getLocalizedMessage());
        }
        return null;
    }
}
