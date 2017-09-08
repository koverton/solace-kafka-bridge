package com.solacesystems.poc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicStringTranslator implements TopicTranslator {

    public TopicStringTranslator(List<String[]> mappings) {
        for(String[] pair : mappings) {
            left2right.put(pair[0], pair[1]);
        }
    }

    public String translate(String topic) {
        if (left2right.containsKey(topic))
            return left2right.get(topic);
        return topic;
    }

    final private Map<String,String> left2right = new HashMap<>();
}
