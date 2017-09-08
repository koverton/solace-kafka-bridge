package com.solacesystems.poc;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

class IOHelper {

    static Properties readPropsFile(String name) {
        Properties props = new Properties();
        InputStream input = null;
        try {
            input = new FileInputStream(name);
            props.load(input);
            IOHelper.fixPropertyTypes(props);
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return props;
    }

    static void fixPropertyTypes(Properties props) {
        parseTopicTranslations(props, BridgeProperties.PROP_KAFKA_SOLACE_TOPIC_TRANSLATIONS);
        parseTopicTranslations(props, BridgeProperties.PROP_SOLACE_KAFKA_TOPIC_TRANSLATIONS);
        parseKafkaTopicList(props);
    }

    static void parseKafkaTopicList(Properties props) {
        String[] topics = parseCommaSeparatedList(props, BridgeProperties.PROP_KAFKA_BRIDGE_TOPICS);
        List<String> topicList = Arrays.asList(topics);
        props.put(BridgeProperties.PROP_KAFKA_BRIDGE_TOPICS, topicList);
    }

    static void parseTopicTranslations(Properties props, String propname) {
        ArrayList<String[]> mappings = new ArrayList<>();
        for(String entry : getTopicTranslations(props, propname)) {
            String[] pair = parseColonSeparatedPair(entry);
            if (pair.length == 2) {
                mappings.add(pair);
            }
        }
        props.put(propname, mappings);
    }


    static String[] getTopicTranslations(Properties props, String propname) {
        return parseCommaSeparatedList(props, propname);
    }

    static String[] parseColonSeparatedPair(String entry) {
        if (entry.contains(":")) {
            return entry.split(":", 2);
        }
        return new String[0];
    }

    static private String[] parseCommaSeparatedList(Properties props, String key) {
        if (props.containsKey(key)) {
            String valueString = (String) props.getProperty(key);
            return valueString.split(",");
        }
        return new String[0];
    }
}
