package com.solacesystems.poc;

import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class PropsTest {

    private static Properties testProperties(String translations, String kafkaTopics, String solaceBridgeQueue) {
        Properties props = new Properties();
        props.setProperty(BridgeProperties.PROP_KAFKA_SOLACE_TOPIC_TRANSLATIONS, translations);
        props.setProperty(BridgeProperties.PROP_KAFKA_BRIDGE_TOPICS, kafkaTopics);
        props.setProperty(BridgeProperties.PROP_SOLACE_BRIDGE_QUEUE, solaceBridgeQueue);
        return props;
    }

    @Test
    public void emptyTest() {
        Properties props = new Properties();
        IOHelper.fixPropertyTypes(props);
        List<String[]> translations = (List<String[]>)props.get(BridgeProperties.PROP_KAFKA_SOLACE_TOPIC_TRANSLATIONS);
        assertEquals("Parser failed to properly parse the number of topic translations string",
                0, translations.size());

        List<String> kafkaTopics = (List<String>) props.get(BridgeProperties.PROP_KAFKA_BRIDGE_TOPICS);
        assertEquals("Failed to properly parse Kafka topic list",
                0, kafkaTopics.size());
    }

    @Test
    public void testPropsMod() {
        Properties props = testProperties("ktest:stest,ktwo:s/two\\:fluff", "test1,test2", "bridgeq");
        IOHelper.fixPropertyTypes(props);
        List<String[]> translations = (List<String[]>)props.get(BridgeProperties.PROP_KAFKA_SOLACE_TOPIC_TRANSLATIONS);
        assertEquals("Parser failed to properly parse the number of topic translations string",
                2, translations.size());
        assertEquals("Parser failed to properly parse a translation pair with escaped ':'",
                2, translations.get(1).length);

        List<String> kafkaTopics = (List<String>) props.get(BridgeProperties.PROP_KAFKA_BRIDGE_TOPICS);
        assertEquals("Failed to properly parse Kafka topic list",
                2, kafkaTopics.size());
    }
}
