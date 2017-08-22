package com.solacesystems.poc;

import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.junit.Test;

import java.util.Properties;

public class BridgeTests {

    final private static String BOOTSTRAP_SERVERS = "localhost:9092";
    final private static String TOPIC = "test";

    //@Test
    public void basicRun() throws Exception {
        final Properties props = new Properties();
        // Kafka configs
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        // Solace configs
        props.put(JCSMPProperties.HOST, "192.168.56.199");
        props.put(JCSMPProperties.VPN_NAME, "default");
        props.put(JCSMPProperties.USERNAME, "default");
        // Pass-through serializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        BridgingConnector bridge = new BridgingConnector(props);

        bridge.start("bridge_queue", TOPIC );

        bridge.run();
    }
}
