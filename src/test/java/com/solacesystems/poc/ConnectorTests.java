package com.solacesystems.poc;


import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;


public class ConnectorTests
{
    final private static String BOOTSTRAP_SERVERS = "localhost:9092";
    final private static String TOPIC = "test";

    final String messageString = "Hello all you happy people.";
    final int MAX = 10;

    //@Test
    public void kafkaConnectorTest()
    {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaConnector<Long,String> conn = new KafkaConnector<>(props);
        final AtomicInteger got = new AtomicInteger(0);

        conn.start(Collections.singletonList(TOPIC), new ConnectionListener<Long, String>() {
            @Override
            public void onMessage(String topic, Integer partition, Long key, String value) {
                got.incrementAndGet();
                assertEquals("Nonoveddybad", messageString, value);
            }
        });

        for(Long i = 0L; i < MAX; i++) {
            conn.send(TOPIC, i++, messageString);
            conn.poll(1000);
        }

        try { Thread.sleep(1000); } catch(InterruptedException e) { }

        assertEquals("WTF", MAX, got.get());
    }

    //@Test
    public void solaceConnectorTest() throws Exception {
        final Properties props = new Properties();
        props.put(JCSMPProperties.HOST, "192.168.56.199");
        props.put(JCSMPProperties.VPN_NAME, "default");
        props.put(JCSMPProperties.USERNAME, "default");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        SolaceConnector<Long,String> conn = new SolaceConnector<>(props);
        final AtomicInteger got = new AtomicInteger(0);

        conn.start(TOPIC,
                new ConnectionListener<Long, String>() {
                    @Override
                    public void onMessage(String topic, Integer partition, Long key, String value) {
                        got.incrementAndGet();
                        assertEquals("Nonoveddybad", messageString, value);
                    }
                });

        for(Long i = 0L; i < MAX; i++) {
            conn.send(TOPIC, i++, messageString);
        }

        try { Thread.sleep(2000); } catch(InterruptedException e) { }

        assertEquals("WTF", MAX, got.get());
    }

}
