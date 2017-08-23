package com.solacesystems.poc;

import com.solacesystems.jcsmp.JCSMPException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Bridges Kafka <-> Solace traffic bidirectionally.
 *
 * On the Kafka side, subscribes to a list of Kafka topics for bridging.
 *
 * On the Solace side, binds to a Solace queue (topics are be mapped to that queue).
 *
 * When messages arrive in either consumer, they're immediately published to the other.
 */
public class BridgingConnector
{
    private static final Logger logger = LoggerFactory.getLogger(BridgingConnector.class);
    private static final int MAX_RESENDS = 5;

    // Pass-through bridges sending raw bytes across; at the ends,
    // use Kafka serializers/deserializers but no point in doing
    // all that work in the middle
    private KafkaConnector<byte[],byte[]>  kafkaConn;
    private SolaceConnector<byte[],byte[]> solaceConn;

    public BridgingConnector(Properties properties) throws Exception {
        kafkaConn  = new KafkaConnector<>(properties);
        solaceConn = new SolaceConnector<>(properties);
    }

    public void start(String solaceQueueName, String... kafkaTopics) throws Exception {
        List<String> kafkaTopicList = Arrays.asList(kafkaTopics);

        logger.info("Connecting to Solace...");
        solaceConn.start(solaceQueueName, new ConnectionListener<byte[], byte[]>() {
            @Override
            public void onMessage(String topic, Integer partition, byte[] key, byte[] value) {
                kafkaConn.send(topic, key, value);
            }
        });
        logger.info("Connecting to kafka ...");
        kafkaConn.start(kafkaTopicList, new ConnectionListener<byte[], byte[]>() {
            @Override
            public void onMessage(String topic, Integer partition, byte[] key, byte[] value) {
            if (logger.isTraceEnabled())
                logger.trace("Got Kafka message; sending over Solace.");
            int attempts = 0;
            while(attempts < MAX_RESENDS) {
                try {
                    solaceConn.send(topic, key, value);
                    attempts = MAX_RESENDS;
                } catch (JCSMPException solex) {
                    logger.error("FAILED to send to Solace, attempt {}; trying again.", attempts, solex);
                    attempts++;
                }
            }
            }
        });
        logger.info("Kafka client started.");

    }

    public void run() {
        Long l = 1L;
        while(true) {
            if (logger.isTraceEnabled())
                logger.trace(" Polling " + l++);
            kafkaConn.poll(1000);
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("    USAGE: <kafka-solace-props-file.properties> <solace queue> <kafka topics ...>");
            System.out.println("");
            System.out.println("");
            System.exit(1);
        }

        Properties props = IOHelper.readPropsFile(args[0]);
        // These are always pure pass-through, don't let users configure them
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        String queueName = args[1];
        String[] topics = new String[ args.length - 2 ];
        for(int i = 2; i < args.length; i++)
            topics[i-2] = args[i];

        try {
            BridgingConnector bridge = new BridgingConnector(props);

            bridge.start(queueName, topics);

            bridge.run();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}