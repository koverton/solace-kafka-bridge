package com.solacesystems.poc;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

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

    // Pass-through bridges sending raw bytes across; at the ends,
    // use Kafka serializers/deserializers but no point in doing
    // all that work in the middle
    private KafkaConnector<byte[],byte[]>  kafkaConn;
    private SolaceConnector<byte[],byte[]> solaceConn;

    private TopicTranslator kafkaSolaceTranslator;
    private TopicTranslator solaceKafkaTranslator;

    private boolean solaceConnected = false;
    private boolean kafkaConnected  = false;

    BridgingConnector(Properties properties) throws Exception {
        kafkaConn  = new KafkaConnector<>(properties);
        solaceConn = new SolaceConnector<>(properties);
        kafkaSolaceTranslator = new TopicStringTranslator((List<String[]>)
                        properties.get(BridgeProperties.PROP_KAFKA_SOLACE_TOPIC_TRANSLATIONS));
        solaceKafkaTranslator = new TopicStringTranslator((List<String[]>)
                        properties.get(BridgeProperties.PROP_SOLACE_KAFKA_TOPIC_TRANSLATIONS));
    }

    public static BridgingConnector newBridgingConnector(String propertyFilePath) throws Exception {
        Properties props = IOHelper.readPropsFile(propertyFilePath);
        IOHelper.dumpProperties(props);
        // These are always pure pass-through, don't let users configure them
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return new BridgingConnector(props);
    }

    public void start() throws Exception {
        logger.info("Connecting to Solace...");
        solaceConn.start(
                new ConnectionListener<byte[], byte[]>() {
            @Override
            public boolean onMessage(Object source, Integer partition, String topic, byte[] key, byte[] value) {
                boolean result = false;
                try {
                    String kafkaTopic = solaceKafkaTranslator.translate(topic);
                    kafkaConn.send(kafkaTopic, key, value, new KafkaPublishCompletion((BytesXMLMessage)source));
                    result = true;
                } catch (Exception ex) {
                    logger.error("FAILED to send to Solace", ex);
                    result = false;
                }
                return result;
            }
            @Override
            public void onConnected() {
                solaceConnected = true;
            }
            @Override
            public void onDisconnected() {
                solaceConnected = false;
            }
        });
        logger.info("Connecting to kafka ...");
        kafkaConn.start(
                new ConnectionListener<byte[], byte[]>() {
                    @Override
                    public boolean onMessage(Object source, Integer partition, String topic, byte[] key, byte[] value) {
                        logger.trace("Got Kafka message; sending over Solace.");
                        boolean result = false;
                        String solaceTopic = kafkaSolaceTranslator.translate(topic);
                        try {
                            solaceConn.send(partition, solaceTopic, key, value);
                            result = true;
                        }
                        catch(JCSMPException ex) {
                            logger.error("EXCEPTION publishing msg to solace", ex);
                            ex.printStackTrace();
                            result = false;
                        }
                        return result;
                    }
                    @Override
                    public void onConnected() {
                        kafkaConnected = true;
                        solaceConn.startFlow();
                    }
                    @Override
                    public void onDisconnected() {
                        kafkaConnected = false;
                        solaceConn.stopFlow();
                    }
                });
        logger.info("Kafka client started.");

    }

    public void run() {
        Long l = 1L;
        Timer t = new Timer();
        t.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                solaceConn.dumpStats();
            }
        }, 2000L, 2000L);
        while(true) {
            if (logger.isTraceEnabled())
                logger.trace(" Polling " + l++);
            if (solaceConnected) {
                kafkaConn.poll(1000);
            }
            else {
                logger.warn("Skipping Kafka poll because Solace is disconnected.");
                try { Thread.sleep(1000); } catch(InterruptedException e) {}
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("\tUSAGE: <path/to/kafka-solace-props-file.properties>");
            System.out.println("");
            System.exit(1);
        }

        try {
            BridgingConnector bridge = BridgingConnector.newBridgingConnector(args[0]);

            bridge.start();

            bridge.run();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
