package com.solacesystems.poc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TestKafkaProducer
{
    private static final Logger logger = LoggerFactory.getLogger(TestKafkaProducer.class);

    // Pass-through bridges sending raw bytes across; at the ends,
    // use Kafka serializers/deserializers but no point in doing
    // all that work in the middle
    private KafkaConnector<byte[],byte[]>  kafkaConn;

    private boolean isConnected = true; // HACK: until the KafkaConnector can really track this, assume it's up

    TestKafkaProducer(Properties properties) throws Exception {
        kafkaConn  = new KafkaConnector<>(properties);
    }

    public static TestKafkaProducer newProducer(String propertyFilePath) throws Exception {
        Properties props = IOHelper.readPropsFile(propertyFilePath);
        IOHelper.dumpProperties(props);
        // These are always pure pass-through, don't let users configure them
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return new TestKafkaProducer(props);
    }

    public void start() throws Exception {
        logger.info("Connecting to kafka ...");
        kafkaConn.start(
                new ConnectionListener<byte[], byte[]>() {
                    @Override
                    public boolean onMessage(Object source, Integer partition, String topic, byte[] key, byte[] value) {
                        logger.trace("Got Kafka message; ignoring.");
                        return false;
                    }
                    @Override
                    public void onConnected() {
                        isConnected = true;
                    }
                    @Override
                    public void onDisconnected() {
                        isConnected = false;
                    }
                });
        logger.info("Kafka client started.");

    }

    public void run(int sleepMillis) {
        Long l = 1L;
        while(true) {
            try {
                if (sleepMillis > 0) Thread.sleep(sleepMillis);
                if (isConnected)
                    kafkaConn.send("test", "1".getBytes(), "A".getBytes(),
                            new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                // TBD
                            }
                        });
                else
                    Thread.sleep(100); // wait for kafka connection to come up
            } catch(Exception e) {}
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("\tUSAGE: <path/to/kafka-solace-props-file.properties>");
            System.out.println("");
            System.exit(1);
        }

        try {
            TestKafkaProducer producer = TestKafkaProducer.newProducer(args[0]);

            producer.start();

            producer.run(-1);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
