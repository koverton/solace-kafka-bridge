package com.solacesystems.poc;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * Connects to a Kafka service, consuming records from topics and passing them off to interested listeners.
 * Also can be used to publish records to the Kafka service.
 * @param <K> Data type of Record Keys from the Kafka bus.
 * @param <V> Data type of Record Values from the Kafka bus.
 */
class KafkaConnector<K,V> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConnector.class);

    public KafkaConnector(Properties properties) {
        // Create the consumer using props.
        consumer = new KafkaConsumer<>(properties);
        producer = new KafkaProducer<>(properties);
        topics = (List<String>) properties.get(BridgeProperties.PROP_KAFKA_BRIDGE_TOPICS);
        listener = null;
    }

    /**
     * Wireup all callbacks and begin subscribing to Kafka.
     * @param sourceTopics List of topics to subscribe to.
     * @param consumerCallback Callback to be invoked when messages are consumed from Kafka.
     */
    public void start(ConnectionListener<K,V> consumerCallback) {
        listener = consumerCallback;
        // Subscribe to the topic.
        consumer.subscribe(this.topics);
    }

    /**
     * Poll Kafka for any inbound messages.
     * @param timeoutMillis blocking poll operation for the configured number of milliseconds.
     */
    public void poll(int timeoutMillis) {
        final ConsumerRecords<K,V> consumerRecords = consumer.poll(timeoutMillis);

        if (consumerRecords.count()==0) {
            logger.debug("No records found.");
        }
        else {
            if (logger.isDebugEnabled())
                logger.debug("Received {} records from Kafka", consumerRecords.count());
            for(ConsumerRecord<K,V> record : consumerRecords) {
                try {
                    listener.onMessage(record, record.partition(), record.topic(), record.key(), record.value());
                }
                catch(Exception ex) {
                    logger.error("Exception when handling Kafka msg (probably failed publishing to Solace)", ex);
                    ex.printStackTrace();
                }
            }
            //TODO: This is a BAD HACK that can result in message loss. Find a way to individually ack msgs when the upstream ack arrives
            consumer.commitAsync();
        }

    }

    /**
     * Send records to Kafka, invoking the producer callback when the record is ACK'd by the server.
     * @param topic Topic on which to publish the message.
     * @param key Key instance for the record.
     * @param value Value instance for the record.
     * @param producerCallback Callback function invoked when this record is acknowledged by Kafka.
     */
    public void send(String topic, K key, V value, Callback producerCallback) throws Exception {
        logger.debug("Sending message to Kafka with key {}", key);
        producer.send(new ProducerRecord<>(topic, key, value), producerCallback)
                .get(); // This makes the call synchronous
    }

    final private Consumer<K,V> consumer;
    final private Producer<K,V> producer;
    private List<String> topics;
    private ConnectionListener<K,V> listener;
}
