package com.solacesystems.poc;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;


public class KafkaConnector<K,V> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConnector.class);

    public KafkaConnector(Properties properties) {
        // Create the consumer using props.
        consumer = new KafkaConsumer<>(properties);
        producer = new KafkaProducer<>(properties);
        listener = null;
    }

    public void start(List<String> sourceTopics, ConnectionListener<K,V> callback) {
        listener = callback;
        // Subscribe to the topic.
        consumer.subscribe(sourceTopics);
    }

    public void poll(int timeoutMillis) {
        final ConsumerRecords<K,V> consumerRecords = consumer.poll(timeoutMillis);

        if (consumerRecords.count()==0) {
            if (logger.isDebugEnabled())
                logger.debug("No records found.");
        }
        else {
            for(ConsumerRecord<K,V> record : consumerRecords) {
                if (listener != null) {
                    listener.onMessage(record.topic(), record.partition(), record.key(), record.value());
                    consumer.commitAsync();
                    // Don't checkpoint with Kafka server if the bridge hasn't really bridged the message
                }
            }
        }

    }

    public void send(String topic, K key, V value) {
        producer.send(new ProducerRecord<>(topic, key, value),
                new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // TODO: handle this
                    }
                });
    }

    final private Consumer<K,V> consumer;
    final private Producer<K,V> producer;
    private ConnectionListener<K,V> listener;
}
