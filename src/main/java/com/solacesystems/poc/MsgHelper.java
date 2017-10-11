package com.solacesystems.poc;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;

/**
 * Convenience class to encapsulate message serialization and content handling
 * for producers and consumers.
 *
 * @param <K> type of the message Key from the Kafka bus
 * @param <V> type of the message Value from the Kafka bus
 */
class MsgHelper<K,V> {
    public final static String HDR_KAFKA_KEY = "solkaf_bridge_hdr_key";

    /**
     * Scans the properties for key and value serializer/deserializer definitions.
     *
     * @param props -- shared application properties object including Kafka and Solace properties.
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public MsgHelper(Properties props) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        String keyDeserName = props.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        String valDeserName = props.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        String keySerName   = props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        String valSerName   = props.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

        keyDeserializer = (Deserializer<K>)Class.forName(keyDeserName).newInstance();
        valDeserializer = (Deserializer<V>)Class.forName(valDeserName).newInstance();
        keySerializer   = (Serializer<K>)  Class.forName(keySerName).newInstance();
        valSerializer   = (Serializer<V>)  Class.forName(valSerName).newInstance();
    }

    /**
     * Serializes and writes the payload to the outbound message.
     * Serializes and writes the key to a header in the outbound message properties.
     * Sets the correlation object and customized application message ID.
     * Sets the topic on the outbound message for topic publication.
     * @param msgState full message and lifecycle state; includes message, topic, application msgID.
     * @param partition kafka partition from which the message was read.
     * @param topicName name of the source topic and outbound topic for publication.
     * @param key message key object instance.
     * @param payload message payload object instance.
     */
    public void populateMessage(SolaceSentMessageState msgState, int partition, String topicName, K key, V payload) {
        BytesXMLMessage msg = msgState.getMessage();
        putKey(msg, topicName, key);
        putValue(msg, topicName, payload);
        msg.setCorrelationKey(msgState);
        msgState.setMsgID(sequence++);
        msgState.setDestination(JCSMPFactory.onlyInstance().createTopic(topicName));
        msgState.setPartition(partition);
    }

    /**
     * Serializes the key and adds it to the message properties as a header value.
     * @param msg message to be populated with the key.
     * @param topic topic the message will be published on.
     * @param key key instance to be populated on the message.
     */
    public void putKey(BytesXMLMessage msg, String topic, K key) {
        SDTMap map = msg.getProperties();
        if (map == null) {
            map = JCSMPFactory.onlyInstance().createMap();
            msg.setProperties(map);
        }
        try {
            map.putBytes(HDR_KAFKA_KEY, keySerializer.serialize(topic, key));
        }
        catch(SDTException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Deserializes and returns the key instance from the message headers.
     * @param msg message on which to retrieve the key instance.
     * @param topic topic on which the message was received.
     * @return deserialized key instance.
     */
    public K getKey(BytesXMLMessage msg, String topic) {
        K key = null;
        SDTMap props = msg.getProperties();
        if (props != null && props.containsKey(HDR_KAFKA_KEY)) {
            try {
                byte[] keybytes = props.getBytes(HDR_KAFKA_KEY);
                key = (K) keyDeserializer.deserialize(topic, keybytes);
            }
            catch(SDTException ex) {
                ex.printStackTrace();
                key = null;
            }
        }
        return key;
    }

    /**
     * Serializes the payload and attaches it to the message for publication.
     * @param msg message to be populated with the serialized payload.
     * @param topic topic on which to publish the message.
     * @param payload payload to be serialized and attached to the message for publication.
     */
    public void putValue(BytesXMLMessage msg, String topic, V payload) {
        msg.writeAttachment(valSerializer.serialize(topic, payload));
    }

    /**
     * Deserializes and returns the payload instance from the message.
     * @param msg message from which to retrieve the payload instance.
     * @param topic topic on which the message was received.
     * @return deserialized payload instance.
     */
    public V getValue(BytesXMLMessage msg, String topic) {
        // Deserialize the value
        byte[] bytes = new byte[ msg.getAttachmentContentLength() ];
        msg.readAttachmentBytes(bytes);
        return (V) valDeserializer.deserialize(topic, bytes);
    }

    final private Deserializer<K> keyDeserializer;
    final private Deserializer<V> valDeserializer;
    final private Serializer<K>   keySerializer;
    final private Serializer<V>   valSerializer;

    private long sequence = System.nanoTime();
}
