package com.solacesystems.poc;

import com.solacesystems.jcsmp.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SolaceConnector<K,V> {
    private static final Logger logger = LoggerFactory.getLogger(SolaceConnector.class);

    public final static String HDR_KAFKA_KEY = "solkaf_bridge_hdr_key";

    public SolaceConnector(Properties properties) throws Exception {
        listener = null;
        getSerializers(properties);

        final JCSMPProperties solprops = new JCSMPProperties();
        for (String name : properties.stringPropertyNames()) {
            Object value = properties.getProperty(name);
            solprops.setProperty(name, value);
        }
        session = JCSMPFactory.onlyInstance().createSession(solprops);

        producer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {

            public void responseReceived(String messageID) {
                // System.out.println("Producer received response for msg: " + messageID);
                lastMsgAcked = messageID;
            }

            public void handleError(String messageID, JCSMPException e, long timestamp) {
                System.out.printf("Producer received error for msg: %s@%s - %s%n",
                        messageID,timestamp,e);
                // TODO: retrieve from the Kafka connector
            }
        });
    }

    public void start(String sourceQueueName, ConnectionListener<K,V> callback) throws JCSMPException {
        listener = callback;

        ConsumerFlowProperties queueProps = new ConsumerFlowProperties();
        queueProps.setEndpoint(JCSMPFactory.onlyInstance().createQueue(sourceQueueName));
        queueProps.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_AUTO);
        session.connect();
        consumer = session.createFlow(
                new XMLMessageListener() {

                    @Override
                    public void onReceive(BytesXMLMessage msg) {
                        String topic = msg.getDestination().getName();

                        // Deserialize the key
                        K key = getKey(msg, topic);

                        // Deserialize the value
                        byte[] bytes = new byte[ msg.getAttachmentContentLength() ];
                        msg.readAttachmentBytes(bytes);
                        V value = (V) valDeserializer.deserialize(topic, bytes);

                        // Invoke the listener
                        listener.onMessage(topic, null, key, value);
                    }

                    @Override
                    public void onException(JCSMPException e) {
                    }
                },
                queueProps,
                null,
                new FlowEventHandler() {
                    @Override
                    public void handleEvent(Object o, FlowEventArgs args) {
                        if (args.getEvent().equals(FlowEvent.FLOW_ACTIVE)) {
                            // TODO: Active Flow Indication
                        }
                        else {
                            // TODO: INACTIVE flow
                        }
                    }
                });
        consumer.start();
    }

    public void send(String topicName, K key, V payload) throws JCSMPException {
        BytesXMLMessage msg = producer.createBytesXMLMessage();
        putKey(msg, topicName, key);
        msg.writeAttachment( valSerializer.serialize(topicName, payload) );
        msg.setDeliveryMode(DeliveryMode.PERSISTENT);

        producer.send(msg, JCSMPFactory.onlyInstance().createTopic(topicName));
    }

    private void putKey(BytesXMLMessage msg, String topic, K key) {
        SDTMap map = producer.createMap();
        try {
            map.putBytes(HDR_KAFKA_KEY, keySerializer.serialize(topic, key));
        }
        catch(SDTException ex) {
            ex.printStackTrace();
        }
        msg.setProperties(map);
    }

    private K getKey(BytesXMLMessage msg, String topic) {
        K key = null;
        SDTMap props = msg.getProperties();
        if (props.containsKey(HDR_KAFKA_KEY)) {
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

    private void getSerializers(Properties props) throws Exception {
        String keyDeserName = props.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        String valDeserName = props.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        String keySerName   = props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        String valSerName   = props.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

        keyDeserializer = (Deserializer<K>)Class.forName(keyDeserName).newInstance();
        valDeserializer = (Deserializer<V>)Class.forName(valDeserName).newInstance();
        keySerializer   = (Serializer<K>)  Class.forName(keySerName).newInstance();
        valSerializer   = (Serializer<V>)  Class.forName(valSerName).newInstance();
    }

    final private JCSMPSession session;
    private XMLMessageProducer producer;
    private ConnectionListener<K,V> listener;
    private FlowReceiver consumer;
    private String lastMsgAcked;

    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valDeserializer;
    private Serializer<K>   keySerializer;
    private Serializer<V>   valSerializer;
}
