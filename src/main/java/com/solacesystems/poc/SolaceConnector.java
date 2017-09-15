package com.solacesystems.poc;

import com.solacesystems.jcsmp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

class SolaceConnector<K,V> {
    private static final Logger logger = LoggerFactory.getLogger(SolaceConnector.class);

    private static final int MAX_CACHED_MSGS = 256;

    public SolaceConnector(Properties properties) throws Exception {
        listener = null;

        msgHelper = new MsgHelper<K,V>(properties);

        this.sourceQueue = properties.getProperty(BridgeProperties.PROP_SOLACE_BRIDGE_QUEUE);
        final JCSMPProperties solprops = new JCSMPProperties();
        for (String name : properties.stringPropertyNames()) {
            Object value = properties.getProperty(name);
            solprops.setProperty(name, value);
        }
        solprops.setIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, 50);
        solprops.setBooleanProperty(JCSMPProperties.GENERATE_RCV_TIMESTAMPS, true);
        solprops.setBooleanProperty(JCSMPProperties.GENERATE_SEND_TIMESTAMPS, true);
        JCSMPChannelProperties channelProperties =
                (JCSMPChannelProperties) solprops.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
        channelProperties.setConnectRetries(100);
        channelProperties.setReconnectRetries(-1);
        channelProperties.setReconnectRetryWaitInMillis(200);
        session = JCSMPFactory.onlyInstance().createSession(solprops);

        producer = session.getMessageProducer(
                new JCSMPStreamingPublishCorrelatingEventHandler() {
                    @Override
                    public void handleErrorEx(Object key, JCSMPException e, long timestamp) {
                        logger.error("Solace Producer received error for msg: {}@{} - {}",
                                key, timestamp, e);
                    }
                    @Override
                    public void responseReceivedEx(Object key) {
                        SolaceSentMessageState inbound = (SolaceSentMessageState) key;
                        logger.debug("Producer received response for msg: {}", inbound.getMessageID());
                        SolaceSentMessageState state = usedMsgBuffer.remove();
                        if (inbound.getMessageID().equals(state.getMessageID())) {
                            logger.debug("ACK'd message equals allocated message {}", state.getMessageID());
                            // TODO: ACK the message back to the Kafka connector here if we find a way
                            // Put the ack'd message back into our list of Free messages to be reused
                            freeMsgBuffer.append(state);
                        }
                        else {
                            // If these don't match, something bad is going on
                            logger.error("MAYDAY! Latest ACK ID {} does not match earliest sent ID {}; either out of sequence or worse.",
                                    inbound.getMessageID(), state.getMessageID());
                        }
                    }
                    @Override
                    public void responseReceived(String messageID) {
                        logger.error("Not expected to be called on standard JCSMPStreamingPublishEventHandler because Correlation key was set.");
                    }
                    @Override
                    public void handleError(String messageID, JCSMPException e, long timestamp) {
                        logger.error("Solace Producer received error for msg: {}@{} - {}", messageID, timestamp, e);
                    }
                });
    }

    public void start(ConnectionListener<K,V> callback) throws JCSMPException {
        listener = callback;

        ConsumerFlowProperties queueProps = new ConsumerFlowProperties();
        queueProps.setEndpoint(JCSMPFactory.onlyInstance().createQueue(this.sourceQueue));
        queueProps.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
        queueProps.setActiveFlowIndication(true);
        session.connect();
        consumer = session.createFlow(
                new XMLMessageListener() {

                    @Override
                    public void onReceive(BytesXMLMessage msg) {
                        String topic = msg.getDestination().getName();

                        // Deserialize the key
                        K key = msgHelper.getKey(msg, topic);

                        // Deserialize the value
                        V value = msgHelper.getValue(msg, topic);

                        // Invoke the listener
                        try {
                            listener.onMessage(msg, null, topic, key, value);
                        }
                        catch(Exception e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void onException(JCSMPException e) {
                        logger.error("EXCEPTION receiving Solace messages: {}", e);
                    }
                },
                queueProps,
                null,
                new FlowEventHandler() {
                    @Override
                    public void handleEvent(Object o, FlowEventArgs args) {
                        if (args.getEvent().equals(FlowEvent.FLOW_ACTIVE)) {
                            // TODO: Active Flow Indication
                            listener.onConnected();
                        }
                        else if (args.getEvent().equals(FlowEvent.FLOW_INACTIVE)) {
                            // TODO: INACTIVE flow
                            listener.onDisconnected();
                        }
                    }
                });
        consumer.start();
    }

    public void send(int partition, String topicName, K key, V payload) throws JCSMPException {
        SolaceSentMessageState msgState = null;
        if (freeMsgBuffer.used() > 0)
            msgState = freeMsgBuffer.remove();
        else
            msgState = new SolaceSentMessageState();
        msgHelper.populateMessage(msgState, partition, topicName, key, payload);
        usedMsgBuffer.append(msgState);
        producer.send(msgState.getMessage(), msgState.getDestination());
    }

    public void stopFlow() {
        if (consumer != null)
            consumer.stop();
    }

    public void startFlow() {
        try {
            if (consumer != null)
                consumer.start();
        }
        catch(JCSMPException e) {
            logger.error("FAILED to start consumer on {}", consumer.getEndpoint().getName());
            e.printStackTrace();
        }
    }

    // Solace session
    final private JCSMPSession session;

    // Solace producer
    private XMLMessageProducer producer;
    private final RingBuffer<SolaceSentMessageState> usedMsgBuffer = new RingBuffer<>(SolaceSentMessageState.class, MAX_CACHED_MSGS);
    private final RingBuffer<SolaceSentMessageState> freeMsgBuffer = new RingBuffer<>(SolaceSentMessageState.class, MAX_CACHED_MSGS);

    // Solace listener
    private FlowReceiver consumer;
    // Client listener interested in Solace messages
    private ConnectionListener<K,V> listener;
    private String sourceQueue;

    // Encapsulates message handling, serialization
    private final MsgHelper<K,V> msgHelper;
}
