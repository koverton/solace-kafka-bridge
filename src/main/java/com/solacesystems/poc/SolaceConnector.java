package com.solacesystems.poc;

import com.solacesystems.jcsmp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

class SolaceConnector<K,V> {
    private static final Logger logger = LoggerFactory.getLogger(SolaceConnector.class);

    private static final int MAX_CACHED_MSGS = 2048;

    public SolaceConnector(Properties properties) throws Exception {
        listener = null;

        msgHelper = new MsgHelper<K,V>(properties);

        this.sourceQueue = properties.getProperty(BridgeProperties.PROP_SOLACE_BRIDGE_QUEUE);
        final JCSMPProperties solprops = new JCSMPProperties();
        for (String name : properties.stringPropertyNames()) {
            Object value = properties.getProperty(name);
            solprops.setProperty(name, value);
        }
        solprops.setIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, 255);
        //solprops.setBooleanProperty(JCSMPProperties.GENERATE_RCV_TIMESTAMPS, true);
        //solprops.setBooleanProperty(JCSMPProperties.GENERATE_SEND_TIMESTAMPS, true);
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
                        if (inbound.getMsgID() == state.getMsgID()) {
                            logger.debug("ACK'd message equals allocated message {}", inbound.getMessageID());
                            // TODO: ACK the message back to the Kafka connector here if we find a way
                            // Put the ack'd message back into our list of Free messages to be reused
                            logger.debug("Putting message {} back in the msg-pool for re-use", inbound.getMessageID());
                            freeMsgBuffer.append(inbound);
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
                        e.printStackTrace();
                    }
                });
    }

    public void start(ConnectionListener<K,V> callback) throws JCSMPException {
        listener = callback;

        ConsumerFlowProperties queueProps = new ConsumerFlowProperties();
        queueProps.setWindowedAckMaxSize(255);
        queueProps.setAckThreshold(20);
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

    public void send(int partition, String topic, K key, V payload) throws JCSMPException {
        logger.debug("Sending message to Solace with topic {} and key {}", topic, key);

        SolaceSentMessageState msgState = null;
        if (freeMsgBuffer.used() > 0)
            msgState = freeMsgBuffer.remove();
        else {
            msgState = new SolaceSentMessageState();
            allocCount++;
        }
        msgHelper.populateMessage(msgState, partition, topic, key, payload);
        usedMsgBuffer.append(msgState);
        logger.debug("Sending msg: {}", msgState.getMsgID());
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

    public void dumpStats() {
        System.out.println("Buffers available " +
                        freeMsgBuffer.used() +
                        ", used "+
                        usedMsgBuffer.used() +
                        ", total alloc'd " +
                        allocCount);
    }

    // Solace session
    final private JCSMPSession session;

    // Solace producer
    private XMLMessageProducer producer;
    private final RingBuffer<SolaceSentMessageState> usedMsgBuffer = new RingBuffer<>(SolaceSentMessageState.class, MAX_CACHED_MSGS);
    private final RingBuffer<SolaceSentMessageState> freeMsgBuffer = new RingBuffer<>(SolaceSentMessageState.class, MAX_CACHED_MSGS);
    private int allocCount = 0;

    // Solace listener
    private FlowReceiver consumer;
    // Client listener interested in Solace messages
    private ConnectionListener<K,V> listener;
    private String sourceQueue;

    // Encapsulates message handling, serialization
    private final MsgHelper<K,V> msgHelper;
}
