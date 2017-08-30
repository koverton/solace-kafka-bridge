package com.solacesystems.poc;

import com.solacesystems.jcsmp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks Solace sent messages and state of them for streaming and reuse.
 *
 * Also functions as a member of a vector of messages sent in Solace batch-mode.
 */
class SolaceSentMessageState implements JCSMPSendMultipleEntry {
    private static final Logger logger = LoggerFactory.getLogger(SolaceSentMessageState.class);

    /**
     * Allocates a new underlying Solace message envelope for sending.
     */
    public SolaceSentMessageState() {
        message = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
        message.setDeliveryMode(DeliveryMode.PERSISTENT);
        logger.info("ALLOCATING NEW SOLACE MSG");
    }


    //
    // SETTERS
    //

    @Override
    public JCSMPSendMultipleEntry setDestination(Destination destination) {
        this.destination = destination;
        return this;
    }

    @Override
    public JCSMPSendMultipleEntry setMessage(XMLMessage message) {
        this.message = (BytesXMLMessage) message;
        return this;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
//
    // GETTERS
    //

    @Override
    public Destination getDestination() {
        return destination;
    }
    @Override
    public BytesXMLMessage getMessage() {
        return message;
    }

    /**
     * Gets the Kafka partition a record came from (can be useful for acking the record).
     * @return Kafka partition number > 0 or -1 if not a Kafka record.
     */
    public int getPartition() {
        return partition;
    }

    /**
     * Gets the Kafka offset for a record (also useful for acking the record).
     * @return Kafka offset length if > 0, or -1 if it is not a Kafka record.
     */
    public long getOffset() {
        return offset;
    }
    /**
     * Retrieves the application message-ID from the Solace message.
     * @return
     */
    public String getMessageID() {
        return message.getApplicationMessageId();
    }


    //
    // Private members
    //
    private BytesXMLMessage message;
    private Destination destination;

    private int partition;
    private long offset;
}
