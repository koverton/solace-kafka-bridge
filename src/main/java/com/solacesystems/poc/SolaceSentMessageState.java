package com.solacesystems.poc;

import com.solacesystems.jcsmp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks Solace sent messages and state of them for streaming and reuse.
 */
class SolaceSentMessageState implements HasMsgID {
    private static final Logger logger = LoggerFactory.getLogger(SolaceSentMessageState.class);

    /**
     * Allocates a new underlying Solace message envelope for sending.
     */
    public SolaceSentMessageState() {
        message = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
        message.setDeliveryMode(DeliveryMode.PERSISTENT);
        logger.info("Allocating a new Solace Message container.");
    }

    //
    // HasMsgID Implementation
    //

    /**
     * Sets a long integer msg-ID for tracking and uses it as the underlying message
     * ApplicationMessageID (string).
     *
     * @param id Long integer message ID; should be unique within this producer's sequence.
     */
    public void setMsgID(long id) {
        this.id = id;
        message.setApplicationMessageId(Long.toString(id));
    }

    /**
     * Gets the ApplicationMessageID as a long integer.
     *
     * @return The long integer used as the application messageID.
     */
    public long getMsgID() { return id; }

    //
    // SETTERS
    //

    /**
     * Sets the Solace destination topic for this message.
     *
     * @param destination Solace Destination object for this message to be published on.
     */
    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    /**
     * Sets the Kafka Partition this message was consumed from.
     *
     * @param partition Source partition for this message being published to Solace.
     */
    public void setPartition(int partition) {
        this.partition = partition;
    }

    /**
     * TBD Sets the Kafka Offset this message was received from.
     *
     * @param offset Source offset for this message being published to Solace.
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    //
    // GETTERS
    //

    /**
     * Gets the Solace destination topic for this message.
     * @return Solace Destination object for this message to be published on.
     */
    public Destination getDestination() {
        return destination;
    }

    /**
     * Gets the underlying Solace BytesXMLMessage for the Solace bus.
     * @return
     */
    public BytesXMLMessage getMessage() {
        return message;
    }

    /**
     * Gets the Kafka partition a record came from (can be useful for acking the record).
     *
     * @return Kafka partition number > 0 or -1 if not a Kafka record.
     */
    public int getPartition() {
        return partition;
    }

    /**
     * Gets the Kafka offset for a record (also useful for acking the record).
     *
     * @return Kafka offset length if > 0, or -1 if it is not a Kafka record.
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Retrieves the application message-ID from the Solace message.
     *
     * @return
     */
    public String getMessageID() {
        return message.getApplicationMessageId();
    }

    //
    // Private members
    //
    private final BytesXMLMessage message;
    private Destination destination;

    private int partition;
    private long offset;
    private long id;
}
