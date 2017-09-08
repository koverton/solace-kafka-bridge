package com.solacesystems.poc;

/**
 * Defines a listener to a message source.
 * @param <K> Data type of the record keys received.
 * @param <V> Data type of the record values received.
 */
public interface ConnectionListener<K, V> {

    /**
     * Called for each message on the observed message source.
     * @param source Original inbound received object.
     * @param partition Kafka partition from which the inbound message was read.
     * @param topic Topic on which the inbound message was published.
     * @param key Record key instance after deserialization.
     * @param value Record value instance after deserialization.
     * @return False on any failure while processing the message.
     */
    public boolean onMessage(Object source, Integer partition, String topic, K key, V value) throws Exception;

    /**
     * Called when the underlying listener session becomes connected.
     */
    public void onConnected();

    /**
     * Called when the underlying listener session becomes disconnected.
     */
    public void onDisconnected();
}
