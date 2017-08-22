package com.solacesystems.poc;

public interface ConnectionListener<K, V> {

    public void onMessage(String topic, Integer partition, K key, V value);
}
