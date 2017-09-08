package com.solacesystems.poc;

public interface TopicTranslator {
    /**
     * Translate an input topic to an output topic according to the specific implementation strategy.
     * @param topic
     * @return
     */
    String translate(String topic);
}
