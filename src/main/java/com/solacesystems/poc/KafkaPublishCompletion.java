package com.solacesystems.poc;

import com.solacesystems.jcsmp.BytesXMLMessage;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaPublishCompletion implements org.apache.kafka.clients.producer.Callback {
    private static final Logger logger = LoggerFactory.getLogger(KafkaPublishCompletion.class);

    public KafkaPublishCompletion(BytesXMLMessage solaceMsg) {
        this.solaceMsg = solaceMsg;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        // On ACK from Kafka bus, ACK the message back to the Solace bus
        if (exception == null) {
            solaceMsg.ackMessage();
        }
        else {
            logger.error("FAILURE Publishing message to Kafka; Solace message "
                    + solaceMsg.getApplicationMessageId() + " is NOT Acknowledged",
                    exception);
        }
    }

    final private BytesXMLMessage solaceMsg;
}
