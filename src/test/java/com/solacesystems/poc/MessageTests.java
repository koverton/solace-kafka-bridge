package com.solacesystems.poc;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTMap;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class MessageTests {

    @Test
    public void propsMapTest() {
        BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);

        assertNull("Message should not have properties", msg.getProperties());

        SDTMap map = JCSMPFactory.onlyInstance().createMap();
        msg.setProperties(map);
        assertNotNull("Message should have properties", msg.getProperties());

        msg.reset();

        assertNull("Message properties should not survive a reset", msg.getProperties());
    }
}
