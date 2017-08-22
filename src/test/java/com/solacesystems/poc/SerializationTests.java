package com.solacesystems.poc;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SerializationTests {

    @Test
    public void stringTest() throws Exception {
        Deserializer<String> valDeserializer = (Deserializer<String>)Class.forName(StringDeserializer.class.getName()).newInstance();
        Serializer<String> valSerializer   = (Serializer<String>)  Class.forName(StringSerializer.class.getName()).newInstance();

        String hello = "Hello all you happy people.";

        byte[] blob = valSerializer.serialize("blah", hello);

        String rthello = valDeserializer.deserialize("blah", blob);

        assertEquals("WTF", hello, rthello);
    }
}
