package com.solacesystems.poc;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TopicTranslationTests {

    private static List<String[]> getTestMappings() {
        ArrayList<String[]> mappings = new ArrayList<>();

        mappings.add(new String[] { "from" , "to" });

        return mappings;
    }
    @Test
    public void basicStringTransTest() {
        List<String[]> mappings = getTestMappings();
        TopicTranslator translator = new TopicStringTranslator(mappings);

        assertEquals("Existing mapping did not translate",
                "to", translator.translate("from"));

        assertEquals("Topic with no mapping did not come back the same",
                "other", translator.translate("other"));
    }
}
