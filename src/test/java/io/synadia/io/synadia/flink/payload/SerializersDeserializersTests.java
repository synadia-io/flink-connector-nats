// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.io.synadia.flink.payload;

import io.synadia.flink.payload.StringPayloadDeserializer;
import io.synadia.flink.payload.StringPayloadSerializer;
import io.synadia.io.synadia.flink.TestBase;
import io.synadia.io.synadia.flink.WordCount;
import io.synadia.io.synadia.flink.WordCountDeserializer;
import io.synadia.io.synadia.flink.WordCountSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class SerializersDeserializersTests extends TestBase {

    @Test
    public void testStringPayload() throws Exception {
        // validate works from construction
        StringPayloadDeserializer spdAscii = new StringPayloadDeserializer("ASCII");
        StringPayloadDeserializer spdUtf8 = new StringPayloadDeserializer();
        StringPayloadSerializer spsAscii = new StringPayloadSerializer("ASCII");
        StringPayloadSerializer spsUtf8 = new StringPayloadSerializer();
        validateStringPayload(spdAscii, spdUtf8, spsAscii, spsUtf8);

        // validate works after java serialization round trip
        spdAscii = (StringPayloadDeserializer)javaSerializeDeserializeObject(spdAscii);
        spdUtf8 = (StringPayloadDeserializer)javaSerializeDeserializeObject(spdUtf8);
        spsAscii = (StringPayloadSerializer)javaSerializeDeserializeObject(spsAscii);
        spsUtf8 = (StringPayloadSerializer)javaSerializeDeserializeObject(spsUtf8);
        validateStringPayload(spdAscii, spdUtf8, spsAscii, spsUtf8);
    }

    private static void validateStringPayload(StringPayloadDeserializer spdAscii,
                                              StringPayloadDeserializer spdUtf8,
                                              StringPayloadSerializer spsAscii,
                                              StringPayloadSerializer spsUtf8) {

        byte[] bytes = PLAIN_ASCII.getBytes();
        assertEquals(PLAIN_ASCII, spdAscii.getObject(bytes));
        assertEquals(PLAIN_ASCII, spdUtf8.getObject(bytes));

        bytes = spsAscii.getBytes(PLAIN_ASCII);
        assertEquals(PLAIN_ASCII, spdAscii.getObject(bytes));
        assertEquals(PLAIN_ASCII, spdUtf8.getObject(bytes));

        bytes = spsUtf8.getBytes(PLAIN_ASCII);
        assertEquals(PLAIN_ASCII, spdAscii.getObject(bytes));
        assertEquals(PLAIN_ASCII, spdUtf8.getObject(bytes));

        for (String su : UTF8_TEST_STRINGS) {
            bytes = su.getBytes(StandardCharsets.UTF_8);
            assertNotEquals(su, spdAscii.getObject(bytes));
            assertEquals(su, spdUtf8.getObject(bytes));

            bytes = spsUtf8.getBytes(su);
            assertNotEquals(su, spdAscii.getObject(bytes));
            assertEquals(su, spdUtf8.getObject(bytes));
        }
    }

    @Test
    public void testCustomPayload() {
        WordCountSerializer ser = new WordCountSerializer();
        WordCountDeserializer dser = new WordCountDeserializer();
        for (String json : WORD_COUNT_JSONS) {
            WordCount wc = new WordCount(json);
            byte[] bytes = ser.getBytes(wc);
            WordCount wc2 = new WordCount(bytes);
            assertEquals(wc, wc2);
            wc2 = dser.getObject(bytes);
            assertEquals(wc, wc2);
        }
    }

}
