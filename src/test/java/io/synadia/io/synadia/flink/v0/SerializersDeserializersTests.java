// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.io.synadia.flink.v0;

import io.synadia.flink.v0.enumerator.NatsSourceEnumeratorStateSerializer;
import io.synadia.flink.v0.enumerator.NatsSubjectSourceEnumeratorState;
import io.synadia.flink.v0.payload.StringPayloadDeserializer;
import io.synadia.flink.v0.payload.StringPayloadSerializer;
import io.synadia.flink.v0.source.split.NatsSubjectCheckpointSerializer;
import io.synadia.flink.v0.source.split.NatsSubjectSplit;
import io.synadia.flink.v0.source.split.NatsSubjectSplitSerializer;
import io.synadia.io.synadia.flink.TestBase;
import io.synadia.io.synadia.flink.WordCount;
import io.synadia.io.synadia.flink.WordCountDeserializer;
import io.synadia.io.synadia.flink.WordCountSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class SerializersDeserializersTests extends TestBase {

    @Test
    public void testSourceSideSerialization() throws Exception {
        NatsSubjectSplitSerializer splitSerializer = new NatsSubjectSplitSerializer();
        NatsSubjectCheckpointSerializer checkpointSerializer = new NatsSubjectCheckpointSerializer();

        List<NatsSubjectSplit> splits = new ArrayList<>();
        String[] subjects = new String[]{"one", "two", "three", "four", "five"};
        for (String subject : subjects) {
            NatsSubjectSplit split = new NatsSubjectSplit(subject);
            byte[] serialized = splitSerializer.serialize(split);
            NatsSubjectSplit de = splitSerializer.deserialize(NatsSubjectSplitSerializer.CURRENT_VERSION, serialized);
            assertEquals(subject, de.splitId());
            splits.add(split);
        }

        byte[] serialized = checkpointSerializer.serialize(splits);
        Collection<NatsSubjectSplit> deserialized = checkpointSerializer.deserialize(NatsSubjectSplitSerializer.CURRENT_VERSION, serialized);
        assertEquals(splits, deserialized);
    }

    @Test
    void testSourceEnumeratorSerialization() throws IOException {
        NatsSubjectSplit nss1 = new NatsSubjectSplit("one");
        NatsSubjectSplit nss2 = new NatsSubjectSplit("two");
        Set<NatsSubjectSplit> splits = new HashSet<>();
        splits.add(nss1);
        splits.add(nss2);

        NatsSubjectSourceEnumeratorState initialState =
            new NatsSubjectSourceEnumeratorState(splits);

        NatsSubjectSplitSerializer splitSerializer = new NatsSubjectSplitSerializer();
        NatsSourceEnumeratorStateSerializer serializer =
            new NatsSourceEnumeratorStateSerializer(splitSerializer);

        byte[] serialized = serializer.serialize(initialState);
        NatsSubjectSourceEnumeratorState deserializedState =
            serializer.deserialize(serializer.getVersion(), serialized);

        splits = deserializedState.getUnassignedSplits();
        assertEquals(2, splits.size());
        assertTrue(splits.contains(nss1));
        assertTrue(splits.contains(nss2));
    }

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

        String subject = "validateStringPayload";
        byte[] bytes = PLAIN_ASCII.getBytes();
        assertEquals(PLAIN_ASCII, spdAscii.getObject(subject, bytes, null));
        assertEquals(PLAIN_ASCII, spdUtf8.getObject(subject, bytes, null));

        bytes = spsAscii.getBytes(PLAIN_ASCII);
        assertEquals(PLAIN_ASCII, spdAscii.getObject(subject, bytes, null));
        assertEquals(PLAIN_ASCII, spdUtf8.getObject(subject, bytes, null));

        bytes = spsUtf8.getBytes(PLAIN_ASCII);
        assertEquals(PLAIN_ASCII, spdAscii.getObject(subject, bytes, null));
        assertEquals(PLAIN_ASCII, spdUtf8.getObject(subject, bytes, null));

        for (String su : UTF8_TEST_STRINGS) {
            bytes = su.getBytes(StandardCharsets.UTF_8);
            assertNotEquals(su, spdAscii.getObject(subject, bytes, null));
            assertEquals(su, spdUtf8.getObject(subject, bytes, null));

            bytes = spsUtf8.getBytes(su);
            assertNotEquals(su, spdAscii.getObject(subject, bytes, null));
            assertEquals(su, spdUtf8.getObject(subject, bytes, null));
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
            wc2 = dser.getObject("testCustomPayload", bytes, null);
            assertEquals(wc, wc2);
        }
    }

}
