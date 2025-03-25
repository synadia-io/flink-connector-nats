// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.synadia.flink.enumerator.NatsSourceEnumeratorStateSerializer;
import io.synadia.flink.enumerator.NatsSubjectSourceEnumeratorState;
import io.synadia.flink.helpers.WordCount;
import io.synadia.flink.helpers.WordCountDeserializer;
import io.synadia.flink.helpers.WordCountSerializer;
import io.synadia.flink.payload.MessageRecord;
import io.synadia.flink.payload.StringPayloadDeserializer;
import io.synadia.flink.payload.StringPayloadSerializer;
import io.synadia.flink.source.split.NatsSubjectCheckpointSerializer;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.source.split.NatsSubjectSplitSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;

import static io.synadia.flink.source.split.NatsSubjectSplitSerializer.CURRENT_VERSION;
import static org.junit.jupiter.api.Assertions.*;

public class SerializersDeserializersTests extends TestBase {

    @DisplayName("Test Serialization for subject")
    @ParameterizedTest(name = "{2} | Subjects: {1}")
    @MethodSource("provideSplitTestData")
    void testSourceSideSerialization(int version, List<NatsSubjectSplit> splits, String description) throws Exception {
        NatsSubjectSplitSerializer splitSerializer = new NatsSubjectSplitSerializer();
        NatsSubjectCheckpointSerializer checkpointSerializer = new NatsSubjectCheckpointSerializer();

        for (NatsSubjectSplit split : splits) {
            byte[] serialized = splitSerializer.serialize(split);
            NatsSubjectSplit deserializedSplit = splitSerializer.deserialize(version, serialized);
            assertEquals(split.splitId(), deserializedSplit.splitId());

            if (version == CURRENT_VERSION) {
                for (int i = 0; i < split.getCurrentMessages().size(); i++) {
                    Message expectedMessage = split.getCurrentMessages().get(i);
                    Message actualMessage = deserializedSplit.getCurrentMessages().get(i);

                    assertEquals(expectedMessage.getSubject(), actualMessage.getSubject());
                    assertArrayEquals(expectedMessage.getData(), actualMessage.getData());

                    if (expectedMessage.getReplyTo() == null) {
                        assertNull(actualMessage.getReplyTo());
                    } else {
                        assertEquals(expectedMessage.getReplyTo(), actualMessage.getReplyTo());
                    }

                    if (expectedMessage.getHeaders() == null) {
                        assertNull(actualMessage.getHeaders());
                    } else {
                        assertEquals(expectedMessage.getHeaders().get("key1"), actualMessage.getHeaders().get("key1"));
                        assertEquals(expectedMessage.getHeaders().get("key2"), actualMessage.getHeaders().get("key2"));
                    }
                }
            }
        }

        byte[] serializedCheckpoint = checkpointSerializer.serialize(splits);
        Collection<NatsSubjectSplit> deserializedCheckpoint = checkpointSerializer.deserialize(version, serializedCheckpoint);

        assertEquals(splits, deserializedCheckpoint, "Checkpoint serialization failed");
    }

    private static Stream<Arguments> provideSplitTestData() {
        return Stream.of(
                // Standard cases with headers and replyTo
                Arguments.of(CURRENT_VERSION, generateSplits(List.of("three", "four", "five"), false, false),
                        String.format("Version %d | Three splits", CURRENT_VERSION)),
                Arguments.of(CURRENT_VERSION, generateSplits(List.of("six", "seven", "eight", "nine"), false, false),
                        String.format("Version %d | Four splits", CURRENT_VERSION)),
                Arguments.of(CURRENT_VERSION, generateSplits(List.of("ten"), false, false),
                        String.format("Version %d | Single split", CURRENT_VERSION)),

                // Cases without headers
                Arguments.of(CURRENT_VERSION, generateSplits(List.of("three", "four", "five"), true, false),
                        String.format("Version %d | Three splits without headers", CURRENT_VERSION)),
                Arguments.of(CURRENT_VERSION, generateSplits(List.of("six", "seven", "eight", "nine"), true, false),
                        String.format("Version %d | Four splits without headers", CURRENT_VERSION)),
                Arguments.of(CURRENT_VERSION, generateSplits(List.of("ten"), true, false),
                        String.format("Version %d | Single split without headers", CURRENT_VERSION)),

                // Cases without replyTo
                Arguments.of(CURRENT_VERSION, generateSplits(List.of("three", "four", "five"), false, true),
                        String.format("Version %d | Three splits without replyTo", CURRENT_VERSION)),
                Arguments.of(CURRENT_VERSION, generateSplits(List.of("six", "seven", "eight", "nine"), false, true),
                        String.format("Version %d | Four splits without replyTo", CURRENT_VERSION)),
                Arguments.of(CURRENT_VERSION, generateSplits(List.of("ten"), false, true),
                        String.format("Version %d | Single split without replyTo", CURRENT_VERSION))
        );
    }

    private static List<NatsSubjectSplit> generateSplits(List<String> subjects, boolean headersNull, boolean replyToNull) {
        List<NatsSubjectSplit> splits = new ArrayList<>();
        for (String subject : subjects) {
            List<Message> messages = generateMessages(subject, headersNull, replyToNull);
            splits.add(new NatsSubjectSplit(subject, messages));
        }
        return splits;
    }

    private static List<Message> generateMessages(String subject, boolean headersNull, boolean replyToNull) {
        List<Message> messages = new ArrayList<>();

        NatsMessage.Builder builder = new NatsMessage.Builder();
        if (!headersNull) {
            Headers headers = new Headers();
            headers.put("key1", "value1");
            headers.put("key2", "value2");

            builder.headers(headers);
        }

        if (!replyToNull) {
            builder.replyTo("_inbox." + subject);
        }

        Message firstMessage = builder.subject(subject).data(subject + "1").build();
        Message secondMessage = builder.subject(subject).data(subject + "2").build();
        Message thirdMessage = builder.subject(subject).data(subject + "3").build();
        Message fourthMessage = builder.subject(subject).data(subject + "4").build();

        messages.add(firstMessage);
        messages.add(secondMessage);
        messages.add(thirdMessage);
        messages.add(fourthMessage);
        return messages;
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

        MessageRecord p = toPayload(subject, bytes);

        assertEquals(PLAIN_ASCII, spdAscii.getObject(p));
        assertEquals(PLAIN_ASCII, spdUtf8.getObject(p));

        bytes = spsAscii.getBytes(PLAIN_ASCII);
        p = toPayload(subject, bytes);
        assertEquals(PLAIN_ASCII, spdAscii.getObject(p));
        assertEquals(PLAIN_ASCII, spdUtf8.getObject(p));

        bytes = spsUtf8.getBytes(PLAIN_ASCII);
        p = toPayload(subject, bytes);
        assertEquals(PLAIN_ASCII, spdAscii.getObject(p));
        assertEquals(PLAIN_ASCII, spdUtf8.getObject(p));

        for (String data : UTF8_TEST_STRINGS) {
            bytes = data.getBytes(StandardCharsets.UTF_8);
            p = toPayload("utf-data-1", bytes);
            assertNotEquals(data, spdAscii.getObject(p));
            assertEquals(data, spdUtf8.getObject(p));

            bytes = spsUtf8.getBytes(data);
            p = toPayload("utf-data-2", bytes);
            assertNotEquals(data, spdAscii.getObject(p));
            assertEquals(data, spdUtf8.getObject(p));
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
            wc2 = dser.getObject(toPayload("testCustomPayload", bytes));
            assertEquals(wc, wc2);
        }
    }

    private static MessageRecord toPayload(String subject, byte[] bytes) {
        return new MessageRecord(new NatsMessage(subject, null, bytes));
    }
}
