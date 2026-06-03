// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.source.split.NatsSubjectSplitSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit coverage for {@link NatsSubjectSplitSerializer} — version,
 * v1 legacy + v2 round-trip across the message-shape matrix (with /
 * without headers, replyTo, data), and the error paths
 * (unrecognized version, null splitId).
 */
class NatsSubjectSplitSerializerTest {

    private static final NatsSubjectSplitSerializer SER = new NatsSubjectSplitSerializer();

    @Test
    void version_isTwo() {
        assertEquals(NatsSubjectSplitSerializer.CURRENT_VERSION, SER.getVersion());
        assertEquals(2, SER.getVersion());
    }

    @Test
    void roundTrip_emptyMessages() throws IOException {
        NatsSubjectSplit original = new NatsSubjectSplit("orders.us");

        NatsSubjectSplit copy = SER.deserialize(SER.getVersion(), SER.serialize(original));

        assertEquals("orders.us", copy.getSubject());
        assertTrue(copy.getCurrentMessages().isEmpty());
    }

    @Test
    void roundTrip_messageWithSubjectAndDataOnly() throws IOException {
        Message m = NatsMessage.builder()
            .subject("orders.us")
            .data("hello".getBytes())
            .build();
        NatsSubjectSplit original = new NatsSubjectSplit("orders.us", Collections.singletonList(m));

        NatsSubjectSplit copy = SER.deserialize(SER.getVersion(), SER.serialize(original));

        assertEquals("orders.us", copy.getSubject());
        assertEquals(1, copy.getCurrentMessages().size());
        Message rt = copy.getCurrentMessages().get(0);
        assertEquals("orders.us", rt.getSubject());
        assertArrayEquals("hello".getBytes(), rt.getData());
        assertNull(rt.getReplyTo());
        assertNull(rt.getHeaders());
    }

    @Test
    void roundTrip_messageWithEverything() throws IOException {
        Headers h = new Headers();
        h.add("X-Key", "v1", "v2");
        h.add("Y-Key", "only");
        Message m = NatsMessage.builder()
            .subject("orders.us")
            .replyTo("reply.inbox")
            .headers(h)
            .data("payload".getBytes())
            .build();
        NatsSubjectSplit original = new NatsSubjectSplit("orders.us", Collections.singletonList(m));

        NatsSubjectSplit copy = SER.deserialize(SER.getVersion(), SER.serialize(original));

        Message rt = copy.getCurrentMessages().get(0);
        assertEquals("orders.us", rt.getSubject());
        assertEquals("reply.inbox", rt.getReplyTo());
        assertArrayEquals("payload".getBytes(), rt.getData());
        assertNotNull(rt.getHeaders());
        assertEquals(Arrays.asList("v1", "v2"), rt.getHeaders().get("X-Key"));
        assertEquals(Collections.singletonList("only"), rt.getHeaders().get("Y-Key"));
    }

    @Test
    void roundTrip_multipleMessages() throws IOException {
        List<Message> messages = Arrays.asList(
            NatsMessage.builder().subject("a").data("1".getBytes()).build(),
            NatsMessage.builder().subject("a").data("2".getBytes()).build(),
            NatsMessage.builder().subject("a").data("3".getBytes()).build());
        NatsSubjectSplit original = new NatsSubjectSplit("a", messages);

        NatsSubjectSplit copy = SER.deserialize(SER.getVersion(), SER.serialize(original));

        assertEquals(3, copy.getCurrentMessages().size());
        for (int i = 0; i < 3; i++) {
            assertArrayEquals(Integer.toString(i + 1).getBytes(),
                copy.getCurrentMessages().get(i).getData());
        }
    }

    @Test
    void deserialize_v1Legacy_onlyReadsSubject() throws IOException {
        // V1 wire format is just `out.writeUTF(splitId)`.
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeUTF("legacy.subject");

        NatsSubjectSplit copy = SER.deserialize(1, out.getCopyOfBuffer());

        assertEquals("legacy.subject", copy.getSubject());
        assertTrue(copy.getCurrentMessages().isEmpty());
    }

    @Test
    void deserialize_unrecognizedVersionThrows() throws IOException {
        byte[] bytes = SER.serialize(new NatsSubjectSplit("orders.us"));
        IOException io = assertThrows(IOException.class, () -> SER.deserialize(999, bytes));
        assertTrue(io.getMessage().contains("999"), io.getMessage());
    }

    @Test
    void nullSplitIdThrows() {
        // Public path — the up-front guard in serialize() reports IOException
        // rather than NPE on the sizing call.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new NatsSubjectSplit(null));
        assertTrue(e.getMessage().contains("Subject cannot be null"), e.getMessage());
    }
}
