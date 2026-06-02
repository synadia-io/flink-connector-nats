// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.Message;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.synadia.flink.source.split.JetStreamSplit;
import io.synadia.flink.source.split.JetStreamSplitSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for {@link JetStreamSplitSerializer} — version,
 * serialize/deserialize round-trip for a fresh split and a mid-flight
 * split, and the unrecognized-version error path.
 */
class JetStreamSplitSerializerTest {

    private static final JetStreamSplitSerializer SER = new JetStreamSplitSerializer();

    private static JetStreamSubjectConfiguration sc(String subject) {
        return JetStreamSubjectConfiguration.builder()
            .streamName("S").subject(subject).build();
    }

    private static Message jsMessage(long streamSequence, String replyTo) {
        NatsJetStreamMetaData md = mock(NatsJetStreamMetaData.class);
        when(md.streamSequence()).thenReturn(streamSequence);
        Message m = mock(Message.class);
        when(m.metaData()).thenReturn(md);
        when(m.getReplyTo()).thenReturn(replyTo);
        return m;
    }

    @Test
    void version_isOne() {
        assertEquals(JetStreamSplitSerializer.CURRENT_VERSION, SER.getVersion());
        assertEquals(1, SER.getVersion());
    }

    @Test
    void roundTrip_freshSplit() throws IOException {
        JetStreamSubjectConfiguration cfg = sc("a");
        JetStreamSplit original = new JetStreamSplit(cfg);

        JetStreamSplit copy = SER.deserialize(SER.getVersion(), SER.serialize(original));

        assertEquals(cfg, copy.subjectConfig);
        assertEquals(-1L, copy.lastEmittedStreamSequence.get());
        assertNull(copy.lastEmittedMessageReplyTo.get());
        assertEquals(0L, copy.emittedCount.get());
        assertFalse(copy.finished.get());
    }

    @Test
    void roundTrip_midFlight() throws IOException {
        JetStreamSplit original = new JetStreamSplit(sc("a"));
        original.markEmitted(jsMessage(100L, "reply.1"));
        original.markEmitted(jsMessage(101L, "reply.2"));
        original.setFinished();

        JetStreamSplit copy = SER.deserialize(SER.getVersion(), SER.serialize(original));

        assertEquals(101L, copy.lastEmittedStreamSequence.get());
        assertEquals("reply.2", copy.lastEmittedMessageReplyTo.get());
        assertEquals(2L, copy.emittedCount.get());
        assertTrue(copy.finished.get());
    }

    @Test
    void deserialize_unrecognizedVersionThrows() throws IOException {
        byte[] bytes = SER.serialize(new JetStreamSplit(sc("a")));
        IOException io = assertThrows(IOException.class, () -> SER.deserialize(999, bytes));
        assertTrue(io.getMessage().contains("999"), io.getMessage());
    }
}
