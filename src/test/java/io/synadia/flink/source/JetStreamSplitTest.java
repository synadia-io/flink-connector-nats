// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.Message;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.synadia.flink.source.split.JetStreamSplit;
import org.apache.flink.util.FlinkRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for {@link JetStreamSplit} — initial state, the
 * mark-emitted progression, finished flag, and the JSON round-trip
 * (both fresh and mid-flight).
 */
class JetStreamSplitTest {

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
    void initialState() {
        JetStreamSubjectConfiguration cfg = sc("a");
        JetStreamSplit split = new JetStreamSplit(cfg);

        assertEquals(cfg.id, split.splitId());
        assertSame(cfg, split.subjectConfig);
        assertEquals(-1L, split.lastEmittedStreamSequence.get());
        assertNull(split.lastEmittedMessageReplyTo.get());
        assertEquals(0L, split.emittedCount.get());
        assertFalse(split.finished.get());
    }

    @Test
    void markEmitted_advancesSequenceReplyToAndCount() {
        JetStreamSplit split = new JetStreamSplit(sc("a"));

        assertEquals(1L, split.markEmitted(jsMessage(42L, "reply.1")));
        assertEquals(42L, split.lastEmittedStreamSequence.get());
        assertEquals("reply.1", split.lastEmittedMessageReplyTo.get());

        assertEquals(2L, split.markEmitted(jsMessage(43L, "reply.2")));
        assertEquals(43L, split.lastEmittedStreamSequence.get());
        assertEquals("reply.2", split.lastEmittedMessageReplyTo.get());
        assertEquals(2L, split.emittedCount.get());
    }

    @Test
    void setFinished_flipsFlag() {
        JetStreamSplit split = new JetStreamSplit(sc("a"));
        assertFalse(split.finished.get());
        split.setFinished();
        assertTrue(split.finished.get());
    }

    @Test
    void toString_containsSubject() {
        JetStreamSplit split = new JetStreamSplit(sc("orders.us"));
        String s = split.toString();
        assertTrue(s.contains("JetStreamSplit"), s);
        assertTrue(s.contains("orders.us"), s);
    }

    @Test
    void jsonRoundTrip_freshSplit() {
        JetStreamSubjectConfiguration cfg = sc("a");
        JetStreamSplit original = new JetStreamSplit(cfg);

        JetStreamSplit copy = new JetStreamSplit(original.toJson());

        assertEquals(-1L, copy.lastEmittedStreamSequence.get());
        assertNull(copy.lastEmittedMessageReplyTo.get());
        assertEquals(0L, copy.emittedCount.get());
        assertFalse(copy.finished.get());
        assertEquals(cfg, copy.subjectConfig);
        assertEquals(cfg.id, copy.splitId());
    }

    @Test
    void jsonRoundTrip_midFlight() {
        JetStreamSplit original = new JetStreamSplit(sc("a"));
        original.markEmitted(jsMessage(42L, "reply.1"));
        original.markEmitted(jsMessage(43L, "reply.2"));
        original.setFinished();

        JetStreamSplit copy = new JetStreamSplit(original.toJson());

        assertEquals(43L, copy.lastEmittedStreamSequence.get());
        assertEquals("reply.2", copy.lastEmittedMessageReplyTo.get());
        assertEquals(2L, copy.emittedCount.get());
        assertTrue(copy.finished.get());
    }

    @Test
    void jsonCtor_throwsOnGarbage() {
        assertThrows(FlinkRuntimeException.class, () -> new JetStreamSplit("not-json"));
    }
}
