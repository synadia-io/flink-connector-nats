// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.Message;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.synadia.flink.source.split.JetStreamCheckpointSerializer;
import io.synadia.flink.source.split.JetStreamSplit;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for {@link JetStreamCheckpointSerializer} — version,
 * empty / single / multi-split round-trip, and the unrecognized-version
 * error path.
 */
class JetStreamCheckpointSerializerTest {

    private static final JetStreamCheckpointSerializer SER = new JetStreamCheckpointSerializer();

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
        assertEquals(JetStreamCheckpointSerializer.CURRENT_VERSION, SER.getVersion());
        assertEquals(1, SER.getVersion());
    }

    @Test
    void roundTrip_empty() throws IOException {
        Collection<JetStreamSplit> copy = SER.deserialize(SER.getVersion(),
            SER.serialize(Collections.emptyList()));
        assertTrue(copy.isEmpty());
    }

    @Test
    void roundTrip_singleSplit() throws IOException {
        JetStreamSplit original = new JetStreamSplit(sc("a"));
        original.markEmitted(jsMessage(42L, "reply"));

        Collection<JetStreamSplit> copy = SER.deserialize(SER.getVersion(),
            SER.serialize(Collections.singletonList(original)));

        assertEquals(1, copy.size());
        JetStreamSplit only = copy.iterator().next();
        assertEquals(42L, only.lastEmittedStreamSequence.get());
        assertEquals("reply", only.lastEmittedMessageReplyTo.get());
        assertEquals(1L, only.emittedCount.get());
    }

    @Test
    void roundTrip_multiSplit_preservesOrderAndState() throws IOException {
        List<JetStreamSplit> originals = new ArrayList<>();
        JetStreamSplit a = new JetStreamSplit(sc("a"));
        a.markEmitted(jsMessage(10L, "reply.a"));
        originals.add(a);
        JetStreamSplit b = new JetStreamSplit(sc("b"));
        b.markEmitted(jsMessage(20L, "reply.b1"));
        b.markEmitted(jsMessage(21L, "reply.b2"));
        b.setFinished();
        originals.add(b);
        JetStreamSplit c = new JetStreamSplit(sc("c"));
        originals.add(c);

        Collection<JetStreamSplit> copy = SER.deserialize(SER.getVersion(), SER.serialize(originals));

        assertEquals(3, copy.size());
        Iterator<JetStreamSplit> it = copy.iterator();

        JetStreamSplit copyA = it.next();
        assertEquals("a", copyA.subjectConfig.subject);
        assertEquals(10L, copyA.lastEmittedStreamSequence.get());
        assertEquals(1L, copyA.emittedCount.get());
        assertFalse(copyA.finished.get());

        JetStreamSplit copyB = it.next();
        assertEquals("b", copyB.subjectConfig.subject);
        assertEquals(21L, copyB.lastEmittedStreamSequence.get());
        assertEquals("reply.b2", copyB.lastEmittedMessageReplyTo.get());
        assertEquals(2L, copyB.emittedCount.get());
        assertTrue(copyB.finished.get());

        JetStreamSplit copyC = it.next();
        assertEquals("c", copyC.subjectConfig.subject);
        assertEquals(-1L, copyC.lastEmittedStreamSequence.get());
        assertEquals(0L, copyC.emittedCount.get());
    }

    @Test
    void deserialize_unrecognizedVersionThrows() throws IOException {
        byte[] bytes = SER.serialize(Collections.emptyList());
        IOException io = assertThrows(IOException.class, () -> SER.deserialize(999, bytes));
        assertTrue(io.getMessage().contains("999"), io.getMessage());
    }
}
