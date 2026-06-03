// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.Message;
import io.nats.client.impl.NatsMessage;
import io.synadia.flink.source.split.NatsSubjectCheckpointSerializer;
import io.synadia.flink.source.split.NatsSubjectSplit;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit coverage for {@link NatsSubjectCheckpointSerializer} — version,
 * empty / single / multi round-trip, the v1 legacy path, and the
 * unrecognized-version error path.
 */
class NatsSubjectCheckpointSerializerTest {

    private static final NatsSubjectCheckpointSerializer SER = new NatsSubjectCheckpointSerializer();

    @Test
    void version_isTwo() {
        assertEquals(NatsSubjectCheckpointSerializer.CURRENT_VERSION, SER.getVersion());
        assertEquals(2, SER.getVersion());
    }

    @Test
    void roundTrip_empty() throws IOException {
        Collection<NatsSubjectSplit> copy = SER.deserialize(SER.getVersion(),
            SER.serialize(new ArrayList<>()));
        assertTrue(copy.isEmpty());
    }

    @Test
    void roundTrip_singleSplitWithMessages() throws IOException {
        Message m = NatsMessage.builder()
            .subject("a")
            .data("payload".getBytes())
            .build();
        NatsSubjectSplit original = new NatsSubjectSplit("a", Collections.singletonList(m));

        Collection<NatsSubjectSplit> copy = SER.deserialize(SER.getVersion(),
            SER.serialize(Collections.singletonList(original)));

        assertEquals(1, copy.size());
        NatsSubjectSplit only = copy.iterator().next();
        assertEquals("a", only.getSubject());
        assertEquals(1, only.getCurrentMessages().size());
        assertArrayEquals("payload".getBytes(), only.getCurrentMessages().get(0).getData());
    }

    @Test
    void roundTrip_multipleSplits_preservesOrder() throws IOException {
        Collection<NatsSubjectSplit> originals = Arrays.asList(
            new NatsSubjectSplit("a"),
            new NatsSubjectSplit("b"),
            new NatsSubjectSplit("c"));

        Collection<NatsSubjectSplit> copy = SER.deserialize(SER.getVersion(),
            SER.serialize(originals));

        assertEquals(3, copy.size());
        Iterator<NatsSubjectSplit> it = copy.iterator();
        assertEquals("a", it.next().getSubject());
        assertEquals("b", it.next().getSubject());
        assertEquals("c", it.next().getSubject());
    }

    @Test
    void deserialize_v1Legacy_onlyReadsSubjects() throws IOException {
        // V1 wire format from NatsSubjectSplitSerializer.serializeV1 is just
        // `writeUTF(splitId)` per split, preceded by the int count.
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeInt(2);
        out.writeUTF("legacy.a");
        out.writeUTF("legacy.b");

        Collection<NatsSubjectSplit> copy = SER.deserialize(1, out.getCopyOfBuffer());

        assertEquals(2, copy.size());
        Iterator<NatsSubjectSplit> it = copy.iterator();
        assertEquals("legacy.a", it.next().getSubject());
        assertEquals("legacy.b", it.next().getSubject());
    }

    @Test
    void deserialize_unrecognizedVersionThrows() throws IOException {
        byte[] bytes = SER.serialize(Collections.emptyList());
        IOException io = assertThrows(IOException.class, () -> SER.deserialize(999, bytes));
        assertTrue(io.getMessage().contains("999"), io.getMessage());
    }

    @Test
    void nullSplitIdThrows() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new NatsSubjectSplit(null));
        assertTrue(e.getMessage().contains("Subject cannot be null"), e.getMessage());
    }
}
