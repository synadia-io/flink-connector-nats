package io.synadia.io.synadia.flink.source;

import io.synadia.flink.source.enumerator.NatsSourceEnumeratorStateSerializer;
import io.synadia.flink.source.enumerator.NatsSubjectSourceEnumeratorState;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.source.split.NatsSubjectSplitSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StateSerializerTest {

    @Test
    void testSerializeAndDeserialize() throws IOException {
        NatsSubjectSplit nss1 = new NatsSubjectSplit("sub1");
        NatsSubjectSplit nss2 = new NatsSubjectSplit("sub2");
        Set<NatsSubjectSplit> unassignedSplits = new HashSet<>();
        unassignedSplits.add(nss1);
        unassignedSplits.add(nss2);

        NatsSubjectSourceEnumeratorState initialState =
            new NatsSubjectSourceEnumeratorState(unassignedSplits);

        NatsSubjectSplitSerializer splitSerializer = new NatsSubjectSplitSerializer();
        NatsSourceEnumeratorStateSerializer serializer =
            new NatsSourceEnumeratorStateSerializer(splitSerializer);

        byte[] serialized = serializer.serialize(initialState);
        NatsSubjectSourceEnumeratorState deserializedState =
            serializer.deserialize(serializer.getVersion(), serialized);

        unassignedSplits = deserializedState.getUnassignedSplits();
        assertEquals(2, unassignedSplits.size());
        assertTrue(unassignedSplits.contains(nss1));
        assertTrue(unassignedSplits.contains(nss2));
    }
}
