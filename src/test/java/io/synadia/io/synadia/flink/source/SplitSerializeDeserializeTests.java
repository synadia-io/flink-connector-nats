// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.io.synadia.flink.source;

import io.synadia.flink.source.split.NatsSubjectCheckpointSerializer;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.source.split.NatsSubjectSplitSerializer;
import io.synadia.io.synadia.flink.TestBase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SplitSerializeDeserializeTests extends TestBase {

    static String[] SUBJECTS = new String[]{"one", "two", "three", "four", "five"};

    @Test
    public void testSerializer() throws Exception {
        NatsSubjectSplitSerializer splitSerializer = new NatsSubjectSplitSerializer();
        NatsSubjectCheckpointSerializer checkpointSerializer = new NatsSubjectCheckpointSerializer();

        List<NatsSubjectSplit> splits = new ArrayList<>();
        for (String subject : SUBJECTS) {
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
}
