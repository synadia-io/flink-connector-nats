// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Serializes and deserializes the {@link NatsSubjectSplit}. This class needs to handle
 * deserializing splits from older versions.
 */
@Internal
public class NatsSubjectCheckpointSerializer implements SimpleVersionedSerializer<Collection<NatsSubjectSplit>> {

    public static final int CURRENT_VERSION = 2;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Collection<NatsSubjectSplit> splits) throws IOException {
        int startSize = 4; // account for first value number of splits
        for (NatsSubjectSplit split : splits) {
            startSize += split.splitId().length();
        }
        final DataOutputSerializer out = new DataOutputSerializer(startSize);
        out.writeInt(splits.size());
        for (NatsSubjectSplit split : splits) {
            NatsSubjectSplitSerializer.serializeV2(out, split);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public Collection<NatsSubjectSplit> deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final int num = in.readInt();
        final ArrayList<NatsSubjectSplit> result = new ArrayList<>(num);

        if (version > 2 ) {
            throw new IOException("Unrecognized version or corrupt state: " + version);
        }

        if (version == 1) {
            for (int x = 0; x < num; x++) {
                result.add(NatsSubjectSplitSerializer.deserializeV1(in));
            }

            return result;
        }

        for (int x = 0; x < num; x++) {
            result.add(NatsSubjectSplitSerializer.deserializeV2(in));
        }

        return result;
    }
}
