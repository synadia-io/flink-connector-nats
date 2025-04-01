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
import java.util.List;

/**
 * Serializes and deserializes the {@link JetStreamSplit}. This class needs to handle
 * deserializing splits from older versions.
 */
@Internal
public class JetStreamCheckpointSerializer implements SimpleVersionedSerializer<Collection<JetStreamSplit>> {

    public static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Collection<JetStreamSplit> splits) throws IOException {
        List<String> splitJsons = new ArrayList<>();

        int startSize = 4; // account for first value number of splits
        for (JetStreamSplit split : splits) {
            String splitJson = split.toJson();
            splitJsons.add(splitJson);
            startSize += splitJson.length();
        }
        final DataOutputSerializer out = new DataOutputSerializer(startSize);
        out.writeInt(splits.size());
        for (String splitJson : splitJsons) {
            JetStreamSplitSerializer.serializeV1(out, splitJson);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public Collection<JetStreamSplit> deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unrecognized version: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final int num = in.readInt();
        final ArrayList<JetStreamSplit> result = new ArrayList<>(num);
        for (int x = 0; x < num; x++) {
            result.add(JetStreamSplitSerializer.deserializeV1(in));
        }
        return result;
    }
}
