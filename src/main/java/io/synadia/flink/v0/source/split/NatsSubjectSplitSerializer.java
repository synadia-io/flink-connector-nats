// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Serializes and deserializes the {@link NatsSubjectSplit}. This class needs to handle
 * deserializing splits from older versions.
 */
@Internal
public class NatsSubjectSplitSerializer implements SimpleVersionedSerializer<NatsSubjectSplit> {

    public static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(NatsSubjectSplit split) throws IOException {
        final DataOutputSerializer out =
            new DataOutputSerializer(split.splitId().length());
        serializeV1(out, split);
        return out.getCopyOfBuffer();
    }

    public static void serializeV1(DataOutputView out, NatsSubjectSplit split) throws IOException {
        out.writeUTF(split.splitId());
    }

    @Override
    public NatsSubjectSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unrecognized version: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        return deserializeV1(in);
    }

    static NatsSubjectSplit deserializeV1(DataInputView in) throws IOException {
        return new NatsSubjectSplit(in.readUTF());
    }
}
