// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

public class NatsSubjectSourceSplitSerializer implements SimpleVersionedSerializer<NatsSubjectSourceSplit> {

    public static final NatsSubjectSourceSplitSerializer INSTANCE = new NatsSubjectSourceSplitSerializer();

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
        ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(NatsSubjectSourceSplit split) throws IOException {
        checkArgument(
            split.getClass() == NatsSubjectSourceSplit.class,
            "Cannot serialize subclasses of NatsSubjectSourceSplit");

        // optimization: the splits lazily cache their own serialized form
        if (split.serializedFormCache != null) {
            return split.serializedFormCache;
        }

        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        out.writeUTF(split.splitId());
        out.writeUTF(split.getSubject());

        final byte[] result = out.getCopyOfBuffer();
        out.clear();

        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        split.serializedFormCache = result;

        return result;
    }

    @Override
    public NatsSubjectSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version == 1) {
            return deserializeV1(serialized);
        }
        throw new IOException("Unknown version: " + version);
    }

    private static NatsSubjectSourceSplit deserializeV1(byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        final String id = in.readUTF();
        final String subject = in.readUTF();

        // instantiate a new split and cache the serialized form
        return new NatsSubjectSourceSplit(id, subject);
    }
}
