// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.enumerator;

import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.source.split.NatsSubjectSplitSerializer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.VersionMismatchException;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class NatsSourceEnumeratorStateSerializer
    implements SimpleVersionedSerializer<NatsSubjectSourceEnumeratorState> {

    /**
     * The current version constant
     */
    private static final int CURRENT_VERSION = 0;

    private final NatsSubjectSplitSerializer splitSerializer;

    /**
     * Construct the serializers
     * @param splitSerializer the split serializer
     */
    public NatsSourceEnumeratorStateSerializer(NatsSubjectSplitSerializer splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(NatsSubjectSourceEnumeratorState enumState) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {

            out.writeInt(enumState.getUnassignedSplits().size());
            out.writeInt(splitSerializer.getVersion());
            for (NatsSubjectSplit split : enumState.getUnassignedSplits()) {
                byte[] serializedSplit = splitSerializer.serialize(split);
                out.writeInt(serializedSplit.length);
                out.write(serializedSplit);
            }

            out.flush();

            return baos.toByteArray();
        }
    }

    @Override
    public NatsSubjectSourceEnumeratorState deserialize(int version, byte[] serializedEnumeratorState) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedEnumeratorState);
             DataInputStream in = new DataInputStream(bais)) {

            if (version != getVersion()) {
                throw new VersionMismatchException(
                    "Trying to deserialize NatsSubjectSourceEnumeratorState serialized with unsupported version "
                        + version
                        + ". Serializer version is "
                        + getVersion());
            }

            final int numUnassignedSplits = in.readInt();
            final int splitSerializerVersion = in.readInt();
            if (splitSerializerVersion != splitSerializer.getVersion()) {
                throw new VersionMismatchException(
                    "Trying to deserialize NatsSubjectSplit serialized with unsupported version "
                        + splitSerializerVersion
                        + ". Serializer version is "
                        + splitSerializer.getVersion());
            }
            Set<NatsSubjectSplit> unassignedSplits = new HashSet<>(numUnassignedSplits);
            for (int i = 0; i < numUnassignedSplits; i++) {
                int serializedLength = in.readInt();
                byte[] serializedSplit = new byte[serializedLength];
                if (in.read(serializedSplit) != -1) {
                    unassignedSplits.add(
                        splitSerializer.deserialize(splitSerializerVersion, serializedSplit));
                } else {
                    throw new IOException(
                        "Unexpectedly reading more bytes than is present in stream.");
                }
            }

            if (in.available() > 0) {
                throw new IOException("Unexpected trailing bytes when deserializing.");
            }

            return new NatsSubjectSourceEnumeratorState(unassignedSplits);
        }
    }
}
