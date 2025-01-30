// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.enumerator;

import io.synadia.flink.v0.source.split.ManagedSplit;
import io.synadia.flink.v0.source.split.ManagedSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.VersionMismatchException;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class ManagedSourceEnumeratorStateSerializer
    implements SimpleVersionedSerializer<ManagedSubjectSourceEnumeratorState> {

    private static final int CURRENT_VERSION = 0;

    private final ManagedSplitSerializer splitSerializer;

    public ManagedSourceEnumeratorStateSerializer(ManagedSplitSerializer splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(ManagedSubjectSourceEnumeratorState enumState) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {

            out.writeInt(enumState.getUnassignedSplits().size());
            out.writeInt(splitSerializer.getVersion());
            for (ManagedSplit split : enumState.getUnassignedSplits()) {
                byte[] serializedSplit = splitSerializer.serialize(split);
                out.writeInt(serializedSplit.length);
                out.write(serializedSplit);
            }

            out.flush();

            return baos.toByteArray();
        }
    }

    @Override
    public ManagedSubjectSourceEnumeratorState deserialize(int version, byte[] serializedEnumeratorState) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedEnumeratorState);
             DataInputStream in = new DataInputStream(bais)) {

            if (version != getVersion()) {
                throw new VersionMismatchException(
                    "Trying to deserialize ManagedSubjectSourceEnumeratorState serialized with unsupported version "
                        + version
                        + ". Serializer version is "
                        + getVersion());
            }

            final int numUnassignedSplits = in.readInt();
            final int splitSerializerVersion = in.readInt();
            if (splitSerializerVersion != splitSerializer.getVersion()) {
                throw new VersionMismatchException(
                    "Trying to deserialize ManagedSplit serialized with unsupported version "
                        + splitSerializerVersion
                        + ". Serializer version is "
                        + splitSerializer.getVersion());
            }
            Set<ManagedSplit> unassignedSplits = new HashSet<>(numUnassignedSplits);
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

            return new ManagedSubjectSourceEnumeratorState(unassignedSplits);
        }
    }
}
