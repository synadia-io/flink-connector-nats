// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.source.enumerator;

import io.synadia.source.split.NatsSubjectSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class NatsSourceEnumStateSerializer
    implements SimpleVersionedSerializer<NatsSourceEnumState> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(NatsSourceEnumState enumState) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos))
        {
            Set<NatsSubjectSplit> asplits = enumState.getAssignedSplits();
            Set<NatsSubjectSplit> usplits = enumState.getUnassignedSplits();
            out.writeInt(asplits.size());
            out.writeInt(usplits.size());
            for (NatsSubjectSplit split : asplits) {
                out.writeUTF(split.getSubject());
            }
            for (NatsSubjectSplit split : usplits) {
                out.writeUTF(split.getSubject());
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public NatsSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        if (version == 1) {
            return deserializeV1(serialized);
        }
        throw new IOException("Unknown version: " + version);
    }

    private static NatsSourceEnumState deserializeV1(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {

            int anum = in.readInt();
            int unum = in.readInt();
            Set<String> assigned = new HashSet<>(anum);
            Set<String> unassigned = new HashSet<>(unum);
            for (int i = 0; i < anum; i++) {
                assigned.add(in.readUTF());
            }
            for (int i = 0; i < unum; i++) {
                unassigned.add(in.readUTF());
            }
            if (in.available() > 0) {
                throw new IOException("Unexpected trailing bytes in serialized topic partitions");
            }
            return new NatsSourceEnumState(assigned, unassigned);
        }
    }
}
