// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 * Serializes and deserializes the {@link JetStreamSplit}. This class needs to handle
 * deserializing splits from older versions.
 */
@Internal
public class JetStreamSplitSerializer implements SimpleVersionedSerializer<JetStreamSplit> {

    /**
     * The current version constant
     */
    public static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(JetStreamSplit split) throws IOException {
        String json = split.toJson();
        final DataOutputSerializer out = new DataOutputSerializer(json.length());
        serializeV1(out, json);
        return out.getCopyOfBuffer();
    }

    /**
     * Serialize version 1
     * @param out the data output view
     * @param splitJson the json
     * @throws IOException if an I/O error occurs.
     */
    public static void serializeV1(DataOutputView out, String splitJson) throws IOException {
        out.writeUTF(splitJson);
    }

    @Override
    public JetStreamSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unrecognized version: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        return deserializeV1(in);
    }

    /**
     *
     * @param in the Data Input View
     * @return the JetStreamSplit
     * @throws IOException if an I/O error occurs.
     */
    static JetStreamSplit deserializeV1(DataInputView in) throws IOException {
        return new JetStreamSplit(in.readUTF());
    }
}
