// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.split;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Serializes and deserializes the {@link NatsSubjectSplit}. This class needs to handle
 * deserializing splits from older versions.
 */
@Internal
public class NatsSubjectSplitSerializer implements SimpleVersionedSerializer<NatsSubjectSplit> {

    public static final int CURRENT_VERSION = 2;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(NatsSubjectSplit split) throws IOException {
        final DataOutputSerializer out =
            new DataOutputSerializer(split.splitId().length());
        serializeV2(out, split);
        return out.getCopyOfBuffer();
    }

    public static void serializeV1(DataOutputView out, NatsSubjectSplit split) throws IOException {
        out.writeUTF(split.splitId());
    }

    public static void serializeV2(DataOutputView out, NatsSubjectSplit split) throws IOException {
        if (split.splitId() == null) {
            throw new IOException("Split ID cannot be null");
        }

        out.writeUTF(split.splitId());
        out.writeInt(split.getCurrentMessages().size());
        for (Message message : split.getCurrentMessages()) {
            serializeNatsMessage(out, message);
        }
    }

    @Override
    public NatsSubjectSplit deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        // check version
        // handle older versions
        if (version == 1) {
            return deserializeV1(in);
        } else if (version == 2) {
            return deserializeV2(in);
        } else {
            throw new IOException("Unrecognized version or corrupted state: " + version);
        }
    }

    static NatsSubjectSplit deserializeV1(DataInputView in) throws IOException {
        return new NatsSubjectSplit(in.readUTF());
    }

    static NatsSubjectSplit deserializeV2(DataInputView in) throws IOException {
        String subject = in.readUTF();
        List<Message> messages = new ArrayList<>();
        int numOfMessages = in.readInt();
        for (int i = 0; i < numOfMessages; i++) {
            messages.add(deserializeNatsMessage(in));
        }

        return new NatsSubjectSplit(subject, messages);
    }

    // Deserialize individual NATS Message
    private static Message deserializeNatsMessage(DataInputView in) throws IOException {
        // Deserialize subject
        String subject = in.readBoolean() ? in.readUTF() : null;

        Headers headers = in.readBoolean()? new Headers() : null;
        if (headers != null) {
            deserializeHeaders(in, headers);
        }

        // Deserialize replyTo
        String replyTo = in.readBoolean() ? in.readUTF() : null;

        // Deserialize data
        int dataLength = in.readInt();
        byte[] data = null;
        if (dataLength != -1) {
            data = new byte[dataLength];
            in.readFully(data);
        }

        NatsMessage.Builder builder = NatsMessage.builder();
        builder.subject(subject);

        if (data != null) {
            builder.data(data);
        }

        if (replyTo != null) {
            builder.replyTo(replyTo);
        }

        if (headers != null) {
            builder.headers(headers);
        }

        return builder.build();
    }

    // Serialize individual NATS Message
    private static void serializeNatsMessage(DataOutputView out, Message message) throws IOException {
        // Serialize subject
        if (message.getSubject() == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(message.getSubject());
        }

        // serialize headers
        if (message.getHeaders() == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            serializeHeaders(out, message);
        }

        // Serialize replyTo
        if (message.getReplyTo() == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(message.getReplyTo());
        }

        // Serialize data (payload)
        if (message.getData() == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(message.getData().length);
            out.write(message.getData());
        }
    }

    private static void serializeHeaders(DataOutputView out, Message message) throws IOException {
        Set<String> keys = message.getHeaders().keySet();
        out.writeInt(keys.size());

        // serialize headers
        for (String key : keys) {
            out.writeUTF(key);

            // serialize header value
            List<String> values = message.getHeaders().get(key);
            out.writeInt(values.size());

            for (String value : values) {
                out.writeUTF(value);
            }
        }
    }

    private static void deserializeHeaders(DataInputView in, Headers headers) throws IOException {
        // Deserialize headers
        int numOfKeys = in.readInt();

        for (int i = 0; i < numOfKeys; i++) {
            String key = in.readUTF();
            List<String> values = new ArrayList<>();

            int numOfValues = in.readInt();
            for (int j = 0; j < numOfValues; j++) {

                String value = in.readUTF();
                values.add(value);
            }

            // add back
            headers.add(key, values);
        }
    }
}


