// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.sink.writer;

import io.synadia.flink.v0.payload.*;
import io.synadia.flink.v0.utils.ConnectionFactory;
import org.apache.flink.api.connector.sink2.Sink;
import java.io.IOException;
import java.util.List;

/**
 * This class is responsible to publish to one or more JetStream subjects
 * @param <InputT> The type of the input elements.
 */
public class NatsJetStreamSinkWriter<InputT> extends NatsSinkWriter<InputT> {

    public NatsJetStreamSinkWriter(String sinkId,
                                   List<String> subjects,
                                   PayloadSerializer<InputT> payloadSerializer,
                                   ConnectionFactory connectionFactory,
                                   Sink.InitContext sinkInitContext) throws IOException
    {
        super(sinkId, subjects, payloadSerializer, connectionFactory, sinkInitContext);
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        byte[] payload = payloadSerializer.getBytes(element);

        for (String subject : subjects) {
            ctx.js.publishAsync(subject, null, payload);
        }
    }

    @Override
    public String toString() {
        return "NatsJetStreamSinkWriter{" +
            "id='" + id + '\'' +
            ", subjects=" + subjects +
            '}';
    }
}
