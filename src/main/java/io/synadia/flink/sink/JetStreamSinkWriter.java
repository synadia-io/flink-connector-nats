// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink;

import io.nats.client.JetStreamApiException;
import io.synadia.flink.payload.PayloadSerializer;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.List;

/**
 * This class is responsible to publish to one or more JetStream subjects
 * @param <InputT> The type of the input elements.
 */
public class JetStreamSinkWriter<InputT> extends NatsSinkWriter<InputT> {

    public JetStreamSinkWriter(String sinkId,
                               List<String> subjects,
                               PayloadSerializer<InputT> payloadSerializer,
                               ConnectionFactory connectionFactory,
                               WriterInitContext writerInitContext) throws IOException
    {
        super(sinkId, subjects, payloadSerializer, connectionFactory, writerInitContext);
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        byte[] payload = payloadSerializer.getBytes(element);
        for (String subject : subjects) {
            try {
                ctx.js.publish(subject, null, payload);
            }
            catch (JetStreamApiException e) {
                throw new FlinkRuntimeException(e);
            }
        }
    }
}
