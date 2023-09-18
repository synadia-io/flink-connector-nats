// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.sink;

import io.synadia.common.ConnectionFactory;
import io.synadia.common.NatsSubjectsConnection;
import io.synadia.payload.PayloadSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.util.List;

/**
 * Flink Sink to publish data to one or more NATS subjects
 * @param <InputT> the type of object from the source to convert for publishing
 */
public class NatsSink<InputT> extends NatsSubjectsConnection implements Sink<InputT> {
    private final PayloadSerializer<InputT> payloadSerializer;

    NatsSink(List<String> subjects,
             PayloadSerializer<InputT> payloadSerializer,
             ConnectionFactory connectionFactory)
    {
        super(subjects, connectionFactory);
        this.payloadSerializer = payloadSerializer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SinkWriter<InputT> createWriter(InitContext context) throws IOException {
        return new NatsSinkWriter<>(subjects, payloadSerializer, connectionFactory, context);
    }

    /**
     * Get the payload serializer registered for this sink
     * @return the serializer
     */
    public PayloadSerializer<InputT> getPayloadSerializer() {
        return payloadSerializer;
    }
}
