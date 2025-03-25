// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink;

import io.synadia.flink.payload.PayloadSerializer;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.util.List;

import static io.synadia.flink.utils.MiscUtils.generateId;

/**
 * Flink Sink to publish data to one or more NATS subjects
 * @param <InputT> the type of object from the source to convert for publishing
 */
public class NatsSink<InputT> implements Sink<InputT> {
    protected final String id;
    protected final List<String> subjects;
    protected final PayloadSerializer<InputT> payloadSerializer;
    protected final ConnectionFactory connectionFactory;

    protected NatsSink(List<String> subjects,
             PayloadSerializer<InputT> payloadSerializer,
             ConnectionFactory connectionFactory)
    {
        id = generateId();
        this.subjects = subjects;
        this.payloadSerializer = payloadSerializer;
        this.connectionFactory = connectionFactory;
    }

    @SuppressWarnings("deprecation")
    @Override
    public SinkWriter<InputT> createWriter(InitContext context) throws IOException {
        // THIS IS DEPRECATED and won't be called, createWriter(WriterInitContext context) is called
        // Docs said to implement anyway so this is what I implemented
        return createWriter((WriterInitContext)null);
    }

    @Override
    public SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        return new NatsSinkWriter<>(id, subjects, payloadSerializer, connectionFactory, context);
    }

    @Override
    public String toString() {
        return "NatsSink{" +
            "id='" + id + '\'' +
            ", subjects=" + subjects +
            ", payloadSerializer=" + payloadSerializer.getClass().getCanonicalName() +
            ", connectionFactory=" + connectionFactory +
            '}';
    }
}
