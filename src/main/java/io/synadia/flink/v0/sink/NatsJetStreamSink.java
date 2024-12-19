// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.sink;

import io.synadia.flink.v0.payload.PayloadSerializer;
import io.synadia.flink.v0.sink.writer.NatsJetStreamSinkWriter;
import io.synadia.flink.v0.utils.ConnectionFactory;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.util.List;

/**
 * Flink Sink to publish data to one or more NATS subjects
 * @param <InputT> the type of object from the source to convert for publishing
 */
public class NatsJetStreamSink<InputT> extends NatsSink<InputT> {

    NatsJetStreamSink(List<String> subjects,
                      PayloadSerializer<InputT> payloadSerializer,
                      ConnectionFactory connectionFactory)
    {
        super(subjects, payloadSerializer, connectionFactory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SinkWriter<InputT> createWriter(InitContext context) throws IOException {
        return new NatsJetStreamSinkWriter<>(id, subjects, payloadSerializer, connectionFactory, context);
    }

    @Override
    public String toString() {
        return "NatsJetStreamSink{" +
            "id='" + id + '\'' +
            ", subjects=" + subjects +
            ", payloadSerializer=" + payloadSerializer.getClass().getCanonicalName() +
            ", connectionFactory=" + connectionFactory +
            '}';
    }
}
