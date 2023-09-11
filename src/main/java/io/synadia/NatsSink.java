// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import io.synadia.payload.PayloadSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 *
 * @param <InputT>
 */
public class NatsSink<InputT> implements Sink<InputT> {
    private final List<String> subjects;
    private final Properties sinkProperties;
    private final PayloadSerializer<InputT> payloadSerializer;

    NatsSink(List<String> subjects, Properties sinkProperties, PayloadSerializer<InputT> payloadSerializer) {
        this.subjects = subjects;
        this.sinkProperties = sinkProperties;
        this.payloadSerializer = payloadSerializer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SinkWriter<InputT> createWriter(InitContext context) throws IOException {
        return new NatsWriter<>(subjects, sinkProperties, payloadSerializer, context);
    }

    /**
     * Get the subjects registered for this sink.
     * @return the subjects.
     */
    public List<String> getSubjects() {
        return subjects;
    }

    /**
     * Get the properties registered for this sink
     * @return a copy of the properties object
     */
    public Properties getSinkProperties() {
        return new Properties(sinkProperties);
    }

    /**
     * Get the payload serializer registered for this sink
     * @return the serializer
     */
    public PayloadSerializer<InputT> getPayloadSerializer() {
        return payloadSerializer;
    }
}
