// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import io.synadia.payload.PayloadSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class NatsSink<InputT> implements Sink<InputT> {
    private final List<String> subjects;
    private final Properties sinkProperties;
    private final PayloadSerializer<InputT> payloadSerializer;

    NatsSink(List<String> subjects, Properties sinkProperties, PayloadSerializer<InputT> payloadSerializer) {
        this.subjects = subjects;
        this.sinkProperties = sinkProperties;
        this.payloadSerializer = payloadSerializer;
    }

    @Override
    public SinkWriter<InputT> createWriter(InitContext context) throws IOException {
        return new NatsWriter<>(subjects, sinkProperties, payloadSerializer, context);
    }
}
