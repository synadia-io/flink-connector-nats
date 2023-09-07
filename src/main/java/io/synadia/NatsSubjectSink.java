// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class NatsSubjectSink<InputT> implements Sink<InputT> {
    private static final Logger LOG = LoggerFactory.getLogger(NatsSubjectSink.class);

    private final List<String> subjects;
    private final Properties connectionOptionProps;
    private final PayloadSerializer<InputT> payloadSerializer;

    public NatsSubjectSink(PayloadSerializer<InputT> payloadSerializer, Properties connectionOptionProps, String... subjects) {
        this.subjects = Arrays.asList(subjects);
        this.connectionOptionProps = connectionOptionProps;
        this.payloadSerializer = payloadSerializer;
    }

    @Override
    public SinkWriter<InputT> createWriter(InitContext context) throws IOException {

        return new SubjectWriter<>(subjects,
            connectionOptionProps,
            payloadSerializer,
            context);
    }
}
