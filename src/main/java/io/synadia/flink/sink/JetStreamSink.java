// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink;

import io.synadia.flink.message.SinkConverter;
import io.synadia.flink.sink.writer.JetStreamSinkWriter;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.util.List;

/**
 * Flink Sink to publish data to one or more NATS JetStream subjects
 * @param <InputT> the type of object from the source to convert for publishing
 */
public class JetStreamSink<InputT> extends NatsSink<InputT> {

    JetStreamSink(List<String> subjects,
                  SinkConverter<InputT> sinkConverter,
                  ConnectionFactory connectionFactory)
    {
        super(subjects, sinkConverter, connectionFactory);
    }

    @Override
    public SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        return new JetStreamSinkWriter<>(id, subjects, sinkConverter, connectionFactory, context);
    }

    @Override
    public String toString() {
        return "JetStreamSink{" +
            "id='" + id + '\'' +
            ", subjects=" + subjects +
            ", sinkConverter=" + sinkConverter.getClass().getCanonicalName() +
            ", connectionFactory=" + connectionFactory +
            '}';
    }
}
