// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.sink.js;

import io.nats.client.NUID;
import io.synadia.flink.common.ConnectionFactory;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class NATSJetstreamSink<InputT> implements Sink<InputT> {

    private final String natsSubject;
    private static final Logger LOG = LoggerFactory.getLogger(NATSJetstreamSink.class);
    private final SerializationSchema<InputT> serializationSchema;
    private final NATSStreamConfig config;
    private final String id;
    private final ConnectionFactory connectionFactory;

    NATSJetstreamSink(SerializationSchema<InputT> serializationSchema, ConnectionFactory connectionFactory, String natsSubject, NATSStreamConfig config) {
        id = NUID.nextGlobal().substring(0, 4).toUpperCase();
        this.serializationSchema = serializationSchema;
        this.connectionFactory = connectionFactory;
        this.natsSubject = natsSubject;
        this.config = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SinkWriter<InputT> createWriter(InitContext writerContext) throws IOException {

        return new NatsJetstreamSinkWriter<>(id, connectionFactory, config, serializationSchema, writerContext,natsSubject);
    }

    @Override
    public String toString() {
        return "NatsSink{" +
                "id='" + id + '\'' +
                ", subjects=" + natsSubject +
                ", serializer=" + serializationSchema.getClass().getCanonicalName() +
                ", connectionFactory=" + connectionFactory +
                '}';
    }
}

