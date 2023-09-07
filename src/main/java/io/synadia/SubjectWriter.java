// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class SubjectWriter<InputT> implements SinkWriter<InputT>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SubjectWriter.class);

    private final List<String> subjects;
    private final Properties connectionOptionProps;
    private final PayloadSerializer<InputT> payloadSerializer;
    private final Sink.InitContext sinkInitContext;

    private transient Connection connection;

    public SubjectWriter(List<String> subjects,
                         Properties connectionOptionProps,
                         PayloadSerializer<InputT> payloadSerializer,
                         Sink.InitContext sinkInitContext)
    {
        this.subjects = subjects;
        this.connectionOptionProps = checkNotNull(connectionOptionProps);
        this.payloadSerializer = checkNotNull(payloadSerializer);
        this.sinkInitContext = sinkInitContext;

        createConnection(connectionOptionProps);
    }

    private void createConnection(Properties connectionOptionProps) {
        try {
            connection = Nats.connect(new Options.Builder().properties(connectionOptionProps).build());
        }
        catch (Exception e) {
            throw new FlinkRuntimeException("Cannot connect to NATS server.", e);
        }
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        byte[] payload = payloadSerializer.getBytes(element, context);
        for (String subject : subjects) {
            connection.publish(subject, payload);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        connection.flushBuffer();
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        createConnection(connectionOptionProps);
    }
}
