// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink;

import io.nats.client.Connection;
import io.synadia.flink.common.ConnectionFactory;
import io.synadia.flink.payload.PayloadSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;

import static io.synadia.flink.Utils.generateId;

/**
 * This class is responsible to publish to one or more NATS subjects
 * @param <InputT> The type of the input elements.
 */
public class NatsSinkWriter<InputT> implements SinkWriter<InputT>, Serializable {

    private final List<String> subjects;
    private final ConnectionFactory connectionFactory;
    private final PayloadSerializer<InputT> payloadSerializer;
    private final Sink.InitContext sinkInitContext;

    private transient String id;
    private transient Connection connection;

    public NatsSinkWriter(List<String> subjects,
                          PayloadSerializer<InputT> payloadSerializer,
                          ConnectionFactory connectionFactory,
                          Sink.InitContext sinkInitContext) throws IOException {
        this.id = generateId();
        this.subjects = subjects;
        this.payloadSerializer = payloadSerializer;
        this.connectionFactory = connectionFactory;
        this.sinkInitContext = sinkInitContext;
        connection = connectionFactory.connect();
    }

    @Override
    public void write(InputT element, SinkWriter.Context context) throws IOException, InterruptedException {
        byte[] payload = payloadSerializer.getBytes(element);
        for (String subject : subjects) {
            connection.publish(subject, null, null, payload);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (connection.getStatus() == Connection.Status.CONNECTED) {
            connection.flushBuffer();
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        id = generateId();
        connection = connectionFactory.connect();
    }

    public String getId() {
        return id;
    }

    public List<String> getSubjects() {
        return subjects;
    }
}
