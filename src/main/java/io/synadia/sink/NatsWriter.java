// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.sink;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.synadia.payload.PayloadSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import static io.synadia.Utils.*;

/**
 * This class is responsible to publish to one or more NATS subjects
 * @param <InputT> The type of the input elements.
 */
public class NatsWriter<InputT> implements SinkWriter<InputT>, Serializable {

    private final List<String> subjects;
    private final Properties connectionProperties;
    private final String connectionPropertiesFile;
    private final PayloadSerializer<InputT> payloadSerializer;
    private final long minConnectionJitter;
    private final long maxConnectionJitter;
    private final Sink.InitContext sinkInitContext;

    private transient String id;
    private transient Connection connection;

    public NatsWriter(List<String> subjects,
                      Properties connectionProperties,
                      String connectionPropertiesFile,
                      PayloadSerializer<InputT> payloadSerializer,
                      long minConnectionJitter,
                      long maxConnectionJitter,
                      Sink.InitContext sinkInitContext) throws IOException {
        this.id = generateId();
        this.subjects = subjects;
        this.connectionProperties = connectionProperties;
        this.connectionPropertiesFile = connectionPropertiesFile;
        this.payloadSerializer = payloadSerializer;
        this.minConnectionJitter = minConnectionJitter;
        this.maxConnectionJitter = maxConnectionJitter;
        this.sinkInitContext = sinkInitContext;
        createConnection();
    }

    private void createConnection() throws IOException {
        Options.Builder builder = new Options.Builder();
        if (connectionPropertiesFile == null) {
            builder = builder.properties(connectionProperties);
        }
        else {
            builder = builder.properties(loadPropertiesFromFile(connectionPropertiesFile));
        }

        try {
            Options options = builder.maxReconnects(0).build();
            jitter(minConnectionJitter, maxConnectionJitter);
            connection = Nats.connect(options);
        }
        catch (Exception e) {
            throw new IOException("Cannot connect to NATS server.", e);
        }

    }

    @Override
    public void write(InputT element, SinkWriter.Context context) throws IOException, InterruptedException {
        byte[] payload = payloadSerializer.getBytes(element, context);
        for (String subject : subjects) {
            connection.publish(subject, payload);
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
        createConnection();
    }

    public String getId() {
        return id;
    }

    public List<String> getSubjects() {
        return subjects;
    }
}
