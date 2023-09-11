// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.synadia.payload.PayloadSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import static io.synadia.Constants.SINK_CONNECTION_PROPERTIES_FILE;
import static io.synadia.Utils.loadPropertiesFromFile;

/**
 * This class is responsible to publish to one or more NATS subjects
 *
 * @param <InputT> The type of the input elements.
 */
public class NatsWriter<InputT> implements SinkWriter<InputT>, Serializable {

    private final List<String> subjects;
    private final Properties properties;
    private final PayloadSerializer<InputT> payloadSerializer;
    private final Sink.InitContext sinkInitContext;

    private transient Connection connection;

    public NatsWriter(List<String> subjects,
                      Properties properties,
                      PayloadSerializer<InputT> payloadSerializer,
                      Sink.InitContext sinkInitContext)
    {
        this.subjects = subjects;
        this.properties = properties;
        this.payloadSerializer = payloadSerializer;
        this.sinkInitContext = sinkInitContext;
        createConnection();
    }

    private void createConnection() {
        try {
            String path = properties.getProperty(SINK_CONNECTION_PROPERTIES_FILE);
            Options options;
            if (path == null) {
                options = new Options.Builder().properties(properties).build();
            }
            else {
                options = new Options.Builder().properties(loadPropertiesFromFile(path)).build();
            }
            connection = Nats.connect(options);
        }
        catch (Exception e) {
            throw new FlinkRuntimeException("Cannot connect to NATS server.", e);
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
        connection.flushBuffer();
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        createConnection();
    }
}
