// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.sink.js;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.api.PublishAck;
import io.synadia.flink.common.ConnectionFactory;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import static io.synadia.flink.Utils.generatePrefixedId;
import static org.apache.flink.optimizer.Optimizer.LOG;

public class NatsJetstreamSinkWriter<InputT> implements SinkWriter<InputT>, Serializable {

    private final String sinkId;
    private final String subject;
    private final ConnectionFactory connectionFactory;
    private final SerializationSchema<InputT> serializer;
    private final Sink.InitContext sinkInitContext;
    private transient String id;
    private transient Connection connection;
    private final NATSStreamConfig config;
    private transient JetStream js;


    public NatsJetstreamSinkWriter(String sinkId, ConnectionFactory connectionFactory, NATSStreamConfig config, SerializationSchema<InputT> payloadSerializer, Sink.InitContext writerContext, String natsSubject) throws IOException {
        this.sinkId = sinkId;
        this.id = generatePrefixedId(sinkId);
        this.subject = natsSubject;
        this.serializer = payloadSerializer;
        this.connectionFactory = connectionFactory;
        this.sinkInitContext = writerContext;
        this.config = config;
        this.connection = connectionFactory.connect();
        this.js = connection.jetStream();
    }

    @Override
    public void write(InputT element, Context context) throws IOException {
        byte[] serializedElement;
        try {
            serializedElement = serializer.serialize(element);
        } catch (Exception e) {
            throw new IOException("Error serializing element", e);
        }

        try {
            PublishAck ack = js.publish(subject, serializedElement);
            LOG.info("Sent message to subject: {} with seq: {}", subject, ack.getSeqno());
        } catch (Exception e) {
            throw new IOException("Failed to publish message to JetStream", e);
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
        id = generatePrefixedId(sinkId);
        connection = connectionFactory.connect();
    }

    @Override
    public String toString() {
        return "NatsJetstreamSinkWriter{" +
                "id='" + id + '\'' +
                ", subjects=" + subject +
                '}';
    }

}

