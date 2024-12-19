// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.sink.writer;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.synadia.flink.v0.payload.PayloadSerializer;
import io.synadia.flink.v0.utils.ConnectionContext;
import io.synadia.flink.v0.utils.ConnectionFactory;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;

import static io.synadia.flink.v0.utils.MiscUtils.generatePrefixedId;

/**
 * This class is responsible to publish to one or more NATS subjects
 * @param <InputT> The type of the input elements.
 */
public class NatsJetStreamSinkWriter<InputT> implements SinkWriter<InputT>, Serializable {

    private final String sinkId;
    private final List<String> subjects;
    private final ConnectionFactory connectionFactory;
    private final PayloadSerializer<InputT> payloadSerializer;
    private final Sink.InitContext sinkInitContext;

    private transient String id;
    private transient ConnectionContext connCtx;

    public NatsJetStreamSinkWriter(String sinkId,
                                   List<String> subjects,
                                   PayloadSerializer<InputT> payloadSerializer,
                                   ConnectionFactory connectionFactory,
                                   Sink.InitContext sinkInitContext) throws IOException {
        this.sinkId = sinkId;
        this.id = generatePrefixedId(sinkId);
        this.subjects = subjects;
        this.payloadSerializer = payloadSerializer;
        this.connectionFactory = connectionFactory;
        this.sinkInitContext = sinkInitContext;
        connCtx = connectionFactory.connectContext();
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        byte[] payload = payloadSerializer.getBytes(element);
        for (String subject : subjects) {
            try {
                connCtx.js.publish(subject, null, payload);
            }
            catch (JetStreamApiException e) {
                throw new FlinkRuntimeException(e);
            }
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (connCtx.connection.getStatus() == Connection.Status.CONNECTED) {
            connCtx.connection.flushBuffer();
        }
    }

    @Override
    public void close() throws Exception {
        connCtx.connection.close();
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        id = generatePrefixedId(sinkId);
        connCtx = connectionFactory.connectContext();
    }

    @Override
    public String toString() {
        return "NatsSinkWriter{" +
            "id='" + id + '\'' +
            ", subjects=" + subjects +
            '}';
    }
}
