// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.sink.writer;

import io.nats.client.Connection;
import io.synadia.flink.v0.payload.PayloadSerializer;
import io.synadia.flink.v0.utils.ConnectionContext;
import io.synadia.flink.v0.utils.ConnectionFactory;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;

import static io.synadia.flink.v0.utils.MiscUtils.generatePrefixedId;

/**
 * This class is responsible to publish to one or more NATS subjects
 * @param <InputT> The type of the input elements.
 */
public class NatsSinkWriter<InputT> implements SinkWriter<InputT>, Serializable {

    protected final String sinkId;
    protected final List<String> subjects;
    protected final ConnectionFactory connectionFactory;
    protected final PayloadSerializer<InputT> payloadSerializer;
    protected final Sink.InitContext sinkInitContext;

    protected transient String id;
    protected transient ConnectionContext ctx;

    public NatsSinkWriter(String sinkId,
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
        this.ctx = connectionFactory.connectContext();
    }

    @Override
    public void write(InputT element, SinkWriter.Context context) throws IOException, InterruptedException {
        byte[] payload = payloadSerializer.getBytes(element);
        for (String subject : subjects) {
            ctx.connection.publish(subject, null, null, payload);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (ctx.connection.getStatus() == Connection.Status.CONNECTED) {
            ctx.connection.flushBuffer();
        }
    }

    @Override
    public void close() throws Exception {
        ctx.connection.close();
    }

    protected void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        id = generatePrefixedId(sinkId);
        ctx = connectionFactory.connectContext();
    }

    @Override
    public String toString() {
        return "NatsSinkWriter{" +
            "id='" + id + '\'' +
            ", subjects=" + subjects +
            '}';
    }
}
