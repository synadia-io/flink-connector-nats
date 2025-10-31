// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink.writer;

import io.nats.client.Connection;
import io.synadia.flink.message.SinkConverter;
import io.synadia.flink.message.SinkMessage;
import io.synadia.flink.utils.ConnectionContext;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static io.synadia.flink.utils.MiscUtils.generatePrefixedId;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class NatsSinkWriter<InputT> implements SinkWriter<InputT>, Serializable {

    /**
     * The sink id
     */
    protected final String sinkId;

    /**
     * The subjects
     */
    protected final List<String> subjects;

    /**
     * The connection factory
     */
    protected final ConnectionFactory connectionFactory;

    /**
     * The sink converter
     */
    protected final SinkConverter<InputT> sinkConverter;

    /**
     * The flink context. Not used.
     */
    protected final WriterInitContext writerInitContext;

    /**
     * The sink writer's id
     */
    protected final String id;

    /**
     * The connection context
     */
    protected transient ConnectionContext ctx;

    /**
     * Create a NatsSinkWriter
     * @param sinkId the id for the sink
     * @param subjects the subjects
     * @param sinkConverter the converter
     * @param connectionFactory the connection factory
     * @param writerInitContext the context, unused
     * @throws IOException if there is an IO exception getting the connection context
     */
    public NatsSinkWriter(String sinkId,
                          List<String> subjects,
                          SinkConverter<InputT> sinkConverter,
                          ConnectionFactory connectionFactory,
                          WriterInitContext writerInitContext) throws IOException {
        this.sinkId = sinkId;
        this.id = generatePrefixedId(sinkId);
        this.subjects = Collections.unmodifiableList(subjects);
        this.sinkConverter = sinkConverter;
        this.connectionFactory = connectionFactory;
        this.writerInitContext = writerInitContext;
        this.ctx = connectionFactory.getConnectionContext();
    }

    /**
     * Get the id
     * @return the id
     */
    public String getId() {
        return id;
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        SinkMessage sm = sinkConverter.convert(element);
        if (sm != null) {
            for (String subject : subjects) {
                ctx.connection.publish(subject, null, sm.headers, sm.payload);
            }
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

    /**
     * {@inheritDoc}
     */
    @Serial
    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        ctx = connectionFactory.getConnectionContext();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
            "sinkId='" + sinkId + '\'' +
            ", id='" + id + '\'' +
            ", subjects=" + subjects +
            '}';
    }
}
