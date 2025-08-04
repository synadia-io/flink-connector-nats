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
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static io.synadia.flink.utils.MiscUtils.generatePrefixedId;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class NatsSinkWriter<InputT> implements SinkWriter<InputT>, Serializable {

    protected final String sinkId;
    protected final List<String> subjects;
    protected final ConnectionFactory connectionFactory;
    protected final SinkConverter<InputT> sinkConverter;
    protected final WriterInitContext writerInitContext;

    protected final String id;
    protected transient ConnectionContext ctx;

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
