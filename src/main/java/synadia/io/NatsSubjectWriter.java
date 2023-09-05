// Copyright (c) 2023 Synadia Communications Inc.  All Rights Reserved.

package synadia.io;

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
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class NatsSubjectWriter<InputT> implements SinkWriter<InputT>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(NatsSubjectWriter.class);

    private final String subject;
    private final Properties connectionOptionProps;
    private final NatsPayloadSerializer<InputT> natsPayloadSerializer;
    private final Sink.InitContext sinkInitContext;
    private transient Connection connection;

    public NatsSubjectWriter(String subject,
                             Properties connectionOptionProps,
                             NatsPayloadSerializer<InputT> natsPayloadSerializer,
                             Sink.InitContext sinkInitContext)
    {
        this.subject = subject;
        this.connectionOptionProps = checkNotNull(connectionOptionProps);
        this.natsPayloadSerializer = checkNotNull(natsPayloadSerializer);
//        this.sinkInitContext = checkNotNull(sinkInitContext, "sinkInitContext");
        this.sinkInitContext = null;

        createConnection(connectionOptionProps);
    }

    private void createConnection(Properties connectionOptionProps) {
        try {
            connection = Nats.connect(new Options.Builder().properties(connectionOptionProps).build());
            System.out.println("Connection Created: " + connection.getServerInfo());
        }
        catch (Exception e) {
            throw new FlinkRuntimeException("Cannot connect to NATS server.", e);
        }
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        System.out.println("WRITE " + element);
        connection.publish(subject, natsPayloadSerializer.getBytes(element, context));
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
