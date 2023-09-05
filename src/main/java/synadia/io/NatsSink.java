// Copyright (c) 2023 Synadia Communications Inc.  All Rights Reserved.

package synadia.io;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class NatsSink<InputT> implements Sink<InputT> {
    private static final Logger LOG = LoggerFactory.getLogger(NatsSink.class);

    private final String subject;
    private final Properties connectionOptionProps;
    private final NatsPayloadSerializer<InputT> natsPayloadSerializer;

    public NatsSink(String subject, Properties connectionOptionProps, NatsPayloadSerializer<InputT> natsPayloadSerializer) {
        this.subject = subject;
        this.connectionOptionProps = connectionOptionProps;
        this.natsPayloadSerializer = natsPayloadSerializer;
    }

    @Override
    public SinkWriter<InputT> createWriter(InitContext context) throws IOException {
        System.out.println("createWriter");
        return new NatsSubjectWriter<>(subject,
            connectionOptionProps,
            natsPayloadSerializer,
            context);
    }
}
