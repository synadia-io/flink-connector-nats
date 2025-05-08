// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink.writer;

import io.nats.client.JetStreamApiException;
import io.synadia.flink.message.SinkConverter;
import io.synadia.flink.message.SinkMessage;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.List;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class JetStreamSinkWriter<InputT> extends NatsSinkWriter<InputT> {

    public JetStreamSinkWriter(String sinkId,
                               List<String> subjects,
                               SinkConverter<InputT> sinkConverter,
                               ConnectionFactory connectionFactory,
                               WriterInitContext writerInitContext) throws IOException
    {
        super(sinkId, subjects, sinkConverter, connectionFactory, writerInitContext);
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        SinkMessage sm = sinkConverter.convert(element);
        if (sm != null) {
            for (String subject : subjects) {
                try {
                    ctx.js.publish(subject, sm.headers, sm.payload);
                }
                catch (JetStreamApiException e) {
                    throw new FlinkRuntimeException(e);
                }
            }
        }
    }
}
