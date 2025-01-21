// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.emitter;

import io.nats.client.Message;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.split.NatsSubjectSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NatsRecordEmitter<OutputT>
        implements RecordEmitter<Message, OutputT, NatsSubjectSplitState> {

    private final PayloadDeserializer<OutputT> payloadDeserializer;
    private static final Logger LOG = LoggerFactory.getLogger(NatsRecordEmitter.class);

    public NatsRecordEmitter(PayloadDeserializer<OutputT> payloadDeserializer) {
        this.payloadDeserializer = payloadDeserializer;
    }

    @Override
    public void emitRecord(Message element,
                           SourceOutput<OutputT> output,
                           NatsSubjectSplitState splitState)
            throws Exception {

        try {
            // Deserialize the message and send it to output.
            output.collect(payloadDeserializer.getObject(splitState.getSplit().getSubject(), element.getData(), element.getHeaders(), element.getReplyTo()));
        } catch (Exception e) {
            LOG.error("Failed to deserialize message", e);
            throw new FlinkRuntimeException("Failed to deserialize message", e);
        }

        splitState.getSplit().getCurrentMessages().add(element);
    }
}

