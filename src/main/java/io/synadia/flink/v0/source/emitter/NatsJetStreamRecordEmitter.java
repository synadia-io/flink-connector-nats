// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.emitter;

import io.nats.client.Message;
import io.synadia.flink.payload.MessageRecord;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.split.NatsSubjectSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class NatsJetStreamRecordEmitter<OutputT>
        implements RecordEmitter<Message, OutputT, NatsSubjectSplitState> {

    private final PayloadDeserializer<OutputT> payloadDeserializer;

    public NatsJetStreamRecordEmitter(PayloadDeserializer<OutputT> payloadDeserializer) {
        this.payloadDeserializer = payloadDeserializer;
    }

    @Override
    public void emitRecord(Message element,
                           SourceOutput<OutputT> output,
                           NatsSubjectSplitState splitState)
            throws Exception {

        // Deserialize the message and send it to output.
        output.collect(payloadDeserializer.getObject(new MessageRecord(element)));
        splitState.getSplit().getCurrentMessages().add(element);
    }
}

