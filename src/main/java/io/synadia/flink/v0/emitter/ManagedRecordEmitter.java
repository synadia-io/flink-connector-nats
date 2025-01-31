// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.emitter;

import io.nats.client.Message;
import io.synadia.flink.v0.payload.MessageRecord;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.split.ManagedSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class ManagedRecordEmitter<OutputT>
        implements RecordEmitter<Message, OutputT, ManagedSplitState> {

    private final PayloadDeserializer<OutputT> payloadDeserializer;

    public ManagedRecordEmitter(PayloadDeserializer<OutputT> payloadDeserializer) {
        this.payloadDeserializer = payloadDeserializer;
    }

    @Override
    public void emitRecord(Message element,
                           SourceOutput<OutputT> output,
                           ManagedSplitState splitState)
            throws Exception {

        // Deserialize the message and send it to output.
        output.collect(payloadDeserializer.getObject(new MessageRecord(element)));
        splitState.getSplit().setLastEmittedStreamSequence(element.metaData().streamSequence());
    }
}

