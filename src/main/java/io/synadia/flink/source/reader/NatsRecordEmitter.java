// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.source.reader;

import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.split.NatSubjectStateSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class NatsRecordEmitter<OutputT> implements RecordEmitter<byte[], OutputT, NatSubjectStateSplitState> {
    private final PayloadDeserializer<OutputT> payloadDeserializer;

    public NatsRecordEmitter(PayloadDeserializer<OutputT> payloadDeserializer) {
        this.payloadDeserializer = payloadDeserializer;
    }

    @Override
    public void emitRecord(byte[] bytes, SourceOutput<OutputT> sourceOutput, NatSubjectStateSplitState natSubjectStateSplitState) throws Exception {
        sourceOutput.collect(payloadDeserializer.getObject(bytes));
    }
}
