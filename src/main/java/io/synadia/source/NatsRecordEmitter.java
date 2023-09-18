// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.source;

import io.synadia.payload.PayloadDeserializer;
import io.synadia.source.split.NoStateSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class NatsRecordEmitter<OutputT> implements RecordEmitter<byte[], OutputT, NoStateSplitState> {
    private PayloadDeserializer<OutputT> payloadDeserializer;

    public NatsRecordEmitter(PayloadDeserializer<OutputT> payloadDeserializer) {
        this.payloadDeserializer = payloadDeserializer;
    }

    @Override
    public void emitRecord(byte[] bytes, SourceOutput<OutputT> sourceOutput, NoStateSplitState noStateSplitState) throws Exception {
        sourceOutput.collect(payloadDeserializer.getObject(bytes));
    }
}
