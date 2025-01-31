// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.reader;

import io.nats.client.JetStreamApiException;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.split.ManagedSplit;
import io.synadia.flink.v0.utils.ConnectionFactory;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.io.IOException;

public class ManagedBoundSourceReader<OutputT> extends ManagedSourceReaderBase<OutputT> {

    public ManagedBoundSourceReader(String sourceId,
                                    ConnectionFactory connectionFactory,
                                    PayloadDeserializer<OutputT> payloadDeserializer,
                                    SourceReaderContext readerContext) {
        super(sourceId, connectionFactory, payloadDeserializer, readerContext, ManagedBoundSourceReader.class);
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OutputT> output) throws Exception {
        return null; // TODO
    }

    @Override
    protected void subAddSplits(ManagedSplit split, ConsumerHolder holder, OrderedConsumerConfiguration occ) throws JetStreamApiException, IOException {
        holder.consumer = holder.consumerContext.consume(m -> {
            receivedMessages.put(1, new SplitAwareMessage(split.splitId(), m));
        });
        // TODO
        // holder.consumer = holder.consumerContext.fetch();
    }
}
