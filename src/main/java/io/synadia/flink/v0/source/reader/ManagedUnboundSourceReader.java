// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.reader;

import io.nats.client.JetStreamApiException;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.synadia.flink.v0.payload.MessageRecord;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.split.ManagedSplit;
import io.synadia.flink.v0.utils.ConnectionFactory;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.io.IOException;

public class ManagedUnboundSourceReader<OutputT> extends ManagedSourceReaderBase<OutputT> {

    public ManagedUnboundSourceReader(String sourceId,
                                      ConnectionFactory connectionFactory,
                                      PayloadDeserializer<OutputT> payloadDeserializer,
                                      SourceReaderContext readerContext) {
        super(sourceId, connectionFactory, payloadDeserializer, readerContext, ManagedUnboundSourceReader.class);
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OutputT> output) throws Exception {
        SplitAwareMessage m = receivedMessages.poll();
        if (m == null) {
            logger.debug("{} | pollNext no message NOTHING_AVAILABLE", id);
            return InputStatus.NOTHING_AVAILABLE;
        }
        output.collect(payloadDeserializer.getObject(new MessageRecord(m.natsMessage)));
        ManagedSplit split = splitMap.get(m.splitId);
        split.setLastEmittedStreamSequence(m.natsMessage.metaData().streamSequence());
        InputStatus is = receivedMessages.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
        logger.debug("{} | pollNext had message, then {}", id, is);
        return is;
    }

    @Override
    protected void subAddSplits(ManagedSplit split, ManagedSourceReaderBase.ConsumerHolder holder, OrderedConsumerConfiguration occ) throws JetStreamApiException, IOException {
        holder.consumer = holder.consumerContext.consume(m -> {
            receivedMessages.put(1, new SplitAwareMessage(split.splitId(), m));
        });
    }
}
