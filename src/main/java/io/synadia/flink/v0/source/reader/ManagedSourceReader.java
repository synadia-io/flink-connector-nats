// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.reader;

import io.nats.client.*;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.synadia.flink.v0.payload.MessageRecord;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.split.ManagedSplit;
import io.synadia.flink.v0.utils.ConnectionFactory;
import io.synadia.flink.v0.utils.Debug;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static io.synadia.flink.v0.utils.MiscUtils.generatePrefixedId;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class ManagedSourceReader<OutputT> implements SourceReader<OutputT, ManagedSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(ManagedSourceReader.class);

    private final String id;
    private final ConnectionFactory connectionFactory;
    private final PayloadDeserializer<OutputT> payloadDeserializer;
    private final SourceReaderContext readerContext;
    private final Map<String, ManagedSplit> subbedSplitMap;
    private final Map<String, ConsumerHolder> consumerMap;
    private final FutureCompletingBlockingQueue<SplitAwareMessage> receivedMessages;
    private Connection connection;

    static class SplitAwareMessage {
        String splitId;
        Message natsMessage;

        public SplitAwareMessage(String splitId, Message natsMessage) {
            this.splitId = splitId;
            this.natsMessage = natsMessage;
        }
    }

    public ManagedSourceReader(String sourceId,
                               ConnectionFactory connectionFactory,
                               PayloadDeserializer<OutputT> payloadDeserializer,
                               SourceReaderContext readerContext) {
        id = generatePrefixedId(sourceId);
        this.connectionFactory = connectionFactory;
        this.payloadDeserializer = payloadDeserializer;
        this.readerContext = checkNotNull(readerContext);
        subbedSplitMap = new HashMap<>();
        consumerMap = new HashMap<>();
        receivedMessages = new FutureCompletingBlockingQueue<>();
    }

    @Override
    public void start() {
        LOG.debug("{} | start", id);
        try {
            connection = connectionFactory.connect();
        }
        catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OutputT> output) throws Exception {
        SplitAwareMessage m = receivedMessages.poll();
        if (m == null) {
            LOG.debug("{} | pollNext no message NOTHING_AVAILABLE", id);
            return InputStatus.NOTHING_AVAILABLE;
        }
        output.collect(payloadDeserializer.getObject(new MessageRecord(m.natsMessage)));
        ManagedSplit split = subbedSplitMap.get(m.splitId);
        split.setLastEmittedStreamSequence(m.natsMessage.metaData().streamSequence());
        InputStatus is = receivedMessages.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
        LOG.debug("{} | pollNext had message, then {}", id, is);
        return is;
    }

    @Override
    public List<ManagedSplit> snapshotState(long checkpointId) {
        LOG.debug("{} | snapshotState", id);
        return Collections.unmodifiableList(new ArrayList<>(subbedSplitMap.values()));
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return receivedMessages.getAvailabilityFuture();
    }

    static class ConsumerHolder {
        OrderedConsumerContext ctx;
        MessageConsumer consumer;
        public ConsumerHolder(OrderedConsumerContext ctx) {
            this.ctx = ctx;
        }
    }

    @Override
    public void addSplits(List<ManagedSplit> splits) {
        for (ManagedSplit split : splits) {
            if (!subbedSplitMap.containsKey(split.splitId())) {
                OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration()
                    .filterSubject(split.subjectConfig.subject);
                long lastSeq = split.lastEmittedStreamSequence.get();
                if (lastSeq > 0) {
                    occ.deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(lastSeq + 1);
                }
                else {
                    occ.deliverPolicy(split.subjectConfig.deliverPolicy);
                    if (split.subjectConfig.deliverPolicy == DeliverPolicy.ByStartSequence) {
                        occ.startSequence(split.subjectConfig.startSequence);
                    }
                    else if (split.subjectConfig.deliverPolicy == DeliverPolicy.ByStartTime) {
                        occ.startTime(split.subjectConfig.startTime);
                    }
                }
                LOG.debug("{} | addSplits {} {}", id, split, occ.toJson());
                try {
                    StreamContext sc = connection.getStreamContext(split.subjectConfig.streamName);
                    ConsumerHolder holder = new ConsumerHolder(sc.createOrderedConsumer(occ));
                    holder.consumer = holder.ctx.consume(m -> {
                        receivedMessages.put(1, new SplitAwareMessage(split.splitId(), m));
                    });
                    consumerMap.put(split.subjectConfig.configId, holder);
                    subbedSplitMap.put(split.splitId(), split);
                }
                catch (Exception e) {
                    throw new FlinkRuntimeException(e);
                }
            }
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.debug("{} | notifyNoMoreSplits", id);
    }

    @Override
    public void close() throws Exception {
        LOG.debug("{} | close", id);
        Debug.stackTrace("CLOSE");
        connection.close();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        LOG.debug("{} | handleSourceEvents {}", id, sourceEvent);
    }
}
