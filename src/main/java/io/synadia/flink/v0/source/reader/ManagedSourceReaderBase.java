// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.reader;

import io.nats.client.*;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.split.ManagedSplit;
import io.synadia.flink.v0.utils.ConnectionFactory;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static io.synadia.flink.v0.utils.MiscUtils.generatePrefixedId;
import static org.apache.flink.util.Preconditions.checkNotNull;

abstract class ManagedSourceReaderBase<OutputT> implements SourceReader<OutputT, ManagedSplit> {
    protected final String id;
    protected final ConnectionFactory connectionFactory;
    protected final PayloadDeserializer<OutputT> payloadDeserializer;
    protected final Map<String, ManagedSplit> splitMap;
    protected final Map<String, ConsumerHolder> consumerMap;
    protected final FutureCompletingBlockingQueue<SplitAwareMessage> receivedMessages;
    protected Connection connection;
    protected final Logger logger;

    protected static class SplitAwareMessage {
        protected String splitId;
        protected Message natsMessage;

        protected SplitAwareMessage(String splitId, Message natsMessage) {
            this.splitId = splitId;
            this.natsMessage = natsMessage;
        }
    }

    protected static class ConsumerHolder {
        protected OrderedConsumerContext consumerContext;
        protected MessageConsumer consumer;

        protected ConsumerHolder(OrderedConsumerContext consumerContext) {
            this.consumerContext = consumerContext;
        }
    }

    protected ManagedSourceReaderBase(String sourceId,
                                      ConnectionFactory connectionFactory,
                                      PayloadDeserializer<OutputT> payloadDeserializer,
                                      SourceReaderContext readerContext,
                                      Class<?> logClazz)
    {
        id = generatePrefixedId(sourceId);
        this.connectionFactory = connectionFactory;
        this.payloadDeserializer = payloadDeserializer;
        checkNotNull(readerContext); // it's not used but is supposed to be provided
        splitMap = new HashMap<>();
        consumerMap = new HashMap<>();
        receivedMessages = new FutureCompletingBlockingQueue<>();
        logger = LoggerFactory.getLogger(logClazz.getCanonicalName()); // done this way to get full package path - needed to
        logger.debug("{} | Init", id);
    }

    @Override
    public void start() {
        logger.debug("{} | start", id);
        try {
            connection = connectionFactory.connect();
        }
        catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public List<ManagedSplit> snapshotState(long checkpointId) {
        logger.debug("{} | snapshotState", id);
        return Collections.unmodifiableList(new ArrayList<>(splitMap.values()));
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return receivedMessages.getAvailabilityFuture();
    }

    @Override
    public void addSplits(List<ManagedSplit> splits) {
        for (ManagedSplit split : splits) {
            if (!splitMap.containsKey(split.splitId())) {
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
                logger.debug("{} | addSplits {} {}", id, split, occ.toJson());
                try {
                    StreamContext sc = connection.getStreamContext(split.subjectConfig.streamName);
                    ConsumerHolder holder = new ConsumerHolder(sc.createOrderedConsumer(occ));
                    subAddSplits(split, holder, occ);
                    consumerMap.put(split.splitId(), holder);
                    splitMap.put(split.splitId(), split);
                }
                catch (Exception e) {
                    throw new FlinkRuntimeException(e);
                }
            }
        }
    }

    protected abstract void subAddSplits(ManagedSplit split, ConsumerHolder holder, OrderedConsumerConfiguration occ) throws JetStreamApiException, IOException;

    @Override
    public void notifyNoMoreSplits() {
        logger.debug("{} | notifyNoMoreSplits", id);
    }

    @Override
    public void close() throws Exception {
        logger.debug("{} | close", id);
        for (ConsumerHolder holder : consumerMap.values()) {
            holder.consumer.stop();
        }
        connection.close();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        logger.debug("{} | handleSourceEvents {}", id, sourceEvent);
    }
}
