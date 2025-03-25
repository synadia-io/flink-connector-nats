// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.reader;

import io.nats.client.*;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.synadia.flink.payload.MessageRecord;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.split.JetStreamSplit;
import io.synadia.flink.source.split.JetStreamSplitMessage;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static io.synadia.flink.utils.MiscUtils.generatePrefixedId;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class JetStreamSourceReader<OutputT> implements SourceReader<OutputT, JetStreamSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(JetStreamSourceReader.class);

    private final String id;
    private final boolean bounded;
    private final ConnectionFactory connectionFactory;
    private final PayloadDeserializer<OutputT> payloadDeserializer;
    private final Map<String, JetStreamSourceReaderSplit<MessageConsumer, BaseConsumerContext>> splitMap;
    private final FutureCompletingBlockingQueue<JetStreamSplitMessage> queue;
    private final CompletableFuture<Void> availableFuture;
    private int activeSplits;
    private Connection connection;

    public JetStreamSourceReader(String sourceId,
                                 Boundedness boundedness,
                                 ConnectionFactory connectionFactory,
                                 PayloadDeserializer<OutputT> payloadDeserializer,
                                 SourceReaderContext readerContext) {
        id = generatePrefixedId(sourceId);
        this.bounded = boundedness == Boundedness.BOUNDED;
        this.connectionFactory = connectionFactory;
        this.payloadDeserializer = payloadDeserializer;
        checkNotNull(readerContext); // it's not used but is supposed to be provided
        splitMap = new HashMap<>();
        queue = new FutureCompletingBlockingQueue<>();
        this.availableFuture = CompletableFuture.completedFuture(null);
        LOG.debug("{} | Init", id);
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
        JetStreamSplitMessage sm = queue.poll();
        if (sm == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        // 1. Get the split
        // 2. The split could be finished but more messages came into the queue. These will just be ignored.
        JetStreamSourceReaderSplit<MessageConsumer, BaseConsumerContext> srSplit = splitMap.get(sm.splitId);
        if (!srSplit.isFinished()) {
            // 1. collect the message
            // 2. mark the message as emitted - this increments the count
            // 3. if bounded check to see if
            output.collect(payloadDeserializer.getObject(new MessageRecord(sm.message)));
            long emittedCount = srSplit.markEmitted(sm.message);
            if (bounded && emittedCount >= srSplit.getMaxMessagesToRead()) {
                // this split has fulfilled it's bound. Not all splits reader necessarily have yet
                // so only say END_OF_INPUT if all are done
                LOG.debug("{} | pollNext {} {} > {}", id, sm.splitId, emittedCount, srSplit.getMaxMessagesToRead());
                srSplit.done();
                if (--activeSplits < 1) {
                    availableFuture.complete(null);
                    return InputStatus.END_OF_INPUT;
                }
            }
        }

        return queue.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<JetStreamSplit> snapshotState(long checkpointId) {
        LOG.debug("{} | snapshotState", id);
        List<JetStreamSplit> splits = new ArrayList<>();
        for (JetStreamSourceReaderSplit<MessageConsumer, BaseConsumerContext> srSplit : splitMap.values()) {
            splits.add(srSplit.split);
        }
        return Collections.unmodifiableList(splits);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availableFuture.isDone() ? availableFuture : queue.getAvailabilityFuture();
    }

    @Override
    public void addSplits(List<JetStreamSplit> splits) {
        for (JetStreamSplit split : splits) {
            if (!splitMap.containsKey(split.splitId()) && !split.finished.get()) {
                OrderedConsumerConfiguration ocConfig = new OrderedConsumerConfiguration()
                    .filterSubject(split.subjectConfig.subject);
                long lastSeq = split.lastEmittedStreamSequence.get();
                if (lastSeq > 0) {
                    ocConfig.deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(lastSeq + 1);
                }
                else {
                    ocConfig.deliverPolicy(split.subjectConfig.deliverPolicy);
                    if (split.subjectConfig.deliverPolicy == DeliverPolicy.ByStartSequence) {
                        ocConfig.startSequence(split.subjectConfig.startSequence);
                    }
                    else if (split.subjectConfig.deliverPolicy == DeliverPolicy.ByStartTime) {
                        ocConfig.startTime(split.subjectConfig.startTime);
                    }
                }
                LOG.debug("{} | addSplits {} {}", id, split, ocConfig.toJson());
                try {
                    StreamContext sc = connection.getStreamContext(split.subjectConfig.streamName);
                    OrderedConsumerContext consumerContext = sc.createOrderedConsumer(ocConfig);
                    MessageConsumer consumer = consumerContext.consume(
                        split.subjectConfig.consumeOptions.getConsumeOptions(),
                        msg -> queue.put(1, new JetStreamSplitMessage(split.splitId(), msg)));
                    JetStreamSourceReaderSplit<MessageConsumer, BaseConsumerContext> srSplit =
                        new JetStreamSourceReaderSplit<>(split, consumerContext, consumer);
                    splitMap.put(split.splitId(), srSplit);
                    activeSplits++;
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
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("{} | notifyCheckpointComplete", id);
        SourceReader.super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        LOG.debug("{} | handleSourceEvents {}", id, sourceEvent);
    }

    @Override
    public void close() throws Exception {
        LOG.debug("{} | close", id);
        for (JetStreamSourceReaderSplit<MessageConsumer, BaseConsumerContext> srSplit : splitMap.values()) {
            srSplit.consumer.stop();
        }
        connection.close();
    }
}
