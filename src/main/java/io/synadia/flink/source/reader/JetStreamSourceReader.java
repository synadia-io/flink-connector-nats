// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.reader;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.nats.client.impl.AckType;
import io.nats.client.support.SerializableConsumeOptions;
import io.synadia.flink.message.SourceConverter;
import io.synadia.flink.source.AckBehavior;
import io.synadia.flink.source.split.JetStreamSplit;
import io.synadia.flink.source.split.JetStreamSplitMessage;
import io.synadia.flink.utils.ConnectionContext;
import io.synadia.flink.utils.ConnectionFactory;
import io.synadia.flink.utils.MiscUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import static io.nats.client.ConsumeOptions.DEFAULT_CONSUME_OPTIONS;
import static io.synadia.flink.utils.Constants.FLINK_CONSUMER_PREFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class JetStreamSourceReader<OutputT> implements SourceReader<OutputT, JetStreamSplit> {
    private static final byte[] ACK_BODY_BYTES = AckType.AckAck.bodyBytes(-1);

    private final boolean bounded;
    private final ConnectionFactory connectionFactory;
    private final SourceConverter<OutputT> sourceConverter;
    private final Map<String, JetStreamSourceReaderSplit> splitMap;
    private final FutureCompletingBlockingQueue<JetStreamSplitMessage> queue;
    private final ExecutorService scheduler;
    private final ReentrantLock connectionLock;

    private int activeSplits;
    private ConnectionContext _connectionContext;

    public JetStreamSourceReader(Boundedness boundedness,
                                 SourceConverter<OutputT> sourceConverter,
                                 ConnectionFactory connectionFactory,
                                 SourceReaderContext readerContext
    ) {
        this.bounded = boundedness == Boundedness.BOUNDED;
        this.sourceConverter = sourceConverter;
        this.connectionFactory = connectionFactory;
        connectionLock = new ReentrantLock();

        checkNotNull(readerContext); // it's not used but is supposed to be provided

        splitMap = new HashMap<>();
        queue = new FutureCompletingBlockingQueue<>();
        scheduler = Executors.newCachedThreadPool();
    }

    @Override
    public void start() {
        getConnectionContext();
    }

    private ConnectionContext getConnectionContext() {
        connectionLock.lock();
        try {
            if (_connectionContext == null) {
                try {
                    _connectionContext = connectionFactory.getConnectionContext();
                }
                catch (IOException e) {
                    throw new FlinkRuntimeException(e);
                }
            }
            return _connectionContext;
        }
        finally {
            connectionLock.unlock();
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OutputT> output) throws Exception {
        JetStreamSplitMessage sm = queue.poll();
        if (sm == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        // 1. Get the split
        // 2. The split could be finished, but more messages came into the queue. These will just be ignored.
        JetStreamSourceReaderSplit readerSplit = splitMap.get(sm.splitId);
        if (!readerSplit.isFinished()) {
            // 1. collect the message
            // 2. mark the message as emitted - this increments the count
            // 3. if bounded, check to see if
            output.collect(sourceConverter.convert(sm.message));
            long emittedCount = readerSplit.markEmitted(sm.message);
            if (bounded && emittedCount >= readerSplit.split.subjectConfig.maxMessagesToRead) {
                // This split has fulfilled it's bound. Not all splits readers necessarily have yet,
                // so only say END_OF_INPUT if all are done
                readerSplit.done();
                if (--activeSplits < 1) {
                    return InputStatus.END_OF_INPUT;
                }
            }
        }

        // 1. This just is faster than checking the queue size as the queue size check locks
        // 2. It might give false positives, but that's fine
        return queue.getAvailabilityFuture() == FutureCompletingBlockingQueue.AVAILABLE
            ? InputStatus.MORE_AVAILABLE
            : InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<JetStreamSplit> snapshotState(long checkpointId) {
        List<JetStreamSplit> splits = new ArrayList<>();
        for (JetStreamSourceReaderSplit srSplit : splitMap.values()) {
            srSplit.takeSnapshot(checkpointId);
            splits.add(srSplit.split);
        }
        return Collections.unmodifiableList(splits);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return queue.getAvailabilityFuture();
    }

    @Override
    public void addSplits(List<JetStreamSplit> splits) {
        for (JetStreamSplit split : splits) {
            if (!splitMap.containsKey(split.splitId()) && !split.finished.get()) {
                try {
                    StreamContext sc = connectionFactory.getConnectionContext().js.getStreamContext(split.subjectConfig.streamName);
                    BaseConsumerContext consumerContext = split.subjectConfig.ackBehavior == AckBehavior.NoAck
                        ? createOrderedConsumer(split, sc)
                        : createConsumer(split, sc);

                    SerializableConsumeOptions sco = split.subjectConfig.serializableConsumeOptions;
                    ConsumeOptions consumeOptions = sco == null ? DEFAULT_CONSUME_OPTIONS : sco.getConsumeOptions();
                    MessageHandler messageHandler = msg -> queue.put(1, new JetStreamSplitMessage(split.splitId(), msg));
                    io.nats.client.MessageConsumer consumer = consumerContext.consume(consumeOptions, messageHandler);

                    JetStreamSourceReaderSplit srSplit =
                        new JetStreamSourceReaderSplit(split, consumerContext, consumer);
                    splitMap.put(split.splitId(), srSplit);
                    activeSplits++;
                }
                catch (Exception e) {
                    throw new FlinkRuntimeException(e);
                }
            }
        }
    }

    private BaseConsumerContext createConsumer(JetStreamSplit split, StreamContext sc) throws JetStreamApiException, IOException {
        ConsumerConfiguration.Builder b = ConsumerConfiguration.builder()
            .ackPolicy(split.subjectConfig.ackBehavior.ackPolicy)
            .filterSubject(split.subjectConfig.subject);

        if (split.subjectConfig.ackWait != null) {
            b.ackWait(split.subjectConfig.ackWait);
        }

        if (MiscUtils.provided(split.subjectConfig.consumerName)) {
            // Add Flink prefix to consumer name for identification
            String prefixedConsumerName = FLINK_CONSUMER_PREFIX + split.subjectConfig.consumerName;

            b.name(prefixedConsumerName);
            b.durable(prefixedConsumerName);

            // delivery policy is not set for durable consumers
            // once created, it cannot be changed
            // nats server maintains the last delivered sequence
            sc.createOrUpdateConsumer(b.build());
        }

        long lastSeq = split.lastEmittedStreamSequence.get();
        if (lastSeq > 0) {
            b.deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(lastSeq + 1);
        }
        else {
            b.deliverPolicy(split.subjectConfig.deliverPolicy);
            if (split.subjectConfig.deliverPolicy == DeliverPolicy.ByStartSequence) {
                b.startSequence(split.subjectConfig.startSequence);
            }
            else if (split.subjectConfig.deliverPolicy == DeliverPolicy.ByStartTime) {
                b.startTime(split.subjectConfig.startTime);
            }
        }
        return sc.createOrUpdateConsumer(b.build());
    }

    private BaseConsumerContext createOrderedConsumer(JetStreamSplit split, StreamContext sc) throws JetStreamApiException, IOException {
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
        return sc.createOrderedConsumer(ocConfig);
    }

    @Override
    public void notifyNoMoreSplits() {
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        ConnectionContext connectionContext  = getConnectionContext();
        for (JetStreamSourceReaderSplit srSplit : splitMap.values()) {
            JetStreamSourceReaderSplit.Snapshot snapshot = srSplit.removeSnapshot(checkpointId);
            if (snapshot != null && srSplit.split.subjectConfig.ackBehavior == AckBehavior.AckAll) {
                // AckBehavior.AckAll is the only behavior that we ack.
                // All other behaviors are either AckPolicy.None or left for the sink to deal with.
                // Manual ack since we don't have the message.
                // Use the original message's "reply_to" since this is where the ack info is kept.
                // Also, we execute as a task so as not to slow down the reader
                // This is probably not perfect, but the whole acking thing is questionable anyway...
                scheduler.execute(() -> connectionContext.connection.publish(snapshot.replyTo, ACK_BODY_BYTES));
            }
        }
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        // N/A
    }

    @Override
    public void close() throws Exception {
        connectionLock.lock();
        try {
            if (_connectionContext != null && _connectionContext.connection != null) {
                for (JetStreamSourceReaderSplit srSplit : splitMap.values()) {
                    srSplit.consumer.stop();
                }
                _connectionContext.connection.close();
            }
        }
        finally {
            _connectionContext = null;
            connectionLock.unlock();
        }
    }
}
