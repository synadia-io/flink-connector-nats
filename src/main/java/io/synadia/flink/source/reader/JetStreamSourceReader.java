// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.reader;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.nats.client.impl.AckType;
import io.synadia.flink.payload.MessageRecord;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.source.split.JetStreamSplit;
import io.synadia.flink.source.split.JetStreamSplitMessage;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.synadia.flink.utils.MiscUtils.generatePrefixedId;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class JetStreamSourceReader<OutputT> implements SourceReader<OutputT, JetStreamSplit> {
    private static final byte[] ACK_BODY_BYTES = AckType.AckAck.bodyBytes(-1);

    private final String id;
    private final boolean bounded;
    private final ConnectionFactory connectionFactory;
    private final PayloadDeserializer<OutputT> payloadDeserializer;
    private final Map<String, JetStreamSourceReaderSplit> splitMap;
    private final FutureCompletingBlockingQueue<JetStreamSplitMessage> queue;
    private final CompletableFuture<Void> availableFuture;
    private final ExecutorService scheduler;

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
        availableFuture = CompletableFuture.completedFuture(null);
        scheduler = Executors.newCachedThreadPool();
    }

    @Override
    public void start() {
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
        JetStreamSourceReaderSplit srSplit = splitMap.get(sm.splitId);
        if (!srSplit.isFinished()) {
            // 1. collect the message
            // 2. mark the message as emitted - this increments the count
            // 3. if bounded check to see if
            output.collect(payloadDeserializer.getObject(new MessageRecord(sm.message)));
            long emittedCount = srSplit.markEmitted(sm.message);
            if (bounded && emittedCount >= srSplit.getMaxMessagesToRead()) {
                // this split has fulfilled it's bound. Not all splits reader necessarily have yet
                // so only say END_OF_INPUT if all are done
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
        List<JetStreamSplit> splits = new ArrayList<>();
        for (JetStreamSourceReaderSplit srSplit : splitMap.values()) {
            srSplit.takeSnapshot(checkpointId);
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
                try {
                    StreamContext sc = connectionFactory.connectContext().js.getStreamContext(split.subjectConfig.streamName);
                    BaseConsumerContext consumerContext = split.subjectConfig.ack
                        ? createConsumer(split, sc)
                        : createOrderedConsumer(split, sc);

                    ConsumeOptions consumeOptions = split.subjectConfig.consumeOptions.getConsumeOptions();
                    MessageHandler messageHandler = msg -> queue.put(1, new JetStreamSplitMessage(split.splitId(), msg));
                    MessageConsumer consumer = consumerContext.consume(consumeOptions, messageHandler);

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
            .ackPolicy(AckPolicy.All)
            .filterSubject(split.subjectConfig.subject);
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
        for (JetStreamSourceReaderSplit srSplit : splitMap.values()) {
            JetStreamSourceReaderSplit.Snapshot snapshot = srSplit.removeSnapshot(checkpointId);
            if (snapshot != null && srSplit.ack()) {
                // Manual ack since we don't have the message, just the reply_to
                // but we know that this \/ is what an ack is
                // Also we execute as a task so as not to slow down the reader
                // This is probably not perfect, but the whole acking thing is questionable anyway...
                scheduler.execute(() -> connection.publish(snapshot.replyTo, ACK_BODY_BYTES));
            }
        }
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        // N/A
    }

    @Override
    public void close() throws Exception {
        for (JetStreamSourceReaderSplit srSplit : splitMap.values()) {
            srSplit.consumer.stop();
        }
        connection.close();
    }
}
