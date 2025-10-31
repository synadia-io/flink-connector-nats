// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.reader;

import io.nats.client.BaseConsumerContext;
import io.nats.client.Message;
import io.nats.client.MessageConsumer;
import io.synadia.flink.source.split.JetStreamSplit;
import org.apache.flink.annotation.Internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class JetStreamSourceReaderSplit {
    /**
     * The split
     */
    public final JetStreamSplit split;

    /**
     * The consumer context
     */
    public final BaseConsumerContext consumerContext;

    /**
     * The simplified message consumer
     */
    public final MessageConsumer consumer;

    /**
     * The snapshot by checkpoint id map
     */
    public final Map<Long, Snapshot> snapshots;

    /**
     * class to represent a Snapshot
     */
    public static class Snapshot {
        /**
         * The reply to
         */
        public final String replyTo;

        /**
         * The stream sequence
         */
        public final long streamSequence;

        /**
         * Construct a Snapshot from a split
         * @param split the split
         */
        public Snapshot(JetStreamSplit split) {
            this.replyTo = split.lastEmittedMessageReplyTo.get();
            this.streamSequence = split.lastEmittedStreamSequence.get();
        }
    }

    /**
     * Construct a JetStreamSourceReaderSplit
     * @param split the split
     * @param consumerContext the consumer context
     * @param consumer the simplified consumer
     */
    public JetStreamSourceReaderSplit(JetStreamSplit split, BaseConsumerContext consumerContext, MessageConsumer consumer) {
        this.split = split;
        this.consumerContext = consumerContext;
        this.consumer = consumer;
        snapshots = new ConcurrentHashMap<>();
    }

    /**
     * mark a message as emitted
     * @param message the message
     * @return the total emitted count
     */
    public long markEmitted(Message message) {
        return split.markEmitted(message);
    }

    /**
     * Take a snapshot for the specified id
     * @param checkpointId the checkpoint id
     */
    public void takeSnapshot(long checkpointId) {
        if (split.lastEmittedStreamSequence.get() != -1) {
            snapshots.put(checkpointId, new Snapshot(split));
        }
    }

    /**
     * remove a snapshot by id
     * @param checkpointId the checkpoint id
     * @return the removed snapshot
     */
    public Snapshot removeSnapshot(long checkpointId) {
        return snapshots.remove(checkpointId);
    }

    /**
     * Mark the split as done
     */
    public void done() {
        split.setFinished();
        consumer.stop();
        try {
            consumer.close();
        }
        catch (Exception ignore) {
            // TODO log maybe?
        }
    }

    /**
     * the finished state of the split
     * @return the state
     */
    public boolean isFinished() {
        return split.finished.get();
    }
}
