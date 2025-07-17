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
    public final JetStreamSplit split;
    public final BaseConsumerContext consumerContext;
    public final MessageConsumer consumer;
    public final Map<Long, Snapshot> snapshots; // snapshot by checkpoint id

    public static class Snapshot {
        public final String replyTo;
        public final long streamSequence;

        public Snapshot(JetStreamSplit split) {
            this.replyTo = split.lastEmittedMessageReplyTo.get();
            this.streamSequence = split.lastEmittedStreamSequence.get();
        }
    }

    public JetStreamSourceReaderSplit(JetStreamSplit split, BaseConsumerContext consumerContext, MessageConsumer consumer) {
        this.split = split;
        this.consumerContext = consumerContext;
        this.consumer = consumer;
        snapshots = new ConcurrentHashMap<>();
    }

    public long markEmitted(Message message) {
        return split.markEmitted(message);
    }

    public void takeSnapshot(long checkpointId) {
        if (split.lastEmittedStreamSequence.get() != -1) {
            snapshots.put(checkpointId, new Snapshot(split));
        }
    }

    public Snapshot removeSnapshot(long checkpointId) {
        return snapshots.remove(checkpointId);
    }

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

    public boolean isFinished() {
        return split.finished.get();
    }
}
