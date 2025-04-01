// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.reader;

import io.nats.client.BaseConsumerContext;
import io.nats.client.Message;
import io.nats.client.MessageConsumer;
import io.synadia.flink.source.JetStreamSubjectConfiguration;
import io.synadia.flink.source.split.JetStreamSplit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
        snapshots.put(checkpointId, new Snapshot(split));
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

    public long getLastEmittedStreamSequence() {
        return split.lastEmittedStreamSequence.get();
    }

    public long getEmittedCount() {
        return split.emittedCount.get();
    }

    public boolean isFinished() {
        return split.finished.get();
    }

    public JetStreamSubjectConfiguration getSubjectConfig() {
        return split.subjectConfig;
    }

    public boolean ack() {
        return split.subjectConfig.ack;
    }

    public long getMaxMessagesToRead() {
        return split.subjectConfig.maxMessagesToRead;
    }
}
