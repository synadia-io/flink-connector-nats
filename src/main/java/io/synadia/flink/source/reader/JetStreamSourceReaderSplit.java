// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.reader;

import io.nats.client.Message;
import io.nats.client.MessageConsumer;
import io.synadia.flink.source.JetStreamSubjectConfiguration;
import io.synadia.flink.source.split.JetStreamSplit;

public class JetStreamSourceReaderSplit<ConsumerT extends MessageConsumer, BaseConsumerContext> {
    public final JetStreamSplit split;
    public final BaseConsumerContext consumerContext;
    public final MessageConsumer consumer;

    public JetStreamSourceReaderSplit(JetStreamSplit split, BaseConsumerContext consumerContext, MessageConsumer consumer) {
        this.split = split;
        this.consumerContext = consumerContext;
        this.consumer = consumer;
    }

    public long markEmitted(Message message) {
        return split.markEmitted(message);
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

    public long getMaxMessagesToRead() {
        return split.subjectConfig.maxMessagesToRead;
    }
}
