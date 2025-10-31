// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.split;

import io.nats.client.Message;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class NatsSubjectSplit implements SourceSplit {

    private final String subject;

    private final List<Message> currentMessages;

    /**
     * Construct a NatsSubjectSplit
     * @param subject the split subject
     */
    public NatsSubjectSplit(String subject) {
        this(subject, null);
    }

    /**
     * Construct a NatsSubjectSplit
     * @param subject the split subject
     * @param currentMessages the messages that are currently part of the split
     */
    public NatsSubjectSplit(String subject, List<Message> currentMessages){
        this.subject = subject;

        // Synchronization is required because record emission and snapshotState occur on different threads.
        // we need to flush this list once state is captured since we do ack-ing once state is captured.
        this.currentMessages = Collections.synchronizedList(new ArrayList<>());

        if (currentMessages != null && !currentMessages.isEmpty()) {
            this.currentMessages.addAll(currentMessages);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String splitId() {
        return subject;
    }

    /**
     * get the split's subject
     * @return the subject
     */
    public String getSubject() {
        return subject;
    }

    /**
     * Get the split's messages
     * @return the subject
     */
    public List<Message> getCurrentMessages(){ return currentMessages; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NatsSubjectSplit that = (NatsSubjectSplit) o;

        return subject.equals(that.subject);
    }

    @Override
    public int hashCode() {
        return subject.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s", subject);
    }
}
