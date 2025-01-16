// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.split;

import io.nats.client.Message;
import org.apache.flink.api.connector.source.SourceSplit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NatsSubjectSplit implements SourceSplit {

    private final String subject;

    private final List<Message> currentMessages;

    public NatsSubjectSplit(String subject) {
        this(subject, null);
    }

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

    public String getSubject() {
        return subject;
    }

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
        return String.format("[Subject: %s]", subject);
    }
}
