// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.split;

import io.nats.client.Message;
import java.util.ArrayList;
import org.apache.flink.api.connector.source.SourceSplit;
import java.util.List;

public class NatsSubjectSplit implements SourceSplit {

    private final String subject;

    private List<Message> currentMessages = new ArrayList<>();

    public NatsSubjectSplit(String subject) {
        this.subject = subject;
    }

    public NatsSubjectSplit(String subject, List<Message> currentMessages){
        this.subject = subject;
        this.currentMessages = currentMessages;
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
