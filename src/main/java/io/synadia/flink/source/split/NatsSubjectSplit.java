// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

public class NatsSubjectSplit implements SourceSplit {

    private final String subject;

    public NatsSubjectSplit(String subject) {
        this.subject = subject;
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
