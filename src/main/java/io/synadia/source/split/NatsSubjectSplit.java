// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.source.split;

import io.synadia.source.enumerator.NatsSourceEnumStateSerializer;
import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;
import java.io.Serializable;

public class NatsSubjectSplit implements SourceSplit, Serializable {
    private static final long serialVersionUID = 1L;

    private final String subject;

    /**
     * The splits are frequently serialized into checkpoints. Caching the byte representation makes
     * repeated serialization cheap. This field is used by {@link NatsSourceEnumStateSerializer}.
     */
    @Nullable
    transient byte[] serializedFormCache;

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
