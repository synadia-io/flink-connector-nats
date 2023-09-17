// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.source;

import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;
import java.io.Serializable;

import static io.synadia.Utils.generateId;

public class NatsSubjectSourceSplit implements SourceSplit, Serializable {
    private static final long serialVersionUID = 1L;

    private final String id;
    private final String subject;

    /**
     * The splits are frequently serialized into checkpoints. Caching the byte representation makes
     * repeated serialization cheap. This field is used by {@link NatsSubjectSourceSplitSerializer}.
     */
    @Nullable
    transient byte[] serializedFormCache;

    public NatsSubjectSourceSplit(String subject) {
        this.id = generateId();
        this.subject = subject;
    }

    public NatsSubjectSourceSplit(String id, String subject) {
        this.id = id;
        this.subject = subject;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String splitId() {
        return id;
    }

    /**
     * Get the split id of this source split.
     * @return id of this source split.
     */
    public String getId() {
        return id;
    }

    /**
     * Get the subject id of this source split.
     * @return subject of this source split.
     */
    public String getSubject() {
        return subject;
    }
}
