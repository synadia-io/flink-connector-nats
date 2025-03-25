// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.enumerator;

import io.synadia.flink.source.split.JetStreamSplit;
import org.apache.flink.annotation.Internal;

import java.util.Set;

/** The state of Managed source enumerator. */
@Internal
public class JetStreamSubjectSourceEnumeratorState {
    private final Set<JetStreamSplit> unassignedSplits;

    public JetStreamSubjectSourceEnumeratorState(Set<JetStreamSplit> unassignedSplits) {
        this.unassignedSplits = unassignedSplits;
    }

    public Set<JetStreamSplit> getUnassignedSplits() {
        return unassignedSplits;
    }
}
