// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.enumerator;

import io.synadia.flink.v0.source.split.ManagedSplit;
import org.apache.flink.annotation.Internal;

import java.util.Set;

/** The state of Managed source enumerator. */
@Internal
public class ManagedSubjectSourceEnumeratorState {
    private final Set<ManagedSplit> unassignedSplits;

    public ManagedSubjectSourceEnumeratorState(Set<ManagedSplit> unassignedSplits) {
        this.unassignedSplits = unassignedSplits;
    }

    public Set<ManagedSplit> getUnassignedSplits() {
        return unassignedSplits;
    }
}
