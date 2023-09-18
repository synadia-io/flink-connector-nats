// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.source.enumerator;

import io.synadia.source.split.NatsSubjectSplit;
import org.apache.flink.annotation.Internal;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** The state of Nats source enumerator. */
@Internal
public class NatsSourceEnumState {
    private final Set<NatsSubjectSplit> assignedSplits;
    private final Set<NatsSubjectSplit> unassignedSplits;

    public NatsSourceEnumState(Set<String> unassigned) {
        this(Collections.emptySet(), unassigned);
    }

    NatsSourceEnumState(Set<String> assigned, Set<String> unassigned) {
        assignedSplits = new HashSet<>();
        for (String s : assigned) {
            assignedSplits.add(new NatsSubjectSplit(s));
        }
        unassignedSplits = new HashSet<>();
        for (String s : unassigned) {
            unassignedSplits.add(new NatsSubjectSplit(s));
        }
    }

    // used for copy method
    private NatsSourceEnumState() {
        this(Collections.emptySet(), Collections.emptySet());
    }

    public NatsSourceEnumState copy() {
        NatsSourceEnumState state = new NatsSourceEnumState();
        state.assignedSplits.addAll(assignedSplits);
        state.unassignedSplits.addAll(unassignedSplits);
        return state;
    }

    public boolean hasUnassigned() {
        return !unassignedSplits.isEmpty();
    }

    public NatsSubjectSplit assign() {
        if (unassignedSplits.isEmpty()) {
            return null;
        }
        NatsSubjectSplit subject = unassignedSplits.iterator().next();
        unassignedSplits.remove(subject);
        assignedSplits.add(subject);
        return subject;
    }

    public void addSplitsBack(List<NatsSubjectSplit> splits) {
        for (NatsSubjectSplit split : splits) {
            if (assignedSplits.remove(split)) {
                unassignedSplits.add(split);
            }
        }
    }

    public Set<NatsSubjectSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public Set<NatsSubjectSplit> getUnassignedSplits() {
        return unassignedSplits;
    }
}
