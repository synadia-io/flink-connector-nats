// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.enumerator;

import io.synadia.flink.v0.source.split.NatsSubjectSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

import static io.synadia.flink.v0.utils.MiscUtils.generatePrefixedId;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class NatsSourceEnumerator implements SplitEnumerator<NatsSubjectSplit, Collection<NatsSubjectSplit>> {
    private static final Logger LOG = LoggerFactory.getLogger(NatsSourceEnumerator.class);

    private final String id;
    private final SplitEnumeratorContext<NatsSubjectSplit> context;
    private final Queue<NatsSubjectSplit> remainingSplits;
    private Integer minimumSplitsToAssign = 1;


    public NatsSourceEnumerator(String sourceId,
                                SplitEnumeratorContext<NatsSubjectSplit> context,
                                Collection<NatsSubjectSplit> splits)
    {
        id = generatePrefixedId(sourceId);
        this.context = checkNotNull(context);
        this.remainingSplits = splits == null ? new ArrayDeque<>() : new ArrayDeque<>(splits);
    }

    @Override
    public void start() {
        int noOfSplits = remainingSplits.size();
        int parallelism = context.currentParallelism();

        // minimum splits that needs to be assigned to reader
        this.minimumSplitsToAssign = noOfSplits / parallelism;
    }

    @Override
    public void close() {
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (remainingSplits.isEmpty()) {
            context.signalNoMoreSplits(subtaskId);
            return;
        }

        List<NatsSubjectSplit> nextSplits = new ArrayList<>();
        for (int i = 0; i < this.minimumSplitsToAssign; i++) {
            NatsSubjectSplit nextSplit = remainingSplits.poll();
            nextSplits.add(nextSplit);
        }

        Map<Integer, List<NatsSubjectSplit>> assignedSplits = new HashMap<>();
        assignedSplits.put(subtaskId, nextSplits);

        // assign the splits back to the source reader
        context.assignSplits(new SplitsAssignment<>(assignedSplits));
        LOG.debug("{} | Assigned splits to subtask: {}", id, subtaskId);

        // Perform round-robin assignment for leftover splits
        // Assign only one split at a time since the number of leftover splits will always be less than the parallelism.
        // Each leftover split can be assigned to any reader, and the list will be exhausted quickly.
        NatsSubjectSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
            LOG.debug("{} | Assigned split in round-robin to subtask: {}", id, subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<NatsSubjectSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        handleSplitRequest(subtaskId, null);
    }

    @Override
    public Collection<NatsSubjectSplit> snapshotState(long checkpointId) throws Exception {
        return remainingSplits;
    }
}
