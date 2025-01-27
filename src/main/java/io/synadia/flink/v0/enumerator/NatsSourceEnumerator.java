// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
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
    private List<List<NatsSubjectSplit>> precomputedSplitAssignments;



    public NatsSourceEnumerator(String sourceId,
                                SplitEnumeratorContext<NatsSubjectSplit> context,
                                Collection<NatsSubjectSplit> splits)
    {
        id = generatePrefixedId(sourceId);
        this.context = checkNotNull(context);
        this.remainingSplits = splits == null ? new ArrayDeque<>() : new ArrayDeque<>(splits);
        this.precomputedSplitAssignments = Collections.synchronizedList(new LinkedList<>());
        LOG.debug("{} | init {}", id, remainingSplits);
    }

    @Override
    public void start() {
        int totalSplits = remainingSplits.size();
        int parallelism = context.currentParallelism();

        // Calculate the minimum splits per reader and leftover splits
        int minimumSplitsPerReader = totalSplits / parallelism;
        int leftoverSplits = totalSplits % parallelism;

        // Precompute split assignments
        this.precomputedSplitAssignments = preComputeSplitsAssignments(parallelism, minimumSplitsPerReader, leftoverSplits);
    }

    private List<List<NatsSubjectSplit>> preComputeSplitsAssignments (int parallelism, int minimumSplitsPerReader, int leftoverSplits) {
        List<List<NatsSubjectSplit>> splitAssignments = new ArrayList<>(parallelism);

        // Initialize lists
        for (int i = 0; i < parallelism; i++) {
            splitAssignments.add(new ArrayList<>());
        }

        // Distribute splits evenly among subtasks
        for (int j = 0; j < parallelism; j++) {
            List<NatsSubjectSplit> readerSplits = splitAssignments.get(j);

            // Assign minimum splits to each reader
            for (int i = 0; i < minimumSplitsPerReader && !remainingSplits.isEmpty(); i++) {
                readerSplits.add(remainingSplits.poll());
            }

            // Assign one leftover split if available
            if (leftoverSplits > 0 && !remainingSplits.isEmpty()) {
                readerSplits.add(remainingSplits.poll());
                leftoverSplits--;
            }
        }

        LOG.debug("{} | Precomputed split assignments: {}", id, splitAssignments);
        return splitAssignments;
    }

    @Override
    public void close() {
        LOG.debug("{} | close", id);
        // remove precomputed split assignments if any
        precomputedSplitAssignments.clear();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        int size = precomputedSplitAssignments.size();

        if (size == 0) {
            LOG.debug("{} | No more splits available for subtask {}", id, subtaskId);
            context.signalNoMoreSplits(subtaskId);
        } else {
            // O(1) operation with LinkedList
            // Remove the first element from the list
            // and assign splits to subtask
            List<NatsSubjectSplit> splits = precomputedSplitAssignments.remove(0);
            if (splits.isEmpty()) {
                LOG.debug("{} | Empty split assignment for subtask {}", id, subtaskId);
                context.signalNoMoreSplits(subtaskId);
            } else {

                // Assign splits to subtask
                LOG.debug("{} | Assigning splits {} to subtask {}", id, splits, subtaskId);
                context.assignSplits(new SplitsAssignment<>(Map.of(subtaskId, splits)));
            }
        }
    }

    @Override
    public void addSplitsBack(List<NatsSubjectSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
        LOG.debug("{} | addSplitsBack IN {} {}", id, subtaskId, remainingSplits);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("{} | addReader {}", id, subtaskId);
        handleSplitRequest(subtaskId, null);
    }

    @Override
    public Collection<NatsSubjectSplit> snapshotState(long checkpointId) throws Exception {
        return remainingSplits;
    }
}
