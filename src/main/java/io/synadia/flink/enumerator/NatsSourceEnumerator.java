// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.enumerator;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import javax.annotation.Nullable;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class NatsSourceEnumerator<SplitT extends SourceSplit> implements SplitEnumerator<SplitT, Collection<SplitT>> {
    private final SplitEnumeratorContext<SplitT> context;
    private final Queue<SplitT> remainingSplits;
    private List<List<SplitT>> precomputedSplitAssignments;

    public NatsSourceEnumerator(SplitEnumeratorContext<SplitT> context,
                                Collection<SplitT> splits) {
        this.context = checkNotNull(context);
        this.remainingSplits = splits == null ? new ArrayDeque<>() : new ArrayDeque<>(splits);
        this.precomputedSplitAssignments = Collections.synchronizedList(new LinkedList<>());
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

    private List<List<SplitT>> preComputeSplitsAssignments (int parallelism, int minimumSplitsPerReader, int leftoverSplits) {
        List<List<SplitT>> splitAssignments = new ArrayList<>(parallelism);

        // Initialize lists
        for (int i = 0; i < parallelism; i++) {
            splitAssignments.add(new ArrayList<>());
        }

        // Distribute splits evenly among subtasks
        for (int j = 0; j < parallelism; j++) {
            List<SplitT> readerSplits = splitAssignments.get(j);

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

        return splitAssignments;
    }

    @Override
    public void close() {
        // remove precomputed split assignments if any
        precomputedSplitAssignments.clear();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        int size = precomputedSplitAssignments.size();

        if (size == 0) {
            context.signalNoMoreSplits(subtaskId);
        } else {
            // O(1) operation with LinkedList
            // Remove the first element from the list
            // and assign splits to subtask
            List<SplitT> splits = precomputedSplitAssignments.remove(0);
            if (splits.isEmpty()) {
                context.signalNoMoreSplits(subtaskId);
            } else {

                // Assign splits to subtask
                context.assignSplits(new SplitsAssignment<>(Map.of(subtaskId, splits)));
            }
        }
    }

    @Override
    public void addSplitsBack(List<SplitT> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        handleSplitRequest(subtaskId, null);
    }

    @Override
    public Collection<SplitT> snapshotState(long checkpointId) throws Exception {
        return remainingSplits;
    }
}
