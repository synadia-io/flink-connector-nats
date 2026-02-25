// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import javax.annotation.Nullable;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class NatsSourceEnumerator<SplitT extends SourceSplit> implements SplitEnumerator<SplitT, Collection<SplitT>> {
    private final SplitEnumeratorContext<SplitT> context;
    private final Queue<SplitT> initialSplits;
    private final List<List<SplitT>> splitAssignments;

    /**
     * Construct the NatsSourceEnumerator
     * @param context the context
     * @param splits the splits
     */
    public NatsSourceEnumerator(SplitEnumeratorContext<SplitT> context,
                                Collection<SplitT> splits) {
        this.context = checkNotNull(context);
        this.initialSplits = splits == null ? new ArrayDeque<>() : new ArrayDeque<>(splits);
        this.splitAssignments = Collections.synchronizedList(new LinkedList<>());
    }

    @Override
    public void start() {
        int totalSplits = initialSplits.size();
        int parallelism = context.currentParallelism();

        // Calculate the minimum splits per reader and leftover splits
        int minimumSplitsPerReader = totalSplits / parallelism;
        int leftoverSplits = totalSplits % parallelism;

        // Precompute split assignments
        splitAssignments.clear();
        splitAssignments.addAll(preComputeSplitsAssignments(parallelism, minimumSplitsPerReader, leftoverSplits));
    }

    private List<List<SplitT>> preComputeSplitsAssignments (int parallelism, int minimumSplitsPerReader, int leftoverSplits) {
        List<List<SplitT>> computed = new ArrayList<>(parallelism);

        // Initialize lists
        for (int i = 0; i < parallelism; i++) {
            computed.add(new ArrayList<>());
        }

        // Distribute splits evenly among subtasks
        for (int j = 0; j < parallelism; j++) {
            List<SplitT> readerSplits = computed.get(j);

            // Assign minimum splits to each reader
            for (int i = 0; i < minimumSplitsPerReader && !initialSplits.isEmpty(); i++) {
                readerSplits.add(initialSplits.poll());
            }

            // Assign one leftover split if available
            if (leftoverSplits > 0 && !initialSplits.isEmpty()) {
                readerSplits.add(initialSplits.poll());
                leftoverSplits--;
            }
        }

        return computed;
    }

    @Override
    public void close() {
        // remove precomputed split assignments if any
        splitAssignments.clear();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        int size = splitAssignments.size();

        if (size == 0) {
            context.signalNoMoreSplits(subtaskId);
        } else {
            // O(1) operation using LinkedList
            // Remove the first element from the list
            // This is the only place where splits are assigned,
            // and this list is the only one that should be snapshotted
            List<SplitT> splits = splitAssignments.remove(0);
            if (splits.isEmpty()) {
                context.signalNoMoreSplits(subtaskId);
            } else {

                // Assign splits to subtask
                context.assignSplits(new SplitsAssignment<>(Map.of(subtaskId, splits)));
            }
        }
    }

    /**
     * Add splits back to the split enumerator. This will only happen when a SourceReader
     * fails and there are splits assigned to it after the last successful checkpoint.
     *
     * @param splits The splits to add back to the enumerator for reassignment.
     * @param subtaskId The id of the subtask to which the returned splits belong.
     */
    @Override
    public void addSplitsBack(List<SplitT> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            // Add the failed reader splits back for reassignment.
            splitAssignments.add(new ArrayList<>(splits));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        handleSplitRequest(subtaskId, null);
    }

    @Override
    public Collection<SplitT> snapshotState(long checkpointId) throws Exception {
        synchronized (splitAssignments) {
            List<SplitT> state = new ArrayList<>();
            for (List<SplitT> pending : splitAssignments) {
                state.addAll(pending);
            }
            return state;
        }
    }
}
