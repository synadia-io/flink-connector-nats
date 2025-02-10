// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.enumerator;

import io.synadia.flink.v0.source.split.ManagedSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import static io.synadia.flink.v0.utils.MiscUtils.generatePrefixedId;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class ManagedSourceEnumerator implements SplitEnumerator<ManagedSplit, Collection<ManagedSplit>> {
    private final String id;
    private final SplitEnumeratorContext<ManagedSplit> context;
    private final Queue<ManagedSplit> remainingSplits;
    private static final Logger LOG = LoggerFactory.getLogger(ManagedSourceEnumerator.class);

    public ManagedSourceEnumerator(String sourceId,
                                   SplitEnumeratorContext<ManagedSplit> context,
                                   Collection<ManagedSplit> splits)
    {
        id = generatePrefixedId(sourceId);
        this.context = checkNotNull(context);
        this.remainingSplits = splits == null ? new ArrayDeque<>() : new ArrayDeque<>(splits);
    }

    @Override
    public void start() {
    }

    @Override
    public void close() {
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.debug("handleSplitRequest {}", subtaskId);

        final ManagedSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        }
        else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<ManagedSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        handleSplitRequest(subtaskId, null);
    }

    @Override
    public Collection<ManagedSplit> snapshotState(long checkpointId) throws Exception {
        return remainingSplits;
    }
}
