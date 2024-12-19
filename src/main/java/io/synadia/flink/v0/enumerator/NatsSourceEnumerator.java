// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.enumerator;

import io.synadia.flink.v0.source.split.NatsSubjectSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import static io.synadia.flink.v0.utils.MiscUtils.generatePrefixedId;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class NatsSourceEnumerator implements SplitEnumerator<NatsSubjectSplit, Collection<NatsSubjectSplit>> {
    private final String id;
    private final SplitEnumeratorContext<NatsSubjectSplit> context;
    private final Queue<NatsSubjectSplit> remainingSplits;

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
    }

    @Override
    public void close() {
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        final NatsSubjectSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        }
        else {
            context.signalNoMoreSplits(subtaskId);
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
