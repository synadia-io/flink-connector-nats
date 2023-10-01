// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.source.enumerator;

import io.synadia.flink.source.split.NatsSubjectSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class NatsSourceEnumerator implements SplitEnumerator<NatsSubjectSplit, Collection<NatsSubjectSplit>> {
    private static final Logger LOG = LoggerFactory.getLogger(NatsSourceEnumerator.class);

    private final SplitEnumeratorContext<NatsSubjectSplit> context;
    private final Queue<NatsSubjectSplit> remainingSplits;

    public NatsSourceEnumerator(SplitEnumeratorContext<NatsSubjectSplit> context,
                                Collection<NatsSubjectSplit> splits)
    {
        LOG.debug("construct " + splits);
        this.context = checkNotNull(context);
        this.remainingSplits = splits == null ? new ArrayDeque<>() : new ArrayDeque<>(splits);
    }

    @Override
    public void start() {
        LOG.debug("start");
    }

    @Override
    public void close() {
        LOG.debug("close");
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        final NatsSubjectSplit nextSplit = remainingSplits.poll();
        LOG.debug("handleSplitRequest | subtaskId:" + subtaskId + " | " + nextSplit);
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<NatsSubjectSplit> splits, int subtaskId) {
        LOG.debug("addSplitsBack | splits:" + (splits == null ? -1 : splits.size()) + " | subtaskId:" + subtaskId);
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("addReader | subtaskId:" + subtaskId);
        handleSplitRequest(subtaskId, null);
    }

    @Override
    public Collection<NatsSubjectSplit> snapshotState(long checkpointId) throws Exception {
        LOG.debug("snapshotState | checkpointId:" + checkpointId);
        return remainingSplits;
    }
}
