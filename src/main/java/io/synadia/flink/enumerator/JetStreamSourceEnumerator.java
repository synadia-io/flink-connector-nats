// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.enumerator;

import io.synadia.flink.source.split.JetStreamSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import static io.synadia.flink.utils.MiscUtils.generatePrefixedId;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class JetStreamSourceEnumerator implements SplitEnumerator<JetStreamSplit, Collection<JetStreamSplit>> {
    private final String id;
    private final SplitEnumeratorContext<JetStreamSplit> context;
    private final Queue<JetStreamSplit> remainingSplits;
    private static final Logger LOG = LoggerFactory.getLogger(JetStreamSourceEnumerator.class);

    public JetStreamSourceEnumerator(String sourceId,
                                     SplitEnumeratorContext<JetStreamSplit> context,
                                     Collection<JetStreamSplit> splits)
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

        final JetStreamSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        }
        else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<JetStreamSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        handleSplitRequest(subtaskId, null);
    }

    @Override
    public Collection<JetStreamSplit> snapshotState(long checkpointId) throws Exception {
        return remainingSplits;
    }
}
