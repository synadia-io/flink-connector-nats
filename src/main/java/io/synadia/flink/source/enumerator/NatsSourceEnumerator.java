// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.source.enumerator;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.support.Debug;
import io.synadia.flink.source.split.NatsSubjectSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class NatsSourceEnumerator<SplitT> implements SplitEnumerator<NatsSubjectSplit, NatsSubjectSourceEnumeratorState> {
    private static final Logger LOG = LoggerFactory.getLogger(NatsSourceEnumerator.class);

    private final SplitEnumeratorContext<NatsSubjectSplit> context;

    private final NatsSubjectSourceEnumeratorState state;
    private final Map<Integer, Set<NatsSubjectSplit>> splitAssignment = new HashMap<>();
    private final Set<String> assignedSplitIds = new HashSet<>();
    private final Set<NatsSubjectSplit> unassignedSplits;

    private Connection connection;
    private Dispatcher dispatcher;

    public NatsSourceEnumerator(SplitEnumeratorContext<NatsSubjectSplit> context,
                                Set<String> subjects)
    {
        this.context = context;
        this.state = null; // new NatsSubjectSourceEnumeratorState(subjects);
        unassignedSplits = new HashSet<>();
    }

//    public NatsSourceEnumerator(ConnectionFactory connectionFactory,
//                                SplitEnumeratorContext<NatsSubjectSplit> context,
//                                NatsSubjectSourceEnumeratorState natsSubjectSourceEnumeratorState)
//    {
//        this.connectionFactory = connectionFactory;
//        this.context = context;
//        this.state = natsSubjectSourceEnumeratorState;
//    }

    @Override
    public void start() {
        Debug.dbg("NatsSourceEnumerator start");
    }

//    private List<NatsSubjectSplit> periodicallyDiscoverSplits() {
//        return mapToSplits(shards, InitialPosition.TRIM_HORIZON);
//    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        Debug.dbg("NatsSourceEnumerator.start | subtaskId:" + subtaskId + " | requesterHostname:" + requesterHostname);
        NatsSubjectSplit split = null;
        if (split == null) {
            context.signalNoMoreSplits(subtaskId);
        }
        else {
            context.assignSplit(split, subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<NatsSubjectSplit> splits, int subtaskId) {
        Debug.dbg("NatsSourceEnumerator.addSplitsBack | splits:" + (splits == null ? -1 : splits.size()) + " | subtaskId:" + subtaskId);
        if (!splitAssignment.containsKey(subtaskId)) {
            LOG.warn(
                "Unable to add splits back for subtask {} since it is not assigned any splits. Splits: {}",
                subtaskId,
                splits);
            return;
        }
//        state.addSplitsBack(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        Debug.dbg("NatsSourceEnumerator.start | subtaskId:" + subtaskId);
        splitAssignment.putIfAbsent(subtaskId, new HashSet<>());
    }

    @Override
    public NatsSubjectSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        Debug.dbg("NatsSourceEnumerator.snapshotState | checkpointId:" + checkpointId);
        return new NatsSubjectSourceEnumeratorState(unassignedSplits);
    }

    @Override
    public void close() throws IOException {
        try {
            connection.close();
        }
        catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
