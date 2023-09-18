// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.source.enumerator;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.support.Debug;
import io.synadia.common.ConnectionFactory;
import io.synadia.source.split.NatsSubjectSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class NatsSourceEnumerator<SplitT> implements SplitEnumerator<NatsSubjectSplit, NatsSourceEnumState> {
    private final ConnectionFactory connectionFactory;
    private final SplitEnumeratorContext<NatsSubjectSplit> context;

    private final NatsSourceEnumState state;

    // Lazily instantiated or mutable fields.
    private Connection connection;
    private Dispatcher dispatcher;

    public NatsSourceEnumerator(Set<String> unassigned,
                                ConnectionFactory connectionFactory,
                                SplitEnumeratorContext<NatsSubjectSplit> context)
    {
        this.connectionFactory = connectionFactory;
        this.context = context;
        this.state = new NatsSourceEnumState(unassigned);
    }

    public NatsSourceEnumerator(ConnectionFactory connectionFactory,
                                SplitEnumeratorContext<NatsSubjectSplit> context,
                                NatsSourceEnumState natsSourceEnumState)
    {
        this.connectionFactory = connectionFactory;
        this.context = context;
        this.state = natsSourceEnumState;
    }

    @Override
    public void start() {
        Debug.dbg("NatsSourceEnumerator start");
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        Debug.dbg("NatsSourceEnumerator.start | subtaskId:" + subtaskId + " | requesterHostname:" + requesterHostname);
        NatsSubjectSplit split = state.assign();
        if (split == null) {
            context.signalNoMoreSplits(subtaskId);
        }
        else {
            context.assignSplit(state.assign(), subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<NatsSubjectSplit> splits, int subtaskId) {
        Debug.dbg("NatsSourceEnumerator.addSplitsBack | splits:" + (splits == null ? -1 : splits.size()) + " | subtaskId:" + subtaskId);
        state.addSplitsBack(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        Debug.dbg("NatsSourceEnumerator.start | subtaskId:" + subtaskId);
    }

    @Override
    public NatsSourceEnumState snapshotState(long checkpointId) throws Exception {
        Debug.dbg("NatsSourceEnumerator.snapshotState | checkpointId:" + checkpointId);
        return state.copy();
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
