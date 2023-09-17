// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.source;

import io.nats.client.support.Debug;
import org.apache.flink.api.connector.source.SplitEnumerator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class NatsSplitEnumerator<SplitT> implements SplitEnumerator<NatsSubjectSourceSplit, NoCheckpoint<NatsSubjectSourceSplit>> {
    @Override
    public void start() {
        Debug.dbg("NatsSplitEnumerator start");
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        Debug.dbg("NatsSplitEnumerator.start | subtaskId:" + subtaskId + " | requesterHostname:" + requesterHostname);
    }

    @Override
    public void addSplitsBack(List<NatsSubjectSourceSplit> splits, int subtaskId) {
        Debug.dbg("NatsSplitEnumerator.addSplitsBack | splits:" + (splits == null ? -1 : splits.size()) + " | subtaskId:" + subtaskId);
    }

    @Override
    public void addReader(int subtaskId) {
        Debug.dbg("NatsSplitEnumerator.start | subtaskId:" + subtaskId);
    }

    @Override
    public NoCheckpoint<NatsSubjectSourceSplit> snapshotState(long checkpointId) throws Exception {
        Debug.dbg("NatsSplitEnumerator.snapshotState | checkpointId:" + checkpointId);
        return new NoCheckpoint<>();
    }

    @Override
    public void close() throws IOException {
        Debug.dbg("NatsSplitEnumerator.close");
    }
}
