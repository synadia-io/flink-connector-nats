// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.split;

public class NatSubjectStateSplitState {
    private final NatsSubjectSplit natsSubjectSplit;

    public NatSubjectStateSplitState(NatsSubjectSplit natsSubjectSplit) {
        this.natsSubjectSplit = natsSubjectSplit;
    }

    public String getSplitId() {
        return natsSubjectSplit.splitId();
    }
}
