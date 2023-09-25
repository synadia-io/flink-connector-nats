// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.split;

public class NatSubjectStateSplitState {
    private final NatsSubjectSplit natsSubjectSplit;

    public NatSubjectStateSplitState(NatsSubjectSplit natsSubjectSplit) {
        this.natsSubjectSplit = natsSubjectSplit;
    }

    public String getSplitId() {
        return natsSubjectSplit.splitId();
    }
}
