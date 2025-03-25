// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.split;

public class NatsSubjectSplitState {

    private final NatsSubjectSplit split;

    public NatsSubjectSplitState(NatsSubjectSplit split) {
        this.split = split;
    }

    public NatsSubjectSplit toNatsSubjectSplit() {
        return split;
    }

    public NatsSubjectSplit getSplit() {
        return split;
    }
}
