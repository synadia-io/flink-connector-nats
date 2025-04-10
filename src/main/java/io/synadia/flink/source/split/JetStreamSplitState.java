// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.split;

public class JetStreamSplitState {

    private final JetStreamSplit split;

    public JetStreamSplitState(JetStreamSplit split) {
        this.split = split;
    }

    public JetStreamSplit toManagedSplit() {
        return split;
    }

    public JetStreamSplit getSplit() {
        return split;
    }
}
