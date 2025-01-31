// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.split;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedSplitState {

    private static final Logger LOG = LoggerFactory.getLogger(ManagedSplitState.class);

    private final ManagedSplit split;

    public ManagedSplitState(ManagedSplit split) {
        this.split = split;
    }

    public ManagedSplit toManagedSplit() {
        LOG.debug("toManagedSplit");
        return split;
    }

    public ManagedSplit getSplit() {
        return split;
    }
}
