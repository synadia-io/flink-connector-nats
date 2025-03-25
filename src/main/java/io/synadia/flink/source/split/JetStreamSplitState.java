// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.split;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JetStreamSplitState {

    private static final Logger LOG = LoggerFactory.getLogger(JetStreamSplitState.class);

    private final JetStreamSplit split;

    public JetStreamSplitState(JetStreamSplit split) {
        this.split = split;
    }

    public JetStreamSplit toManagedSplit() {
        LOG.debug("toManagedSplit");
        return split;
    }

    public JetStreamSplit getSplit() {
        return split;
    }
}
