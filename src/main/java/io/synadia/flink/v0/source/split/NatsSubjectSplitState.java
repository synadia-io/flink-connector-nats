// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source.split;

import io.nats.client.Message;

import java.util.ArrayList;
import java.util.List;

public class NatsSubjectSplitState {

    private final NatsSubjectSplit split;

    public NatsSubjectSplitState(NatsSubjectSplit split) {
        this.split = split;
    }

    // this method gets called on every snapshot done by flink
    // flush the list to remove the last set of messages
    // either they have already passed/failed while ack-ing
    // no need to maintain it anymore
    public NatsSubjectSplit toNatsSubjectSplit() {
        List<Message> messages = new ArrayList<>(split.getCurrentMessages());
        split.getCurrentMessages().clear();
        return new NatsSubjectSplit(split.getSubject(), messages);
    }

    public NatsSubjectSplit getSplit() {
        return split;
    }
}
