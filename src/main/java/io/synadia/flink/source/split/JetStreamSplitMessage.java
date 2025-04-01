// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.split;

import io.nats.client.Message;

public class JetStreamSplitMessage {
    public final String splitId;
    public final Message message;

    public JetStreamSplitMessage(String splitId, Message message) {
        this.splitId = splitId;
        this.message = message;
    }
}
