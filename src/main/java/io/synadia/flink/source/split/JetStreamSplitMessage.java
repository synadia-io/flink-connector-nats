// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source.split;

import io.nats.client.Message;
import org.apache.flink.annotation.Internal;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class JetStreamSplitMessage {
    /**
     * The split id
     */
    public final String splitId;

    /**
     * The split message
     */
    public final Message message;

    /**
     * Construct a JetStreamSplitMessage
     * @param splitId the split id
     * @param message the split message
     */
    public JetStreamSplitMessage(String splitId, Message message) {
        this.splitId = splitId;
        this.message = message;
    }
}
