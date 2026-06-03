// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.Message;
import io.synadia.flink.source.split.JetStreamSplitMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

/**
 * Unit coverage for {@link JetStreamSplitMessage} — a small holder that
 * pairs a split id with the message that arrived for it.
 */
class JetStreamSplitMessageTest {

    @Test
    void fieldsWired() {
        Message m = mock(Message.class);
        JetStreamSplitMessage sm = new JetStreamSplitMessage("split-id-7", m);

        assertEquals("split-id-7", sm.splitId);
        assertSame(m, sm.message);
    }
}
