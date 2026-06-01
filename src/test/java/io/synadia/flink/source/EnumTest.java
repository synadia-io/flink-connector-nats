// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.api.AckPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage for the source-level enums: {@link AckBehavior}. Exercises the
 * public surface — constants, string keys, {@code toString()}, and the
 * {@code get(String)} lookups.
 */
class EnumTest {

    // ===== AckBehavior =====

    @Test
    void ackBehavior_values() {
        assertArrayEquals(
            new AckBehavior[]{
                AckBehavior.NoAck,
                AckBehavior.NoAckUnordered,
                AckBehavior.AckAll,
                AckBehavior.AllButDoNotAck,
                AckBehavior.ExplicitButDoNotAck},
            AckBehavior.values());
    }

    @Test
    void ackBehavior_behaviorAndToString() {
        for (AckBehavior ab : AckBehavior.values()) {
            assertEquals(ab.name(), ab.behavior);
            assertEquals(ab.name(), ab.toString());
        }
    }

    @Test
    void ackBehavior_ackPolicyAndIsNoAck() {
        assertEquals(AckPolicy.None,     AckBehavior.NoAck.ackPolicy);
        assertTrue(AckBehavior.NoAck.isNoAck);

        assertEquals(AckPolicy.None,     AckBehavior.NoAckUnordered.ackPolicy);
        assertTrue(AckBehavior.NoAckUnordered.isNoAck);

        assertEquals(AckPolicy.All,      AckBehavior.AckAll.ackPolicy);
        assertFalse(AckBehavior.AckAll.isNoAck);

        assertEquals(AckPolicy.All,      AckBehavior.AllButDoNotAck.ackPolicy);
        assertFalse(AckBehavior.AllButDoNotAck.isNoAck);

        assertEquals(AckPolicy.Explicit, AckBehavior.ExplicitButDoNotAck.ackPolicy);
        assertFalse(AckBehavior.ExplicitButDoNotAck.isNoAck);
    }

    @Test
    void ackBehavior_get() {
        assertEquals(AckBehavior.NoAck,                AckBehavior.get("NoAck"));
        assertEquals(AckBehavior.NoAck,                AckBehavior.get("noack"));
        assertEquals(AckBehavior.NoAckUnordered,       AckBehavior.get("NoAckUnordered"));
        assertEquals(AckBehavior.AckAll,               AckBehavior.get("AckAll"));
        assertEquals(AckBehavior.AllButDoNotAck,       AckBehavior.get("AllButDoNotAck"));
        assertEquals(AckBehavior.ExplicitButDoNotAck,  AckBehavior.get("ExplicitButDoNotAck"));
    }

    @Test
    void ackBehavior_get_invalidInputReturnsNull() {
        assertNull(AckBehavior.get(null));
        assertNull(AckBehavior.get(""));
        assertNull(AckBehavior.get("nope"));
    }
}
