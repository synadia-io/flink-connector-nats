// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple coverage for {@link JetStreamSourceConfig} — fields, the derived
 * {@code bounded} flag, equality, hashing, and toString.
 */
class JetStreamSourceConfigTest {

    @Test
    void fields_set() {
        JetStreamSourceConfig c = new JetStreamSourceConfig(
            Boundedness.BOUNDED, 32, ConsumerStrategy.Polled);

        assertEquals(Boundedness.BOUNDED, c.boundedness);
        assertTrue(c.bounded);
        assertEquals(32, c.sourceQueueCapacity);
        assertEquals(ConsumerStrategy.Polled, c.consumerStrategy);
    }

    @Test
    void bounded_derivedFromBoundedness() {
        assertTrue(new JetStreamSourceConfig(
            Boundedness.BOUNDED, -1, ConsumerStrategy.Polled).bounded);
        assertFalse(new JetStreamSourceConfig(
            Boundedness.CONTINUOUS_UNBOUNDED, -1, ConsumerStrategy.Polled).bounded);
    }

    @Test
    void equalsAndHashCode() {
        JetStreamSourceConfig a = new JetStreamSourceConfig(
            Boundedness.BOUNDED, 32, ConsumerStrategy.Polled);
        JetStreamSourceConfig b = new JetStreamSourceConfig(
            Boundedness.BOUNDED, 32, ConsumerStrategy.Polled);

        assertEquals(a, a);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        assertNotEquals(a, new JetStreamSourceConfig(
            Boundedness.CONTINUOUS_UNBOUNDED, 32, ConsumerStrategy.Polled));
        assertNotEquals(a, new JetStreamSourceConfig(
            Boundedness.BOUNDED, 64, ConsumerStrategy.Polled));
        assertNotEquals(a, new JetStreamSourceConfig(
            Boundedness.BOUNDED, 32, ConsumerStrategy.Dispatched));

        assertNotEquals(a, null);
        assertNotEquals(a, "not a config");
    }

    @Test
    void toString_includesFields() {
        JetStreamSourceConfig c = new JetStreamSourceConfig(
            Boundedness.BOUNDED, 32, ConsumerStrategy.Polled);
        String s = c.toString();

        assertTrue(s.contains("BOUNDED"), s);
        assertTrue(s.contains("32"), s);
        assertTrue(s.contains("Polled"), s);
    }
}
