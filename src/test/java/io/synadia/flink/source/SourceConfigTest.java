// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple coverage for {@link SourceConfig} — fields, the derived
 * {@code bounded} flag, equality, hashing, and toString. The JetStream
 * queue-capacity formula lives in {@link JetStreamSourceBuilder#build()} and
 * is covered in {@link JetStreamSourceBuilderTest}.
 */
class SourceConfigTest {

    @Test
    void fields_set() {
        SourceConfig c = new SourceConfig(Boundedness.BOUNDED, 32);

        assertEquals(Boundedness.BOUNDED, c.boundedness);
        assertTrue(c.bounded);
        assertEquals(32, c.sourceQueueCapacity);
    }

    @Test
    void bounded_derivedFromBoundedness() {
        assertTrue(new SourceConfig(Boundedness.BOUNDED, 0).bounded);
        assertFalse(new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 0).bounded);
    }

    @Test
    void equalsAndHashCode() {
        SourceConfig a = new SourceConfig(Boundedness.BOUNDED, 32);
        SourceConfig b = new SourceConfig(Boundedness.BOUNDED, 32);

        assertEquals(a, a);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        assertNotEquals(a, new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 32));
        assertNotEquals(a, new SourceConfig(Boundedness.BOUNDED, 64));

        assertNotEquals(a, null);
        assertNotEquals(a, "not a config");
    }

    @Test
    void toString_includesFields() {
        SourceConfig c = new SourceConfig(Boundedness.BOUNDED, 32);
        String s = c.toString();

        assertTrue(s.contains("BOUNDED"), s);
        assertTrue(s.contains("32"), s);
    }
}
