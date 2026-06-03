// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.synadia.flink.message.AsciiStringSourceConverter;
import io.synadia.flink.message.SourceConverter;
import io.synadia.flink.message.Utf8StringSourceConverter;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.connector.source.Boundedness;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit coverage for {@link NatsSource#equals(Object)} /
 * {@link NatsSource#hashCode()}. Drives the early-return branches
 * (self / non-source / null) and each axis of the {@code &&} chain
 * (config, subjects, sourceConverter class, connectionFactory). The
 * generated {@code id} is deliberately excluded from equality, so two
 * structurally-identical sources with different ids must still compare
 * equal — that's the "true path" assertion.
 */
class NatsSourceTest {

    private static ConnectionFactory factory(String url) {
        Properties p = new Properties();
        p.setProperty("io.nats.client.url", url);
        return new ConnectionFactory(p);
    }

    private static <T> NatsSource<T> source(SourceConfig cfg,
                                            List<String> subjects,
                                            SourceConverter<T> converter,
                                            ConnectionFactory cf)
    {
        return new NatsSource<>(cfg, subjects, converter, cf);
    }

    @Test
    void equals_reflexiveAndNonSource() {
        NatsSource<String> s = source(
            new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 100),
            Collections.singletonList("orders.us"),
            new Utf8StringSourceConverter(),
            factory("nats://h:4222"));

        //noinspection EqualsWithItself,SimplifiableAssertion
        assertTrue(s.equals(s));
        //noinspection SimplifiableAssertion
        assertFalse(s.equals(new Object()));
        //noinspection ConstantConditions,SimplifiableAssertion
        assertFalse(s.equals(null));
    }

    @Test
    void equals_truePath_ignoresGeneratedId() {
        SourceConfig cfg = new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 100);
        List<String> subjects = Collections.singletonList("orders.us");
        ConnectionFactory cf = factory("nats://h:4222");

        NatsSource<String> a = source(cfg, subjects, new Utf8StringSourceConverter(), cf);
        NatsSource<String> b = source(cfg, subjects, new Utf8StringSourceConverter(), cf);

        assertNotEquals(a.id, b.id, "ids should differ — the test relies on this");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void notEquals_whenConfigDiffers() {
        List<String> subjects = Collections.singletonList("orders.us");
        ConnectionFactory cf = factory("nats://h:4222");

        NatsSource<String> a = source(
            new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 100),
            subjects, new Utf8StringSourceConverter(), cf);
        NatsSource<String> b = source(
            new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 200),
            subjects, new Utf8StringSourceConverter(), cf);

        assertNotEquals(a, b);
    }

    @Test
    void notEquals_whenSubjectsDiffer() {
        SourceConfig cfg = new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 100);
        ConnectionFactory cf = factory("nats://h:4222");

        NatsSource<String> a = source(cfg, Collections.singletonList("orders.us"),
            new Utf8StringSourceConverter(), cf);
        NatsSource<String> b = source(cfg, Arrays.asList("orders.us", "orders.eu"),
            new Utf8StringSourceConverter(), cf);

        assertNotEquals(a, b);
    }

    @Test
    void notEquals_whenConverterClassDiffers() {
        SourceConfig cfg = new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 100);
        List<String> subjects = Collections.singletonList("orders.us");
        ConnectionFactory cf = factory("nats://h:4222");

        NatsSource<String> a = source(cfg, subjects, new Utf8StringSourceConverter(), cf);
        NatsSource<String> b = source(cfg, subjects, new AsciiStringSourceConverter(), cf);

        assertNotEquals(a, b);
    }

    @Test
    void notEquals_whenConnectionFactoryDiffers() {
        SourceConfig cfg = new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 100);
        List<String> subjects = Collections.singletonList("orders.us");

        NatsSource<String> a = source(cfg, subjects, new Utf8StringSourceConverter(),
            factory("nats://h1:4222"));
        NatsSource<String> b = source(cfg, subjects, new Utf8StringSourceConverter(),
            factory("nats://h2:4222"));

        assertNotEquals(a, b);
    }
}
