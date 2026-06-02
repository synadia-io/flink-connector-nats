// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.synadia.flink.message.AsciiStringSourceConverter;
import io.synadia.flink.message.SourceConverter;
import io.synadia.flink.message.Utf8StringSourceConverter;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.connector.source.Boundedness;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Targeted coverage for {@link JetStreamSource#equals(Object)} /
 * {@link JetStreamSource#hashCode()}. {@code JetStreamSourceBuilderTest}
 * already exercises the equal-and-self branches; this class drives each
 * not-equal axis (config, configById, sourceConverter, connectionFactory)
 * so the {@code &&} chain is fully covered.
 */
class JetStreamSourceTest {

    private static JetStreamSubjectConfiguration sc(String subject) {
        return JetStreamSubjectConfiguration.builder()
            .streamName("S").subject(subject).build();
    }

    private static Map<String, JetStreamSubjectConfiguration> mapOf(JetStreamSubjectConfiguration... cs) {
        Map<String, JetStreamSubjectConfiguration> m = new HashMap<>();
        for (JetStreamSubjectConfiguration c : cs) {
            m.put(c.id, c);
        }
        return m;
    }

    private static ConnectionFactory factory(String url) {
        Properties p = new Properties();
        p.setProperty("io.nats.client.url", url);
        return new ConnectionFactory(p);
    }

    private static <T> JetStreamSource<T> source(SourceConfig cfg,
                                                 Map<String, JetStreamSubjectConfiguration> configById,
                                                 SourceConverter<T> converter,
                                                 ConnectionFactory factory)
    {
        return new JetStreamSource<>(cfg, configById, converter, factory);
    }

    @Test
    void equals_reflexiveAndNonSource() {
        JetStreamSource<String> s = source(
            new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 64),
            mapOf(sc("a")),
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
    void equals_truePathAcrossAllAxes() {
        SourceConfig cfg = new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 64);
        Map<String, JetStreamSubjectConfiguration> configById = mapOf(sc("a"));
        ConnectionFactory cf = factory("nats://h:4222");

        JetStreamSource<String> a = source(cfg, configById, new Utf8StringSourceConverter(), cf);
        JetStreamSource<String> b = source(cfg, configById, new Utf8StringSourceConverter(), cf);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void notEquals_whenConfigDiffers() {
        Map<String, JetStreamSubjectConfiguration> configById = mapOf(sc("a"));
        ConnectionFactory cf = factory("nats://h:4222");

        JetStreamSource<String> a = source(
            new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 64),
            configById, new Utf8StringSourceConverter(), cf);
        JetStreamSource<String> b = source(
            new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 128),
            configById, new Utf8StringSourceConverter(), cf);

        assertNotEquals(a, b);
    }

    @Test
    void notEquals_whenConfigByIdDiffers() {
        SourceConfig cfg = new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 64);
        ConnectionFactory cf = factory("nats://h:4222");

        JetStreamSource<String> a = source(cfg, mapOf(sc("a")),
            new Utf8StringSourceConverter(), cf);
        JetStreamSource<String> b = source(cfg, mapOf(sc("b")),
            new Utf8StringSourceConverter(), cf);

        assertNotEquals(a, b);
    }

    @Test
    void notEquals_whenConverterClassDiffers() {
        SourceConfig cfg = new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 64);
        Map<String, JetStreamSubjectConfiguration> configById = mapOf(sc("a"));
        ConnectionFactory cf = factory("nats://h:4222");

        JetStreamSource<String> a = source(cfg, configById, new Utf8StringSourceConverter(), cf);
        JetStreamSource<String> b = source(cfg, configById, new AsciiStringSourceConverter(), cf);

        assertNotEquals(a, b);
    }

    @Test
    void notEquals_whenConnectionFactoryDiffers() {
        SourceConfig cfg = new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, 64);
        Map<String, JetStreamSubjectConfiguration> configById = mapOf(sc("a"));

        JetStreamSource<String> a = source(cfg, configById, new Utf8StringSourceConverter(),
            factory("nats://h1:4222"));
        JetStreamSource<String> b = source(cfg, configById, new Utf8StringSourceConverter(),
            factory("nats://h2:4222"));

        assertNotEquals(a, b);
    }
}
