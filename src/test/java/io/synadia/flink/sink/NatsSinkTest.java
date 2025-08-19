// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.TestBase;
import io.synadia.flink.TestServerContext;
import io.synadia.flink.helpers.WordSubscriber;
import io.synadia.flink.message.Utf8StringSinkConverter;
import io.synadia.flink.utils.MiscUtils;
import nats.io.NatsServerRunner;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.synadia.flink.utils.Constants.SINK_CONVERTER_CLASS_NAME;
import static io.synadia.flink.utils.Constants.SUBJECTS;
import static io.synadia.flink.utils.MiscUtils.generatePrefixedId;
import static io.synadia.flink.utils.MiscUtils.random;
import static org.junit.jupiter.api.Assertions.*;

public class NatsSinkTest extends TestBase {
    static TestServerContext ctx;

    @BeforeAll
    public static void beforeAll() throws Exception {
        ctx = createContext(ctx);
    }

    @AfterAll
    public static void afterAll() {
        ctx = shutdownContext(ctx);
    }

    @AfterEach
    public void afterEach() {
        cleanupJs(ctx.nc);
    }

    @Test
    public void testBasic() throws Exception {
        _testSink("testBasic", ctx.nc, defaultConnectionProperties(ctx.url), null);
    }

    @Test
    public void testTlsPassProperties() throws Exception {
        try (NatsServerRunner ts = new NatsServerRunner("src/test/resources/tls.conf", false, true)) {
            String url = ts.getURI();
            Properties connectionProperties = addTestSslProperties(defaultConnectionProperties(url));
            connectionProperties.put(Options.PROP_URL, url);
            Options options = Options.builder().properties(connectionProperties).build();
            try (Connection nc = Nats.connect(options)) {
                _testSink("testTlsPassProperties", nc, connectionProperties, null);
            }
        }
    }

    @Test
    public void testTlsPassPropertiesLocation() throws Exception {
        try (NatsServerRunner ts = new NatsServerRunner("src/test/resources/tls.conf", false, true)) {
            String url = ts.getURI();

            Properties props = addTestSslProperties(defaultConnectionProperties(url));
            String connectionPropertiesFile = createTempPropertiesFile(props);

            Options options = Options.builder().properties(props).build();
            try (Connection nc = Nats.connect(options)) {
                _testSink("testTlsPassPropertiesLocation", nc, null, connectionPropertiesFile);
            }
        }
    }

    private static void _testSink(String jobName, Connection nc,
                                  Properties connectionProperties,
                                  String connectionPropertiesFile) throws Exception
    {
        String subject = random();
        WordSubscriber sub = new WordSubscriber(nc, subject);
        Sink<String> sink = newNatsStringSink(subject, connectionProperties, connectionPropertiesFile);
        __testSink(jobName + "-TestCoreSink", sink, sub, subject);

        subject = random();
        nc.jetStreamManagement().addStream(StreamConfiguration.builder()
            .name(subject).storageType(StorageType.Memory).build());
        sub = new WordSubscriber(nc, subject, true);
        JetStreamSink<String> jsSink = newNatsJetStreamSink(subject, connectionProperties, connectionPropertiesFile);
        __testSink(jobName + "-TestJsSink", jsSink, sub, subject);
    }

    private static void __testSink(String jobName, Sink<String> sink, WordSubscriber sub, String subject) throws Exception {
        String s = sink.toString(); // toString COVERAGE
        assertTrue(s.contains(sink.getClass().getSimpleName()));
        assertTrue(s.contains(subject));

        NatsSink<String> baseSink = (NatsSink<String>)sink;
        s = baseSink.toJson();
        assertTrue(s.contains(SINK_CONVERTER_CLASS_NAME));
        assertTrue(s.contains("Utf8StringSinkConverter"));
        assertTrue(s.contains(SUBJECTS));
        assertTrue(s.contains(subject));

        s = baseSink.toYaml();
        assertTrue(s.contains(SINK_CONVERTER_CLASS_NAME));
        assertTrue(s.contains("Utf8StringSinkConverter"));
        assertTrue(s.contains(SUBJECTS));
        assertTrue(s.contains(subject));

        assertTrue(baseSink.getSubjects().contains(subject));

        //noinspection deprecation
        assertNotNull(sink.createWriter((Sink.InitContext)null)); // COVERAGE

        StreamExecutionEnvironment env = getStreamExecutionEnvironment();
        DataStream<String> dataStream = getPayloadDataStream(env);
        dataStream.sinkTo(sink);
        env.execute(generatePrefixedId(jobName));
        sub.assertAllMessagesReceived();
    }

    @Test
    void testSinkBuilderCoverage() throws IOException {
        assertThrows(IllegalArgumentException.class,
            () -> new NatsSinkBuilder<String>()
                .subjects("foo")
                .connectionProperties(defaultConnectionProperties(ctx.url))
                .build());
        assertThrows(IllegalArgumentException.class,
            () -> new NatsSinkBuilder<String>()
                .subjects("foo")
                .connectionProperties(defaultConnectionProperties(ctx.url))
                .sinkConverterClass("not-a-class")
                .build());

        // COVERAGE for no connectionProperties and subject(s) setters
        List<String> hasNullAndEmpty = new ArrayList<>();
        hasNullAndEmpty.add("");
        hasNullAndEmpty.add(null);

        final Utf8StringSinkConverter converter = new Utf8StringSinkConverter();

        NatsSink<String> sink = new NatsSinkBuilder<String>()
            .subjects((String) null)
            .subjects(new String[0])
            .subjects((List<String>) null)
            .subjects(hasNullAndEmpty)
            .subjects("subject1", "subject2")
            .sinkConverter(converter)
            .build();

        assertEquals(converter.getClass(), sink.sinkConverter.getClass());
        assertNotNull(sink.subjects);
        assertEquals(2, sink.subjects.size());
        assertTrue(sink.subjects.contains("subject1"));
        assertTrue(sink.subjects.contains("subject2"));

        sink = new NatsSinkBuilder<String>()
            .jsonConfigFile("src/test/resources/core-sink-config.json")
            .build();
        assertEquals(converter.getClass(), sink.sinkConverter.getClass());
        assertNotNull(sink.subjects);
        assertEquals(2, sink.subjects.size());
        assertTrue(sink.subjects.contains("subject1"));
        assertTrue(sink.subjects.contains("subject2"));

        sink = new NatsSinkBuilder<String>()
            .yamlConfigFile("src/test/resources/core-sink-config.yaml")
            .build();
        assertEquals(converter.getClass(), sink.sinkConverter.getClass());
        assertNotNull(sink.subjects);
        assertEquals(2, sink.subjects.size());
        assertTrue(sink.subjects.contains("subject1"));
        assertTrue(sink.subjects.contains("subject2"));






        sink = new NatsSinkBuilder<String>()
            .jsonConfigFile("src/test/resources/core-sink-config-subject.json")
            .build();
        assertEquals(converter.getClass(), sink.sinkConverter.getClass());
        assertNotNull(sink.subjects);
        assertEquals(1, sink.subjects.size());
        assertTrue(sink.subjects.contains("subject1"));

        sink = new NatsSinkBuilder<String>()
            .yamlConfigFile("src/test/resources/core-sink-config-subject.yaml")
            .build();
        assertEquals(converter.getClass(), sink.sinkConverter.getClass());
        assertNotNull(sink.subjects);
        assertEquals(1, sink.subjects.size());
        assertTrue(sink.subjects.contains("subject1"));
    }

    @Test
    void testJsSinkBuilderCoverage() throws IOException {
        final Utf8StringSinkConverter converter = new Utf8StringSinkConverter();

        JetStreamSinkBuilder<String> builder = new JetStreamSinkBuilder<String>()
            .subjects("subject1", "subject2")
            .sinkConverter(converter);
        JetStreamSink<String> sink = builder.build();
        assertEquals(converter.getClass(), sink.sinkConverter.getClass());
        assertNotNull(sink.subjects);
        assertEquals(2, sink.subjects.size());
        assertTrue(sink.subjects.contains("subject1"));
        assertTrue(sink.subjects.contains("subject2"));

        List<String> subjects = new ArrayList<>();
        subjects.add("subject1");
        subjects.add("subject2");
        builder = new JetStreamSinkBuilder<String>()
            .subjects(subjects)
            .sinkConverterClass(MiscUtils.getClassName(converter));
        sink = builder.build();
        assertEquals(converter.getClass(), sink.sinkConverter.getClass());
        assertNotNull(sink.subjects);
        assertEquals(2, sink.subjects.size());
        assertTrue(sink.subjects.contains("subject1"));
        assertTrue(sink.subjects.contains("subject2"));

        builder = new JetStreamSinkBuilder<String>()
            .jsonConfigFile("src/test/resources/js-sink-config.json");
        sink = builder.build();
        assertEquals(converter.getClass(), sink.sinkConverter.getClass());
        assertNotNull(sink.subjects);
        assertEquals(1, sink.subjects.size());
        assertTrue(sink.subjects.contains("uk"));

        builder = new JetStreamSinkBuilder<String>()
            .yamlConfigFile("src/test/resources/js-sink-config.yaml");
        sink = builder.build();
        assertEquals(converter.getClass(), sink.sinkConverter.getClass());
        assertNotNull(sink.subjects);
        assertEquals(1, sink.subjects.size());
        assertTrue(sink.subjects.contains("uk"));
    }
}
