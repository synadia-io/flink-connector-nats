// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.TestBase;
import io.synadia.flink.helpers.WordSubscriber;
import nats.io.NatsServerRunner;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static io.synadia.flink.utils.MiscUtils.generatePrefixedId;
import static io.synadia.flink.utils.MiscUtils.random;

public class NatsSinkTest extends TestBase {

    @Test
    public void testBasic() throws Exception {
        runInJsServer((nc, url) -> {
            _testSink("testBasic", nc, defaultConnectionProperties(url), null);
        });
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
        __testSink(jobName + "-TestCoreSink", sink, sub);

        subject = random();
        nc.jetStreamManagement().addStream(StreamConfiguration.builder()
            .name(subject).storageType(StorageType.Memory).build());
        sub = new WordSubscriber(nc, subject, true);
        sink = newNatsJetStreamSink(subject, connectionProperties, connectionPropertiesFile);
        __testSink(jobName + "-TestJsSink", sink, sub);
    }

    private static void __testSink(String jobName, Sink<String> sink, WordSubscriber sub) throws Exception {
        StreamExecutionEnvironment env = getStreamExecutionEnvironment();
        DataStream<String> dataStream = getPayloadDataStream(env);
        dataStream.sinkTo(sink);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
        env.execute(generatePrefixedId(jobName));
        sub.assertAllMessagesReceived();
    }
}
