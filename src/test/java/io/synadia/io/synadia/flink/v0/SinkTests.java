// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.io.synadia.flink.v0;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.synadia.flink.v0.sink.NatsSink;
import io.synadia.io.synadia.flink.TestBase;
import io.synadia.io.synadia.flink.WordSubscriber;
import nats.io.NatsServerRunner;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class SinkTests extends TestBase {

    @Test
    public void testSink() throws Exception {
        runInServer((nc, url) -> {
            _testSink(nc, random(), defaultConnectionProperties(url), null);
        });
    }

    @Test
    public void testTlsPassProperties() throws Exception {
        try (NatsServerRunner ts = new NatsServerRunner("src/test/resources/tls.conf", false)) {
            String subject = random();
            String url = ts.getURI();
            Properties connectionProperties = addTestSslProperties(defaultConnectionProperties(url));
            connectionProperties.put(Options.PROP_URL, url);
            Options options = Options.builder().properties(connectionProperties).build();
            try (Connection nc = Nats.connect(options)) {
                _testSink(nc, subject, connectionProperties, null);
            }
        }
    }

    @Test
    public void testTlsPassPropertiesLocation() throws Exception {
        try (NatsServerRunner ts = new NatsServerRunner("src/test/resources/tls.conf", false)) {
            String subject = random();
            String url = ts.getURI();

            Properties props = addTestSslProperties(defaultConnectionProperties(url));
            String connectionPropertiesFile = createTempPropertiesFile(props);

            Options options = Options.builder().properties(props).build();
            try (Connection nc = Nats.connect(options)) {
                _testSink(nc, subject, null, connectionPropertiesFile);
            }
        }
    }

    private static void _testSink(Connection nc, String subject,
                                  Properties connectionProperties,
                                  String connectionPropertiesFile) throws Exception
    {
        WordSubscriber sub = new WordSubscriber(nc, subject);

        NatsSink<String> sink = newNatsSink(subject, connectionProperties, connectionPropertiesFile);

        StreamExecutionEnvironment env = getStreamExecutionEnvironment();
        DataStream<String> dataStream = getPayloadDataStream(env);
        dataStream.sinkTo(sink);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
        env.execute("TestSink");

        sub.assertAllMessagesReceived();
    }
}
