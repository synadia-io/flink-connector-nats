// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.synadia.payload.StringPayloadSerializer;
import io.synadia.sink.NatsSinkBuilder;
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
            Properties connectionProperties = new Properties() ;
            connectionProperties.put(Options.PROP_URL, url);
            _testSink(nc, random(), connectionProperties, null);
        });
    }

    @Test
    public void testTlsPassProperties() throws Exception {
        try (NatsServerRunner ts = new NatsServerRunner("src/test/resources/tls.conf", false)) {
            String subject = random();
            String url = ts.getURI();
            Properties connectionProperties = AddTestSslProperties(null);
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

            Properties props = AddTestSslProperties(null);
            props.put(Options.PROP_URL, url);
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

        final StringPayloadSerializer serializer = new StringPayloadSerializer();
        NatsSinkBuilder<String> builder = new NatsSinkBuilder<String>()
            .subjects(subject)
            .payloadSerializer(serializer);

        if (connectionProperties == null) {
            builder.connectionPropertiesFile(connectionPropertiesFile);
        }
        else {
            builder.connectionProperties(connectionProperties);
        }

        StreamExecutionEnvironment env = getStreamExecutionEnvironment();
        DataStream<String> dataStream = getPayloadDataStream(env);
        dataStream.sinkTo(builder.build());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
        env.execute("TestSink");

        sub.assertAllMessagesReceived();
    }
}
