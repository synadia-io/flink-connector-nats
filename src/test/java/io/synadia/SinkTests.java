// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.synadia.payload.StringPayloadSerializer;
import nats.io.NatsServerRunner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static io.synadia.Constants.SINK_PROPERTIES_FILE;

public class SinkTests extends TestBase {

    @Test
    public void testSink() throws Exception {
        String subject = random();
        runInServer((nc, url) -> {
            _testSink(nc, url, subject, null);
        });
    }

    private static void _testSink(Connection nc, String url, String subject, Properties props) throws Exception {
        WordSubscriber sub = new WordSubscriber(nc, subject);

        props = props == null ? new Properties() : props;
        props.put(Options.PROP_URL, url);

        final StringPayloadSerializer serializer = new StringPayloadSerializer();
        final NatsSink<String> natsSink = new NatsSinkBuilder<String>()
            .subjects(subject).sinkProperties(props).payloadSerializer(serializer).build();

        StreamExecutionEnvironment env = getStreamExecutionEnvironment();
        DataStream<String> text = getStringDataStream(env);
        text.sinkTo(natsSink);
        env.execute("TestSink");

        sub.assertAllMessagesReceived();
    }

    @Test
    public void testTlsPassProperties() throws Exception {
        try (NatsServerRunner ts = new NatsServerRunner("src/test/resources/tls.conf", false)) {
            String subject = random();
            String url = ts.getURI();
            Properties props = AddTestSslProperties(null);
            props.put(Options.PROP_URL, url);
            Options options = Options.builder().properties(props).build();
            try (Connection nc = Nats.connect(options)) {
                _testSink(nc, url, subject, props);
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
            String propertiesFileLocation = createTempPropertiesFile(props);
            Properties sinkProps = new Properties();
            sinkProps.put(SINK_PROPERTIES_FILE, propertiesFileLocation);

            Options options = Options.builder().properties(props).build();
            try (Connection nc = Nats.connect(options)) {
                _testSink(nc, url, subject, sinkProps);
            }
        }
    }

}
