// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.io.synadia.flink;

import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.synadia.flink.sink.js.NATSJetstreamSinkBuilder;
import io.synadia.flink.sink.js.NATSStreamConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class NatsJetstreamSinkTest extends TestBase {

    @Test
    public void testSink() throws Exception {
        String sinkSubject = "test-sink";
        String streamName = "test-stream";

        runInExternalServer(true, (nc, url) -> {

            Properties connectionProperties = defaultConnectionProperties(url);
            StreamConfiguration stream = new StreamConfiguration.Builder()
                    .name(streamName)
                    .subjects(sinkSubject)
                    .build();
            nc.jetStreamManagement().addStream(stream);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<String> testStream = env.fromElements("Message 1", "Message 2", "Message 3");
            NATSJetstreamSinkBuilder<String> builder = new NATSJetstreamSinkBuilder<String>()
                    .payloadSerializer(new SimpleStringSchema())
                    .streamConfig(new NATSStreamConfig.Builder()
                            .withStreamName(streamName)
                            .withSubjects(Collections.singletonList(sinkSubject))
                            .build())
                    .subjects(sinkSubject);
            builder.connectionProperties(connectionProperties);

            testStream.sinkTo(builder.build());
            env.executeAsync("Test NATS Jetstream Sink");
            Thread.sleep(5000);
            StreamInfo info = nc.jetStreamManagement().getStreamInfo(streamName);
            assertTrue(info.getStreamState().getMsgCount() >= 3);
        });
    }
}
