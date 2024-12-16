// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.io.synadia.flink.v0;

import io.nats.client.*;
import io.nats.client.api.*;
import io.synadia.flink.v0.NatsJetStreamSource;
import io.synadia.flink.v0.NatsJetStreamSourceBuilder;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.payload.StringPayloadDeserializer;
import io.synadia.io.synadia.flink.TestBase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.nats.client.api.ConsumerConfiguration.INTEGER_UNSET;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsSourceTests extends TestBase {

    static void publish(JetStream js, String subject, int count) throws Exception {
        publish(js, subject, count, 0);
    }

    static void publish(JetStream js, String subject, int count, long delay) throws Exception {
        for (int x = 0; x < count; x++) {
            js.publish(subject, ("data-" + subject + "-" + x + "-" + random()).getBytes());
            if (delay > 0) {
                sleep(delay);
            }
        }
    }

    @Test
    public void testJsSourceBounded() throws Exception {
        final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());
        String sourceSubject = random("sub");
        String streamName = random("strm");
        String consumerName = random("con");

        runInServer(true, (nc, url) -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = jsm.jetStream();

            createStream(jsm, streamName, sourceSubject);
            publish(js, sourceSubject, 10);

            ConsumerConfiguration cc = createConsumer(jsm, streamName, sourceSubject, consumerName, INTEGER_UNSET);

            // --------------------------------------------------------------------------------
            Properties connectionProperties = defaultConnectionProperties(url);
            PayloadDeserializer<String> deserializer = new StringPayloadDeserializer();
            NatsJetStreamSourceBuilder<String> builder =
                new NatsJetStreamSourceBuilder<String>()
                    .subjects(sourceSubject)
                    .payloadDeserializer(deserializer)
                    .connectionProperties(connectionProperties)
                    .consumerName(consumerName)
                    .maxFetchRecords(10)
                    .maxFetchTime(Duration.ofSeconds(5))
                    .boundness(Boundedness.BOUNDED);

            NatsJetStreamSource<String> natsSource = builder.build();
            StreamExecutionEnvironment env = getStreamExecutionEnvironment();
            env.getCheckpointConfig().setCheckpointInterval(11_000L);
            DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-input");

            ds.map(String::toUpperCase); //To Avoid Sink Dependency
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
            env.executeAsync("TestJsSourceBounded");

            Thread.sleep(12_000);
            env.close();
            ConsumerInfo ci = jsm.getConsumerInfo(streamName, consumerName);
            SequenceInfo sequenceInfo = ci.getDelivered();
            assertTrue(sequenceInfo.getStreamSequence() >= 2);

        });
    }

    @Test
    public void testJsSourceUnbounded() throws Exception {
        String sourceSubject = random("sub");
        String streamName = random("strm");
        String consumerName = random("con");

        runInServer(true, (nc, url) -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = jsm.jetStream();
            createStream(jsm, streamName, sourceSubject);

            ConsumerConfiguration cc = createConsumer(jsm, streamName, sourceSubject, consumerName, 100);
            // --------------------------------------------------------------------------------
            Properties connectionProperties = defaultConnectionProperties(url);
            PayloadDeserializer<String> deserializer = new StringPayloadDeserializer();
            NatsJetStreamSourceBuilder<String> builder = new NatsJetStreamSourceBuilder<String>()
                .subjects(sourceSubject)
                .payloadDeserializer(deserializer)
                .connectionProperties(connectionProperties)
                .consumerName(consumerName)
                .maxFetchRecords(100)
                .maxFetchTime(Duration.ofSeconds(5))
                .boundness(Boundedness.CONTINUOUS_UNBOUNDED);

            // Flink environment setup
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<String> ds = env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "nats-source-input");
            ds.map(String::toUpperCase);

            // Running Flink job in a separate thread
            env.executeAsync("TestJsSourceUnbounded");
            publish(js, sourceSubject, 5, 0);

            Thread.sleep(15_000L); // Increased sleep time to ensure messages are processed
            env.close();
            SequenceInfo sequenceInfo = nc.jetStream().getConsumerContext(streamName, consumerName).getConsumerInfo().getDelivered();
            assertTrue(sequenceInfo.getStreamSequence() >= 5);
        });
    }

    private static ConsumerConfiguration createConsumer(JetStreamManagement jsm, String streamName, String sourceSubject, String consumerName, int maxBatch) throws IOException, JetStreamApiException {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
            .durable(consumerName)
            .ackPolicy(AckPolicy.All)
            .filterSubject(sourceSubject)
            .maxBatch(maxBatch)
            .build();
        jsm.addOrUpdateConsumer(streamName, cc);
        return cc;
    }

    private static void createStream(JetStreamManagement jsm, String streamName, String sourceSubject) throws IOException, JetStreamApiException {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
            .name(streamName)
            .subjects(sourceSubject)
            .storageType(StorageType.Memory)
            .build();
        jsm.addStream(streamConfig);
    }
}

