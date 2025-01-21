// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.io.synadia.flink.v0;

import io.nats.client.*;
import io.nats.client.api.*;
import io.synadia.flink.v0.payload.*;
import io.synadia.flink.v0.sink.NatsJetStreamSink;
import io.synadia.flink.v0.source.NatsJetStreamSource;
import io.synadia.flink.v0.source.NatsJetStreamSourceBuilder;
import io.synadia.flink.v0.sink.NatsSink;
import io.synadia.flink.v0.sink.NatsSinkBuilder;
import io.synadia.flink.v0.utils.ConnectionFactory;
import io.synadia.io.synadia.flink.TestBase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

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
    public void testSourceWithPayloadBytes() throws Exception {
        final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());

        String sourceSubject = random("sub-payload");
        String streamName = random("strm-payload");
        String consumerName = random("con-payload");
        String sinkSubject = random("sink-payload");

        runInServer(true, (nc, url) -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = jsm.jetStream();

            // Step 1: Create the source stream and publish messages
            createStream(jsm, streamName, sourceSubject);
            publish(js, sourceSubject, 10);

            // Step 2: Create a JetStream consumer
            createExplicitConsumer(jsm, streamName, sourceSubject, consumerName);

            // Step 3: Configure the NATS JetStream Source
            Properties connectionProperties = defaultConnectionProperties(url);
            PayloadBytesDeserializer deserializer = new PayloadBytesDeserializer();
            NatsJetStreamSourceBuilder<Payload<byte[]>> builder =
                    new NatsJetStreamSourceBuilder<Payload<byte[]>>()
                            .subjects(sourceSubject)
                            .payloadDeserializer(deserializer)
                            .connectionProperties(connectionProperties)
                            .consumerName(consumerName)
                            .maxFetchRecords(100)
                            .maxFetchTime(Duration.ofSeconds(5))
                            .boundness(Boundedness.BOUNDED);

            // Step 4: Set up Flink Streaming Environment
            NatsJetStreamSource<Payload<byte[]>> natsSource = builder.build();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<Payload<byte[]>> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-input");

            // Step 5: Listen to the sink subject and collect messages into syncList
            Dispatcher dispatcher = nc.createDispatcher();
            dispatcher.subscribe(sinkSubject, syncList::add); // Collect sink messages

            NatsJetStreamSink<Payload<byte[]>> sink = newNatsJetStreamPayloadBytesSink(sinkSubject, connectionProperties, null);
            ds.sinkTo(sink);

            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
            env.executeAsync("testSourceWithPayloadBytes");

            // Step 6: Wait for processing to complete
            Thread.sleep(12_000);

            // Step 7: Validate received messages at sink
            for (Message m : syncList) {
                System.out.printf("Listening. Subject: %s Payload: %s\n", m.getSubject(), new String(m.getData()));
            }

            assertEquals(10, syncList.size(), "All 10 messages should be received at the sink.");

            // Step 8: Cleanup and validation
            env.close();

            // wait for the execution environment to finish
            Thread.sleep(7_000);
            jsm.deleteStream(streamName);
            nc.close();
        });
    }

    @Test
    public void testReplyToBounded() throws Exception {
        String sourceSubject = random("sub");
        String streamName = random("strm");
        String consumerName = random("con");

        runInServer(true, (nc, url) -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = jsm.jetStream();

            // Step 1: Create the source stream and publish messages
            createStream(jsm, streamName, sourceSubject);
            publish(js, sourceSubject, 10);

            // Step 2: Create a JetStream consumer
            createExplicitConsumer(jsm, streamName, sourceSubject, consumerName);

            // Step 3: Configure the NATS JetStream Source
            Properties connectionProperties = defaultConnectionProperties(url);
            PayloadBytesDeserializer deserializer = new PayloadBytesDeserializer();
            NatsJetStreamSourceBuilder<Payload<byte[]>> builder =
                    new NatsJetStreamSourceBuilder<Payload<byte[]>>()
                            .subjects(sourceSubject)
                            .payloadDeserializer(deserializer)
                            .connectionProperties(connectionProperties)
                            .consumerName(consumerName)
                            .maxFetchRecords(100)
                            .maxFetchTime(Duration.ofSeconds(5))
                            .boundness(Boundedness.BOUNDED);

            // Step 4: Set up Flink Streaming Environment
            NatsJetStreamSource<Payload<byte[]>> natsSource = builder.build();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<Payload<byte[]>> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-input");

            // Step 5: Process the messages and ack them
            ConnectionFactory connectionFactory = new ConnectionFactory(connectionProperties);
            ds.flatMap(new AckMessageFunction(connectionFactory, 1, 5));

            // Step 6: Set Flink restart strategy and execute the job asynchronously
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
            env.executeAsync("testReplyToBounded");

            // Step 7: Wait for processing to complete
            Thread.sleep(12_000);

            // Step 8: Get ConsumerInfo and validate
            ConsumerInfo consumerInfo = jsm.getConsumerInfo(streamName, consumerName);
            assertEquals(5, consumerInfo.getNumAckPending(), "5/10 messages should be acked.");

            // Step 9: Cleanup and validation
            env.close();

            // wait for the execution environment to finish
            Thread.sleep(7_000);
            jsm.deleteStream(streamName);
            nc.close();
        });
    }

    @Test
    public void testJsSourceBounded() throws Exception {
        final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());
        String sourceSubject = random("sub");
        String sinkSubject = random("sink");
        String streamName = random("strm");
        String consumerName = random("con");

        runInServer(true, (nc, url) -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = jsm.jetStream();

            // Step 1: Create the source stream and publish messages
            createStream(jsm, streamName, sourceSubject);
            publish(js, sourceSubject, 10);

            // Step 2: Create a JetStream consumer
            createConsumer(jsm, streamName, sourceSubject, consumerName);

            // Step 3: Configure the NATS JetStream Source
            Properties connectionProperties = defaultConnectionProperties(url);
            StringPayloadDeserializer deserializer = new StringPayloadDeserializer();
            NatsJetStreamSourceBuilder<String> builder =
                    new NatsJetStreamSourceBuilder<String>()
                            .subjects(sourceSubject)
                            .payloadDeserializer(deserializer)
                            .connectionProperties(connectionProperties)
                            .consumerName(consumerName)
                            .maxFetchRecords(100)
                            .maxFetchTime(Duration.ofSeconds(5))
                            .boundness(Boundedness.BOUNDED);

            // Step 4: Set up Flink Streaming Environment
            NatsJetStreamSource<String> natsSource = builder.build();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getCheckpointConfig().setCheckpointInterval(10_000L);
            DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-input");

            // Step 5: Listen to the sink subject and collect messages into syncList
            Dispatcher dispatcher = nc.createDispatcher();
            dispatcher.subscribe(sinkSubject, syncList::add); // Collect sink messages

            // Step 6: Configure a NATS Sink to write processed data
            NatsSink<String> sink = new NatsSinkBuilder<String>()
                    .subjects(sinkSubject)
                    .connectionProperties(connectionProperties)
                    .payloadSerializer(new StringPayloadSerializer()) // Serialize messages for sink
                    .build();
            ds.map(String::toUpperCase).sinkTo(sink);

            // Step 7: Set Flink restart strategy and execute the job asynchronously
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
            env.executeAsync("TestJsSourceBounded");

            // Step 8: Wait for processing to complete
            Thread.sleep(12_000);

            // Step 9: Cleanup and validation
            env.close();
            assertEquals(10, syncList.size(), "All 10 messages should be received at the sink.");
            jsm.deleteStream(streamName);
            nc.close();
        });
    }


    @Test
    public void testJsSourceUnbounded() throws Exception {
        final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());
        String sourceSubject = random("sub");
        String sinkSubject = random("sink");
        String streamName = random("strm");
        String consumerName = random("con");

        runInServer(true, (nc, url) -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = jsm.jetStream();

            // Step 1: Create the source stream and publish messages
            createStream(jsm, streamName, sourceSubject);
            createConsumer(jsm, streamName, sourceSubject, consumerName);

            // Step 2: NATS JetStream Source configuration
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

            // Step 3: Flink environment setup
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // Source: Read from NATS
            DataStream<String> ds = env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "nats-source-input");

            // Step 4: Set up a Dispatcher to listen to the sink subject and capture messages
            Dispatcher d = nc.createDispatcher();
            d.subscribe(sinkSubject, syncList::add);

            // Sink: Write to a different subject
            NatsSink<String> sink = new NatsSinkBuilder<String>()
                    .subjects(sinkSubject)
                    .connectionProperties(connectionProperties)
                    .payloadSerializer(new StringPayloadSerializer()) // Serialize messages for sink
                    .build();
            ds.map(String::toUpperCase).sinkTo(sink);

            // Step 5: Execute the Flink job asynchronously
            JobClient jobClient = env.executeAsync("TestJsSourceUnbounded");

            // Step 6: Publish messages to NATS JetStream
            publish(js, sourceSubject, 5, 100);

            // Wait for messages to process
            Thread.sleep(10_000);

            // Step 7: Verify received messages at sink
            assertEquals(5, syncList.size(), "All 5 messages should be received at the sink.");

            // Step 8: Cancel the Flink job gracefully
            try {
                jobClient.cancel().get();
                System.out.println("Flink job canceled successfully.");
            } catch (Exception e) {
                e.printStackTrace();
                fail("Failed to cancel Flink job: " + e.getMessage());
            }

            // Step 9: Cleanup
            env.close();

            // wait for the execution environment to finish
            // flink will take time to close it resources
            // if this is not done, code will attempt to delete jet stream
            // while one of the fetcher threads might be polling
            Thread.sleep(5_000L);

            // delete stream now
            jsm.deleteStream(streamName);
        });
    }

    private static ConsumerConfiguration createConsumer(JetStreamManagement jsm, String streamName, String sourceSubject, String consumerName) throws IOException, JetStreamApiException {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
            .durable(consumerName)
            .ackPolicy(AckPolicy.All)
            .filterSubject(sourceSubject)
            .build();
        jsm.addOrUpdateConsumer(streamName, cc);
        return cc;
    }

    private static void createExplicitConsumer(JetStreamManagement jsm, String streamName, String sourceSubject, String consumerName) throws IOException, JetStreamApiException {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(consumerName)
                .ackPolicy(AckPolicy.Explicit)
                .filterSubject(sourceSubject)
                .build();

        jsm.addOrUpdateConsumer(streamName, cc);
    }

    private static void createStream(JetStreamManagement jsm, String streamName, String sourceSubject) throws IOException, JetStreamApiException {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
            .name(streamName)
            .subjects(sourceSubject)
            .build();
        jsm.addStream(streamConfig);
    }
}

