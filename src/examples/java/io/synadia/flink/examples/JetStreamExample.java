// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.examples;

import io.nats.client.*;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.synadia.flink.examples.support.ExampleUtils;
import io.synadia.flink.examples.support.Publisher;
import io.synadia.flink.payload.StringPayloadDeserializer;
import io.synadia.flink.payload.StringPayloadSerializer;
import io.synadia.flink.sink.JetStreamSink;
import io.synadia.flink.sink.JetStreamSinkBuilder;
import io.synadia.flink.source.JetStreamSource;
import io.synadia.flink.source.JetStreamSourceBuilder;
import io.synadia.flink.source.JetStreamSubjectConfiguration;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.synadia.flink.examples.JetStreamExampleHelper.*;

public class JetStreamExample {
    // ==========================================================================================
    // Example Configuration: Use these settings to change how the example runs
    // ==========================================================================================

    // ------------------------------------------------------------------------------------------
    // This job name is used by flink for management, including the naming
    // of threads, which might appear in logging.
    // ------------------------------------------------------------------------------------------
    public static final String JOB_NAME = "jse";

    // ------------------------------------------------------------------------------------------
    // 0 or less don't report
    // This is just set so you can see a reasonable amount of progress
    // ------------------------------------------------------------------------------------------
    public static final int REPORT_FREQUENCY = 20000;

    // ------------------------------------------------------------------------------------------
    // set the quiet period longer if you have acks 10000 vs 3000 for instance
    // Try 3000 or 10000
    // ------------------------------------------------------------------------------------------
    public static final int QUIET_PERIOD = 3000;

    // ==========================================================================================
    // JetStreamSource Configuration: Use these settings to change how the source is configured
    // ==========================================================================================

    // ------------------------------------------------------------------------------------------
    // ACK false means use an ordered consumer with no acking
    // ACK true means the split(s) will ack (AckPolicy.All) messages at the checkpoint
    // Try false or true
    // ------------------------------------------------------------------------------------------
    public static final boolean ACK = false;

    // ------------------------------------------------------------------------------------------
    // <= 0 makes the source Boundedness.CONTINUOUS_UNBOUNDED
    // > 0 makes the source Boundedness.BOUNDED by giving it a maximum number of messages to read
    // Try 0 or 50000
    // ------------------------------------------------------------------------------------------
    public static final int MAX_MESSAGES_TO_READ = 0;

    // ==========================================================================================
    // Flink Configuration: Use these settings to change how Flink runs
    // ==========================================================================================

    // ------------------------------------------------------------------------------------------
    // if > 0 parallelism will manually set to this value
    // Try 3 or 1
    // ------------------------------------------------------------------------------------------
    public static final int PARALLELISM = 3;

    // ------------------------------------------------------------------------------------------
    // if > 0, how often in milliseconds to checkpoint, otherwise checkpoint will not be done
    // Try 5000 or 0
    // ------------------------------------------------------------------------------------------
    public static final int CHECKPOINTING_INTERVAL = 5000;

    public static void main(String[] args) throws Exception {
        // ==========================================================================================
        // Setup
        // ==========================================================================================
        // Make a connection to use for setting up streams
        // 1. We need data that the source will consume
        // 2. We need a stream/subject for the sink to publish to
        Connection nc = ExampleUtils.connect(ExampleUtils.CONNECTION_PROPS_FILE);
        setupSinkStream(nc);
        setupDataStreams(nc);

        // ==========================================================================================
        // Create a JetStream source
        // ==========================================================================================
        // JetStreamSubjectConfiguration are the key to building a source.
        // Each stream must have its own configuration, but don't worry, you'll see that
        // the source builder can add multiple subject configurations, including
        // both instances and lists of JetStreamSubjectConfiguration.
        // The main restriction is that all configurations for a source
        // must be the same type of Boundedness
        // ------------------------------------------------------------------------------------------

        // ------------------------------------------------------------------------------------------
        // A single JetStreamSubjectConfiguration, one subject for the stream.
        // ------------------------------------------------------------------------------------------
        // Configure the stream, it's subjects, and other source behavior
        // The buildWithSubject method returns an instance of JetStreamSubjectConfiguration.
        // Use this when you have only one subject for a given stream/configuration
        // ------------------------------------------------------------------------------------------
        JetStreamSubjectConfiguration subjectConfigurationA = JetStreamSubjectConfiguration.builder()
            .streamName(SOURCE_A_STREAM)
            .maxMessagesToRead(MAX_MESSAGES_TO_READ)
            .ack(ACK)
            .buildWithSubject(SOURCE_A_SUBJECT);
        System.out.println("JetStreamSubjectConfiguration" + subjectConfigurationA.toJson());

        // ------------------------------------------------------------------------------------------
        // A list of JetStreamSubjectConfiguration, multiple subjects for one stream.
        // ------------------------------------------------------------------------------------------
        // The buildWithSubjects method returns a list of JetStreamSubjectConfiguration.
        // Use this when you have multiple subjects for a given stream/configuration
        // ------------------------------------------------------------------------------------------
        List<JetStreamSubjectConfiguration> subjectConfigurationsB = JetStreamSubjectConfiguration.builder()
            .streamName(SOURCE_B_STREAM)
            .maxMessagesToRead(MAX_MESSAGES_TO_READ)
            .ack(ACK)
            .buildWithSubjects(SOURCE_B_SUBJECTS);
        for (JetStreamSubjectConfiguration jssc : subjectConfigurationsB) {
            System.out.println("JetStreamSubjectConfiguration" + jssc.toJson());
        }

        // ------------------------------------------------------------------------------------------
        // The JetStreamSource
        // ------------------------------------------------------------------------------------------
        // Build the source by setting up the connection properties, the deserializer
        // and subject configurations, etc.
        // ------------------------------------------------------------------------------------------
        // A StringPayloadDeserializer takes the Nats Message and output's it's data payload as a String
        // When we published to these streams the data is in the form "data--<subject>--<num>"
        // ------------------------------------------------------------------------------------------
        JetStreamSource<String> source = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(ExampleUtils.CONNECTION_PROPS_FILE)
            .payloadDeserializer(new StringPayloadDeserializer())
            .addSubjectConfigurations(subjectConfigurationA)
            .addSubjectConfigurations(subjectConfigurationsB)
            .build();

        // ==========================================================================================
        // Create a JetStream sink
        // ==========================================================================================
        // A JetStream sink publishes to a JetStream subject
        // !Technically! you can publish to a JetStream subject with a NATS core publish
        // but that can overwhelm the server very quickly because we can publish so fast
        // The version of JetStreamSink as of this writing only uses synchronized publishing
        // but future versions will be more robust and take advantage of the publishing utilities
        // in the Orbit project. So right now the configuration is pretty simple.
        // ------------------------------------------------------------------------------------------
        // When we published to the source streams the data was in the form "data--<subject>--<num>"
        // The sink takes that payload and publishes it as the message payload to the SINK_SUBJECT
        // ------------------------------------------------------------------------------------------
        // We have one sink for all those source subjects. This means that all messages from
        // all those sources get "sinked" to the same JetStream subject
        // This may or not be a real use-case, it's here for example.
        // ------------------------------------------------------------------------------------------
        JetStreamSink<String> sink = new JetStreamSinkBuilder<String>()
            .connectionPropertiesFile(ExampleUtils.CONNECTION_PROPS_FILE)
            .payloadSerializer(new StringPayloadSerializer())
            .subjects(SINK_SUBJECT)
            .build();
        System.out.println(sink.toString());

        // ==========================================================================================
        // Setup and start flink
        // ==========================================================================================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        if (CHECKPOINTING_INTERVAL > 0) {
            env.enableCheckpointing(CHECKPOINTING_INTERVAL);
        }
        if (PARALLELISM > 0) {
            env.setParallelism(PARALLELISM);
        }

        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), JOB_NAME);
        dataStream.sinkTo(sink);

        env.executeAsync(JOB_NAME);

        // ==========================================================================================
        // Consume messages that the sink produces
        // ==========================================================================================
        // Since we are using a JetStreamSink, messages are getting published to a stream subject.
        // Here we will consume messages that the JetStreamSink published, to demonstrate
        // that we got a message all the way from a source to this sink stream subject
        // ------------------------------------------------------------------------------------------
        StreamContext sc = nc.getStreamContext(SINK_STREAM_NAME);
        OrderedConsumerContext occ = sc.createOrderedConsumer(
            new OrderedConsumerConfiguration().filterSubjects(SINK_SUBJECT));
        try (IterableConsumer consumer = occ.iterate()) {
            long lastMessageReceived = System.currentTimeMillis() + 5000; // 5000 gives it a little time to get started
            int manualTotal = 0;
            Map<String, AtomicInteger> receivedMap = new HashMap<>();
            long sinceLastMessage;
            do {
                Message m = consumer.nextMessage(1000);
                if (m == null) {
                    sinceLastMessage = System.currentTimeMillis() - lastMessageReceived;
                }
                else {
                    // the extractSubject method pulls the subject out of the data string so we
                    // can count the number of messages published per source subject.
                    String data = new String(m.getData());
                    String publishedSubject = Publisher.extractSubject(data);
                    AtomicInteger count = receivedMap.computeIfAbsent(publishedSubject, k -> new AtomicInteger());
                    count.incrementAndGet();
                    lastMessageReceived = System.currentTimeMillis();
                    sinceLastMessage = 0;
                    manualTotal++;
                    if (REPORT_FREQUENCY > 0) {
                        if (manualTotal % REPORT_FREQUENCY == 0) {
                            ExampleUtils.reportSinkListener(receivedMap, manualTotal);
                        }
                    }
                }
            } while (manualTotal < SOURCES_TOTAL_MESSAGES && sinceLastMessage < QUIET_PERIOD);

            ExampleUtils.reportSinkListener(receivedMap, manualTotal);
        }

        System.exit(0); // Threads are running, stuff still going, so force exit. Probably not a production strategy!
    }
}
