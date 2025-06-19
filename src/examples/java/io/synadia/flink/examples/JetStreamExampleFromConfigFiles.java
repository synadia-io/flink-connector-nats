// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.examples;

import io.nats.client.*;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.synadia.flink.examples.support.ExampleUtils;
import io.synadia.flink.examples.support.Publisher;
import io.synadia.flink.sink.JetStreamSink;
import io.synadia.flink.sink.JetStreamSinkBuilder;
import io.synadia.flink.source.JetStreamSource;
import io.synadia.flink.source.JetStreamSourceBuilder;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.synadia.flink.examples.JetStreamExampleHelper.*;

/**
 * This is the same workflow as JetStreamExample, but it creates the source and sink
 * object using a JSON or YAML file instead of being coded.
 * Run the JetStreamExample first, and it will generate the config files, overwriting
 * the ones packaged in the examples' resource directory. See the constants in
 * JetStreamExample for file locations.
 */
public class JetStreamExampleFromConfigFiles {
    // ==========================================================================================
    // General Configuration: Use these settings to change how the example runs
    // ==========================================================================================

    // ------------------------------------------------------------------------------------------
    // This job name is used by flink for management, including the naming
    // of threads, which might appear in logging.
    // ------------------------------------------------------------------------------------------
    public static final String JOB_NAME = "jsefcf";

    // ------------------------------------------------------------------------------------------
    // 0 or less don't report
    // This is just set so you can see a reasonable amount of progress
    // ------------------------------------------------------------------------------------------
    public static final int REPORT_FREQUENCY = 20000;

    // ------------------------------------------------------------------------------------------
    // The quiet period is how long to wait when not receiving messages to end the program.
    // Set the quiet period longer if you have acks 10000 vs. 3000 for instance.
    // Try 3000 or 10000
    // ------------------------------------------------------------------------------------------
    public static final int QUIET_PERIOD = 10000;

    // ------------------------------------------------------------------------------------------
    // Set this flag to tell the program to use the JSON or YAML file for input.
    // true means use the JSON. false means use the YAML
    // The input file is determined by the constants in JetStreamExampleHelper.java
    // either SOURCE_CONFIG_FILE_JSON or SOURCE_CONFIG_FILE_YAML
    // ------------------------------------------------------------------------------------------
    public static final boolean USE_JSON_NOT_YAML = true;

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
        Connection nc = ExampleUtils.connect(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE);
        setupSinkStream(nc);
        setupDataStreams(nc);

        // ==========================================================================================
        // Create a JetStreamSource from a config file
        // ==========================================================================================
        // Build the source by setting up the connection properties, and the json or yaml config file
        // ------------------------------------------------------------------------------------------
        JetStreamSource<String> source;
        JetStreamSourceBuilder<String> sourceBuilder = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE);
        if (USE_JSON_NOT_YAML) {
            source = sourceBuilder.jsonConfigFile(JetStreamExample.SOURCE_CONFIG_FILE_JSON).build();
            System.out.println("Source as configured via JSON\n" + source.toJson());
        }
        else {
            source = sourceBuilder.yamlConfigFile(JetStreamExample.SOURCE_CONFIG_FILE_YAML).build();
            System.out.println("Source as configured via Yaml\n" + source.toYaml());
        }

        // ==========================================================================================
        // Create a JetStream sink
        // ==========================================================================================
        // A JetStream sink publishes to a JetStream subject
        // ------------------------------------------------------------------------------------------
        // When we published to the source streams, the data was in the form "data--<subject>--<num>"
        // The sink takes that payload and publishes it as the message payload
        // to all the sink subjects. For this example, there is only one sink subject, see SINK_SUBJECT
        // ------------------------------------------------------------------------------------------
        JetStreamSink<String> sink;
        JetStreamSinkBuilder<String> sinkBuilder = new JetStreamSinkBuilder<String>()
            .connectionPropertiesFile(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE);
        if (USE_JSON_NOT_YAML) {
            sink = sinkBuilder.jsonConfigFile(JetStreamExample.SINK_CONFIG_FILE_JSON).build();
            System.out.println("Sink as configured via JSON\n" + sink.toJson());
        }
        else {
            sink = sinkBuilder.yamlConfigFile(JetStreamExample.SINK_CONFIG_FILE_YAML).build();
            System.out.println("Sink as configured via Yaml\n" + sink.toYaml());
        }

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
