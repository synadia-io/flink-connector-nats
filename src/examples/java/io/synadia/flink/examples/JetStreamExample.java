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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.synadia.flink.examples.JetStreamExampleHelper.*;
import static io.synadia.flink.examples.support.ExampleUtils.writeToFile;

public class JetStreamExample {
    // ==========================================================================================
    // General Configuration: Use these settings to change how the example runs
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
    // The quiet period is how long to wait when not receiving messages to end the program.
    // Set the quiet period longer if you are using ack mode. See notes on ACK_MODE below.
    // Try 3000 or 10000 in ack mode.
    // ------------------------------------------------------------------------------------------
    public static final int QUIET_PERIOD = 3000;

    // ------------------------------------------------------------------------------------------
    // Locations where to write config files based on how the example gets configured.
    // These files can be used in the JetStreamExampleFromConfigFiles example.
    // ------------------------------------------------------------------------------------------
    public static final String SOURCE_CONFIG_FILE_JSON = "src/examples/resources/js-source-config.json";
    public static final String SOURCE_CONFIG_FILE_YAML = "src/examples/resources/js-source-config.yaml";
    public static final String SINK_CONFIG_FILE_JSON = "src/examples/resources/js-sink-config.json";
    public static final String SINK_CONFIG_FILE_YAML = "src/examples/resources/js-sink-config.yaml";

    // ==========================================================================================
    // JetStreamSource Configuration: Use these settings to change how the source is configured
    // ==========================================================================================

    // ------------------------------------------------------------------------------------------
    // ACK_MODE mode false means use a consumer with no acking
    // ACK_MODE true true means the split(s) will ack (AckPolicy.All) messages at the checkpoint
    // Try false or true
    // ------------------------------------------------------------------------------------------
    public static final boolean ACK_MODE = false;

    // ------------------------------------------------------------------------------------------
    // <= 0 makes the source boundedness "Boundedness.CONTINUOUS_UNBOUNDED"
    // > 0 makes the source boundedness "Boundedness.BOUNDED" by giving it a maximum number of messages to read
    // Try -1 or 50000 or if using ack mode, try 10000
    // ------------------------------------------------------------------------------------------
    public static final int MAX_MESSAGES_TO_READ = -1;

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
        // Create a JetStream source
        // ==========================================================================================
        // JetStreamSubjectConfiguration are the key to building a source.
        // Each subject must have its own configuration.
        // The source builder can add multiple subject configurations, including
        // both instances and lists of JetStreamSubjectConfiguration.
        // ------------------------------------------------------------------------------------------
        // The main restriction is that all configurations for a source
        // must be the same type of Boundedness. Boundedness is determined
        // from the configuration of maxMessagesToRead
        // ------------------------------------------------------------------------------------------

        // ------------------------------------------------------------------------------------------
        // A single JetStreamSubjectConfiguration, one subject for the stream.
        // ------------------------------------------------------------------------------------------
        // Configure the stream, its subjects, and other source behavior
        // The buildWithSubject method returns an instance of JetStreamSubjectConfiguration.
        // Use this when you have only one subject for a given stream/configuration
        // ------------------------------------------------------------------------------------------
        JetStreamSubjectConfiguration subjectConfigurationA = JetStreamSubjectConfiguration.builder()
            .streamName(SOURCE_A_STREAM)
            .subject(SOURCE_A_SUBJECT)
            .maxMessagesToRead(MAX_MESSAGES_TO_READ)
            .ackMode(ACK_MODE)
            .build();
        System.out.println("JetStreamSubjectConfiguration" + subjectConfigurationA.toJson());

        // ------------------------------------------------------------------------------------------
        // A list of JetStreamSubjectConfiguration, multiple subjects for one stream.
        // ------------------------------------------------------------------------------------------
        // The buildWithSubjects method returns a list of JetStreamSubjectConfiguration.
        // Use this when you have multiple subjects for a given stream/configuration
        // ------------------------------------------------------------------------------------------
        List<JetStreamSubjectConfiguration> subjectConfigurationsB = new ArrayList<>();
        subjectConfigurationsB.add(JetStreamSubjectConfiguration.builder()
            .streamName(SOURCE_B_STREAM)
            .subject(SOURCE_B_SUBJECTS[0])
            .maxMessagesToRead(MAX_MESSAGES_TO_READ)
            .ackMode(ACK_MODE)
            .build());
        for (int x = 1; x < SOURCE_B_SUBJECTS.length; x++) {
            subjectConfigurationsB.add(JetStreamSubjectConfiguration.builder()
                .copy(subjectConfigurationsB.get(0))
                .subject(SOURCE_B_SUBJECTS[x])
                .build());
        }
        for (JetStreamSubjectConfiguration jssc : subjectConfigurationsB) {
            System.out.println("JetStreamSubjectConfiguration" + jssc.toJson());
        }

        // ------------------------------------------------------------------------------------------
        // The JetStreamSource
        // ------------------------------------------------------------------------------------------
        // Build the source by setting up the connection properties, the deserializer
        // and subject configurations, etc.
        // ------------------------------------------------------------------------------------------
        // A StringPayloadDeserializer takes the NATS Message and outputs its data payload as a String
        // When we published to these streams, the data is in the form "data--<subject>--<num>"
        // ------------------------------------------------------------------------------------------
        JetStreamSource<String> source = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE)
            .payloadDeserializer(new StringPayloadDeserializer())
            .addSubjectConfigurations(subjectConfigurationA)
            .addSubjectConfigurations(subjectConfigurationsB)
            .build();

        // ------------------------------------------------------------------------------------------
        // Here we write the source config out to a file in different formats.
        // The files can be used in the JetStreamExampleFromConfigFiles example.
        // ------------------------------------------------------------------------------------------
        writeToFile(SOURCE_CONFIG_FILE_JSON, source.toJson());
        writeToFile(SOURCE_CONFIG_FILE_YAML, source.toYaml());

        // ==========================================================================================
        // Create a JetStream sink
        // ==========================================================================================
        // A JetStream sink publishes to a JetStream subject
        // ------------------------------------------------------------------------------------------
        // When we published to the source streams, the data was in the form "data--<subject>--<num>"
        // The sink takes that payload and publishes it as the message payload
        // to all the sink subjects. For this example, there is only one sink subject, see SINK_SUBJECT
        // ------------------------------------------------------------------------------------------
        JetStreamSink<String> sink = new JetStreamSinkBuilder<String>()
            .connectionPropertiesFile(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE)
            .payloadSerializer(new StringPayloadSerializer())
            .subjects(SINK_SUBJECT)
            .build();

        // ------------------------------------------------------------------------------------------
        // Here we write the sink config out to a file in different formats.
        // The files can be used in the JetStreamExampleFromConfigFiles example.
        // ------------------------------------------------------------------------------------------
        writeToFile(SINK_CONFIG_FILE_JSON, sink.toJson());
        writeToFile(SINK_CONFIG_FILE_YAML, sink.toYaml());

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
