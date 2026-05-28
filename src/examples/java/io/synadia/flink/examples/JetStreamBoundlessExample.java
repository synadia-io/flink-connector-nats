// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StorageType;
import io.synadia.flink.examples.support.ExampleUtils;
import io.synadia.flink.message.Utf8StringSourceConverter;
import io.synadia.flink.source.AckBehavior;
import io.synadia.flink.source.JetStreamSource;
import io.synadia.flink.source.JetStreamSourceBuilder;
import io.synadia.flink.source.JetStreamSubjectConfiguration;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import static io.synadia.flink.examples.JetStreamExampleHelper.*;

public class JetStreamBoundlessExample {
    // ==========================================================================================
    // General Configuration: Use these settings to change how the example runs
    // ==========================================================================================

    // ------------------------------------------------------------------------------------------
    // This job name is used by flink for management, including the naming
    // of threads, which might appear in logging.
    // ------------------------------------------------------------------------------------------
    public static final String JOB_NAME = "jsbe";

    // ------------------------------------------------------------------------------------------
    // 0 or less don't report
    // This is just set so you can see a reasonable amount of progress
    // ------------------------------------------------------------------------------------------
    public static final int SINK_REPORT_FREQUENCY = 100;
    public static final int PUBLISH_REPORT_FREQUENCY = 500;

    // ==========================================================================================
    // JetStreamSource Configuration: Use these settings to change how the source is configured
    // ==========================================================================================

    // ------------------------------------------------------------------------------------------
    // AckBehavior for the source, the behavior that the source will use to acknowledge messages.
    // For this example we recommend only 2 of the 4 behaviors since they are the most likely
    // desired behaviors. For the other behaviors, see the JetStreamDoNotAckExample.
    // AckBehavior.NoAck - Ordered consumer used, no acks, messages are not acknowledged.
    //                     This is the default if no behavior is set.
    // AckBehavior.AckAll - Consumer uses AckPolicy.All. Messages are tracked as they are sourced
    //                      and the last one is acked at checkpoint
    // ------------------------------------------------------------------------------------------
    public static final AckBehavior ACK_BEHAVIOR = AckBehavior.NoAck;

    // ==========================================================================================
    // Flink Configuration: Use these settings to change how Flink runs
    // ==========================================================================================

    // ------------------------------------------------------------------------------------------
    // if > 0 parallelism will manually set to this value
    // Try 3 or 1
    // ------------------------------------------------------------------------------------------
    public static final int PARALLELISM = 1;

    // ------------------------------------------------------------------------------------------
    // if > 0, how often in milliseconds to checkpoint, otherwise checkpoint will not be done
    // Try 5000 or 0
    // ------------------------------------------------------------------------------------------
    public static final int CHECKPOINTING_INTERVAL = 5000;

    // ------------------------------------------------------------------------------------------
    // Server
    // MaxReconnects for testing, try 0 or 5 and see what happens when you shutdown the server
    // ------------------------------------------------------------------------------------------
    public static final String SERVER = "nats://localhost:4222";
    public static final int MAX_RECONNECTS = 0;

    public static void main(String[] args) throws Exception {
        // ==========================================================================================
        // Setup
        // ==========================================================================================
        // Make a connection to use for setting up streams
        // 1. We need data that the source will consume
        // 2. We need a stream/subject for the sink to publish to
        Properties props = new Properties();
        props.setProperty("io.nats.client.url", SERVER);
        props.setProperty("io.nats.client.reconnect.max", Integer.toString(MAX_RECONNECTS));
        Connection nc = ExampleUtils.connect(props);
        JetStreamManagement jsm = nc.jetStreamManagement();

        System.out.println("Setting up data stream: " + SOURCE_A_STREAM + "/" + SOURCE_A_SUBJECT);
        ExampleUtils.createOrReplaceStream(jsm, StorageType.File, SOURCE_A_STREAM, SOURCE_A_SUBJECT);

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
            .ackBehavior(ACK_BEHAVIOR)
            .build();
        System.out.println("JetStreamSubjectConfiguration" + subjectConfigurationA.toJson());

        // ------------------------------------------------------------------------------------------
        // The JetStreamSource
        // ------------------------------------------------------------------------------------------
        // Build the source by setting up the connection properties, the message supplier
        // and subject configurations, etc.
        // ------------------------------------------------------------------------------------------
        // A Utf8StringSourceConverter takes the NATS Message and outputs its data payload as a String
        // When we published to these streams, the data is in the form "data--<subject>--<num>"
        // ------------------------------------------------------------------------------------------
        JetStreamSource<String> source = new JetStreamSourceBuilder<String>()
            .connectionPropertiesFile(ExampleUtils.EXAMPLES_CONNECTION_PROPERTIES_FILE)
            .sourceConverter(new Utf8StringSourceConverter())
            .addSubjectConfigurations(subjectConfigurationA)
            .build();

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
        dataStream.sinkTo(new JsbeSink());

        env.executeAsync(JOB_NAME);

        // ==========================================================================================
        // Start publishing messages via JetStreamExampleHelper.
        // publishAsync(JetStream js, String subject, long delay, long jitter, int reportFrequency)
        // ==========================================================================================
        boolean connected = publishAsync(nc, SOURCE_A_SUBJECT, 10, 10, PUBLISH_REPORT_FREQUENCY, MAX_RECONNECTS == 0);

        if (!connected) {
            System.exit(0); // Threads are running, stuff still going, so force exit. Probably not a production strategy!
        }
    }

    static class JsbeSink implements Sink<String>, SupportsConcurrentExecutionAttempts {
        @SuppressWarnings("deprecation")
        @Override
        public SinkWriter<String> createWriter(InitContext context) throws IOException {
            return new JsbeSinkWriter(
                context.getTaskInfo().getIndexOfThisSubtask(),
                context.getTaskInfo().getNumberOfParallelSubtasks());
        }
    }

    static class JsbeSinkWriter implements SinkWriter<String>, Serializable {
        private static final long serialVersionUID = 1L;

        private final String label;
        private long messageCount;

        public JsbeSinkWriter(int subtaskIndex, int numParallelSubtasks) {
            if (numParallelSubtasks > 1) {
                this.label = "Sink." + subtaskIndex + ": ";
            }
            else {
                this.label = "Sink: ";
            }
        }

        @Override
        public void write(String data, Context context) throws IOException, InterruptedException {
            int at = data.lastIndexOf('-');
            long messageId = Long.parseLong(data.substring(at + 1));
            if (messageId != ++messageCount) {
                throw new RuntimeException("Element validation failed: " + data + ", " + messageCount);
            }
            if (messageCount % SINK_REPORT_FREQUENCY == 0) {
                System.out.println(timeLabel() + label + data);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {}

        @Override
        public void close() throws Exception {}
    }
}
