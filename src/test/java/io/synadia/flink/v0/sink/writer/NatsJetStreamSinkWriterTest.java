// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.sink.writer;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.v0.payload.StringPayloadSerializer;
import io.synadia.flink.v0.utils.ConnectionFactory;
import io.synadia.io.synadia.flink.TestBase;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


class NatsJetStreamSinkWriterTest extends TestBase {

    /**
     * Tests NatsJetStreamSinkWriter.write() publishes messages to JetStream subjects.
     * Verifies that multiple messages are correctly published to all configured 
     * JetStream subjects.
     *
     * Flow:
     * 1. Creates JetStream stream and subjects
     * 2. Creates JetStream subscriptions
     * 3. Writes multiple messages using the writer
     * 4. Verifies all messages are received on all subjects in order
     */
    @Test
    void writeToJetStreamSubjects() throws Exception {
        runInJsServer((nc, url) -> {
            // Setup JetStream
            JetStreamManagement jsm = nc.jetStreamManagement();
            String stream = stream();
            String subject1 = subject();
            String subject2 = subject();
            List<String> subjects = Arrays.asList(subject1, subject2);

            // Create stream
            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(stream)
                    .subjects(subjects)
                    .storageType(StorageType.Memory)
                    .build();
            jsm.addStream(sc);

            // Create subscriptions
            JetStream js = nc.jetStream();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().stream(stream).build();
            Subscription sub1 = js.subscribe(subject1, pso);
            Subscription sub2 = js.subscribe(subject2, pso);
            nc.flush(Duration.ofSeconds(1));

            // Create and use writer
            NatsJetStreamSinkWriter<String> writer = createWriter(url, subjects);

            // Send multiple messages
            String[] testMessages = {
                    "Message 1",
                    "Message 2",
                    "Message 3"
            };

            for (String msg : testMessages) {
                writer.write(msg, mock(SinkWriter.Context.class));
            }
            writer.flush(false);

            // Verify messages on subject1
            for (String expectedMsg : testMessages) {
                Message msg = sub1.nextMessage(Duration.ofSeconds(1));
                assertNotNull(msg, "Message should be received on subject1");
                assertEquals(expectedMsg, new String(msg.getData()));
            }

            // Verify messages on subject2
            for (String expectedMsg : testMessages) {
                Message msg = sub2.nextMessage(Duration.ofSeconds(1));
                assertNotNull(msg, "Message should be received on subject2");
                assertEquals(expectedMsg, new String(msg.getData()));
            }

            // Verify no more messages
            assertNull(sub1.nextMessage(Duration.ofMillis(500)), "Should not receive extra messages on subject1");
            assertNull(sub2.nextMessage(Duration.ofMillis(500)), "Should not receive extra messages on subject2");

            writer.close();
        });
    }

    /**
     * Tests NatsJetStreamSinkWriter.close() properly releases resources.
     * Verifies that after closing, the writer rejects new write operations
     * and properly cleans up its JetStream connection.
     */
    @Test
    void closeDisallowsWritesAndCleansUpResources() throws Exception {
        runInJsServer((nc, url) -> {
            // Setup JetStream
            JetStreamManagement jsm = nc.jetStreamManagement();
            String stream = stream();
            String subject = subject();
            List<String> subjects = Arrays.asList(subject);

            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(stream)
                    .subjects(subjects)
                    .storageType(StorageType.Memory)
                    .build();
            jsm.addStream(sc);

            NatsJetStreamSinkWriter<String> writer = createWriter(url, subjects);
            writer.close();

            assertThrows(Exception.class, () ->
                    writer.write("Should fail", mock(SinkWriter.Context.class)));
        });
    }

    /**
     * Tests error handling when JetStream publish fails.
     * Verifies that JetStreamApiException is properly wrapped in FlinkRuntimeException.
     */
    @Test
    void writeWithJetStreamErrorThrowsFlinkRuntimeException() throws Exception {
        // TODO: Implement error handling test
    }

    /**
     * Tests NatsJetStreamSinkWriter.toString() returns the expected format.
     * Verifies that the string representation includes essential information.
     */
    @Test
    void toStringContainsEssentialInfo() throws Exception {
        runInJsServer((nc, url) -> {
            String subject = subject();
            List<String> subjects = Arrays.asList(subject);
            String sinkId = "test-sink";

            NatsJetStreamSinkWriter<String> writer = new NatsJetStreamSinkWriter<>(
                    sinkId,
                    subjects,
                    new StringPayloadSerializer(),
                    new ConnectionFactory(defaultConnectionProperties(url)),
                    new TestInitContext()
            );

            String result = writer.toString();
            assertTrue(result.contains("NatsJetStreamSinkWriter"), "Should contain class name");
            assertTrue(result.contains("id='" + writer.id + "'"), "Should contain id");
            assertTrue(result.contains("subjects=" + subjects), "Should contain subjects");

            writer.close();
        });
    }

    /**
     * Helper method to create a NatsJetStreamSinkWriter with a specific connection and subjects.
     */
    private NatsJetStreamSinkWriter<String> createWriter(String url, List<String> subjects) throws Exception {

        return new NatsJetStreamSinkWriter<>(
                "test-sink",
                subjects,
                new StringPayloadSerializer(),
                new ConnectionFactory(defaultConnectionProperties(url)),
                new TestInitContext()
        );
    }

    /**
     * Test implementation of Sink.InitContext that provides context for the writer.
     * Only getSubtaskId() and getNumberOfParallelSubtasks() return meaningful values,
     * as they're used for writer identification and parallelism information.
     */
    private static class TestInitContext implements Sink.InitContext, Serializable {
        private static final long serialVersionUID = 1L;
        @Override public int getSubtaskId() { return 1; }
        @Override public int getNumberOfParallelSubtasks() { return 4; }
        @Override public UserCodeClassLoader getUserCodeClassLoader() { return null; }
        @Override public MailboxExecutor getMailboxExecutor() { return null; }
        @Override public ProcessingTimeService getProcessingTimeService() { return null; }
        @Override public int getAttemptNumber() { return 0; }
        @Override public SinkWriterMetricGroup metricGroup() { return null; }
        @Override public OptionalLong getRestoredCheckpointId() { return null; }
        @Override public JobInfo getJobInfo() { return null;}
        @Override public TaskInfo getTaskInfo() { return null;}
        @Override public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() { return null; }
        @Override public boolean isObjectReuseEnabled() { return false; }
        @Override public <IN> TypeSerializer<IN> createInputSerializer() { return null; }
    }
}
