// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.sink.writer;

import io.nats.client.Message;
import io.nats.client.Subscription;
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

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class NatsSinkWriterTest extends TestBase {

    /**
     * Tests NatsSinkWriter.write() publishes messages to multiple subjects.
     * Verifies that a single write operation correctly publishes the same message
     * to all configured subjects.
     *
     * Flow:
     * 1. Creates two NATS subscriptions on different subjects
     * 2. Initializes a NatsSinkWriter configured to publish to both subjects
     * 3. Writes a single message which should be published to both subjects
     * 4. Verifies the same message is received on both subscriptions
     *
     * Example:
     * - Subjects: "test.1", "test.2"
     * - Message: "Hello NATS!"
     * - Expected: Both subscribers receive "Hello NATS!"
     */
    @Test
    void writeMultipleSubjectsAndPublish() throws Exception {
        runInServer((nc, url) -> {
            String subject1 = subject();
            String subject2 = subject();
            List<String> subjects = Arrays.asList(subject1, subject2);

            Subscription sub1 = nc.subscribe(subject1);
            Subscription sub2 = nc.subscribe(subject2);
            nc.flush(Duration.ofSeconds(1));

            NatsSinkWriter<String> writer = createWriter(url, subjects);
            String testMessage = "Hello NATS!";

            writer.write(testMessage, mock(SinkWriter.Context.class));
            writer.flush(false);

            Message msg1 = sub1.nextMessage(Duration.ofSeconds(1));
            Message msg2 = sub2.nextMessage(Duration.ofSeconds(1));

            assertNotNull(msg1, "Message should be received on subject1");
            assertNotNull(msg2, "Message should be received on subject2");
            assertEquals(testMessage, new String(msg1.getData()));
            assertEquals(testMessage, new String(msg2.getData()));

            writer.close();
        });
    }

    /**
     * Tests NatsSinkWriter.close() properly releases resources.
     * Verifies that after closing, the writer rejects new write operations
     * and properly cleans up its NATS connection.
     *
     * Flow:
     * 1. Creates a writer
     * 2. Closes the writer via close()
     * 3. Attempts to write a message, which should fail
     *
     * Example:
     * - Writer is closed via close()
     * - Attempting to write "Should fail" throws an exception
     */
    @Test
    void closeDisallowsWritesAndCleansUpResources() throws Exception {
        runInServer((nc, url) -> {
            String subject = subject();
            List<String> subjects = Arrays.asList(subject);

            NatsSinkWriter<String> writer = createWriter(url, subjects);
            writer.close();

            assertThrows(Exception.class, () ->
                    writer.write("Should fail", mock(SinkWriter.Context.class)));
        });
    }

    /**
     * Helper method to create a NatsSinkWriter with a specific connection and subjects.
     * Uses an anonymous ConnectionFactory that returns the provided connection.
     */
    private NatsSinkWriter<String> createWriter(String url, List<String> subjects) throws Exception {

        return new NatsSinkWriter<>(
                "test-sink",
                subjects,
                new StringPayloadSerializer(),
                new ConnectionFactory(defaultConnectionProperties(url)),
                new TestInitContext()
        );
    }

    /**
     * Test implementation of Sink.InitContext that provides serializable context for the writer.
     * Only getSubtaskId() and getNumberOfParallelSubtasks() return meaningful values,
     * as they're used for writer identification and parallelism information.
     * All other methods return null as they're not used in the writer implementation.
     *
     * Note: We can't use Mockito.mock() here because:
     * 1. Mockito mocks are not serializable by default
     * 2. This context needs to be serialized during checkpointing tests
     * 3. We need specific return values that persist through serialization
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
        @Override public JobInfo getJobInfo() { return null; }
        @Override public TaskInfo getTaskInfo() { return null; }
        @Override public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() { return null; }
        @Override public boolean isObjectReuseEnabled() { return false; }
        @Override public <IN> TypeSerializer<IN> createInputSerializer() { return null; }
    }
}
