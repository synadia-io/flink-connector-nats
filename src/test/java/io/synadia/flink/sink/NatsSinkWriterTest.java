// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.sink;

import io.nats.client.Message;
import io.nats.client.Subscription;
import io.synadia.flink.TestBase;
import io.synadia.flink.message.Utf8StringSinkConverter;
import io.synadia.flink.sink.writer.NatsSinkWriter;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

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
                new Utf8StringSinkConverter(),
                new ConnectionFactory(defaultConnectionProperties(url)),
                new MockWriterInitContext()
        );
    }
}
