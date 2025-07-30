// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.sink;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.TestBase;
import io.synadia.flink.message.Utf8StringSinkConverter;
import io.synadia.flink.sink.writer.JetStreamSinkWriter;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;


class JetStreamSinkWriterTest extends TestBase {

    /**
     * Tests JetStreamSinkWriter.write() publishes messages to JetStream subjects.
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
        JetStreamSinkWriter<String> writer = createWriter(url, subjects);

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
    }

    /**
     * Tests JetStreamSinkWriter.close() properly releases resources.
     * Verifies that after closing, the writer rejects new write operations
     * and properly cleans up its JetStream connection.
     */
    @Test
    void closeDisallowsWritesAndCleansUpResources() throws Exception {
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

        JetStreamSinkWriter<String> writer = createWriter(url, subjects);
        writer.close();

        assertThrows(Exception.class, () ->
            writer.write("Should fail", mock(SinkWriter.Context.class)));
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
     * Tests JetStreamSinkWriter.toString() returns the expected format.
     * Verifies that the string representation includes essential information.
     */
    @Test
    void toStringContainsEssentialInfo() throws Exception {
        String subject = subject();
        List<String> subjects = List.of(subject);
        String sinkId = "test-sink";

        JetStreamSinkWriter<String> writer = new JetStreamSinkWriter<>(
            sinkId,
            subjects,
            new Utf8StringSinkConverter(),
            new ConnectionFactory(defaultConnectionProperties(url)),
            new MockWriterInitContext()
        );

        String result = writer.toString();
        assertTrue(result.contains("JetStreamSinkWriter"), "Should contain class name");
        assertTrue(result.contains("id='" + writer.getId() + "'"), "Should contain id");
        assertTrue(result.contains("subjects=" + subjects), "Should contain subjects");

        writer.close();
    }

    /**
     * Helper method to create a JetStreamSinkWriter with a specific connection and subjects.
     */
    private JetStreamSinkWriter<String> createWriter(String url, List<String> subjects) throws Exception {
        return new JetStreamSinkWriter<>(
                "test-sink",
                subjects,
                new Utf8StringSinkConverter(),
                new ConnectionFactory(defaultConnectionProperties(url)),
                new MockWriterInitContext()
        );
    }
}
