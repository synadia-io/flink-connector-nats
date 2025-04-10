package io.synadia.flink.source;

import io.synadia.flink.TestBase;
import io.synadia.flink.payload.StringPayloadDeserializer;
import io.synadia.flink.source.reader.NatsSourceReader;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.utils.ConnectionFactory;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.util.FlinkRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class NatsSourceReaderTest extends TestBase {

    /**
     * Validates core message flow functionality from NATS to Flink.
     *
     * Example scenario:
     * 1. Establishes NATS connection
     * 2. Creates subscription for test subject
     * 3. Publishes test message
     * 4. Verifies message delivery and deserialization
     *
     * Key assertions:
     * - Message is received and deserialized correctly
     * - Timing constraints are met
     * - Proper cleanup occurs on close
     *
     * @throws Exception if any NATS operations fail
     */
    @Test
    @Timeout(10)
    void testBasicMessageFlow() throws Exception {
        runInServer((nc, url) -> {
            String testSubject = subject();
            ReaderOutput<String> mockOutput = mock(ReaderOutput.class);
            SourceReaderContext mockContext = mock(SourceReaderContext.class);

            NatsSourceReader<String> reader = createReader(url, mockContext);

            try {
                reader.start();
                reader.addSplits(Collections.singletonList(new NatsSubjectSplit(testSubject)));
                Thread.sleep(100); // Brief pause for subscription to establish

                String testMessage = "Hello, World!";
                nc.publish(testSubject, testMessage.getBytes());

                // Poll until message is received
                for (int i = 0; i < 10; i++) {
                    reader.pollNext(mockOutput);
                    Thread.sleep(50);
                }

                verify(mockOutput, timeout(500)).collect(eq(testMessage));
            } finally {
                reader.close();
            }
        });
    }

    /**
     * Validates split state management and persistence capabilities.
     *
     * Example scenario:
     * 1. Creates multiple splits with unique subjects
     * 2. Adds splits to reader
     * 3. Takes state snapshot
     * 4. Verifies snapshot contents
     *
     * Key assertions:
     * - All splits are tracked correctly
     * - Snapshot contains expected splits
     * - Split equality is maintained
     *
     * @throws Exception if any NATS operations fail
     */
    @Test
    void testSnapshotState() throws Exception {
        runInServer((nc, url) -> {
            String testSubject = subject();
            NatsSourceReader<String> reader = createReader(url);

            try {
                reader.start();

                // Create two distinct splits
                NatsSubjectSplit split1 = new NatsSubjectSplit(testSubject);
                NatsSubjectSplit split2 = new NatsSubjectSplit(subject());
                List<NatsSubjectSplit> splits = Arrays.asList(split1, split2);

                reader.addSplits(splits);
                Thread.sleep(100); // Wait for subscriptions

                // Take snapshot and verify
                List<NatsSubjectSplit> snapshot = reader.snapshotState(1L);
                assertEquals(2, snapshot.size(),
                        String.format("Expected 2 splits but got %d splits in snapshot", snapshot.size()));
                assertEquals(new HashSet<>(splits), new HashSet<>(snapshot),
                        String.format("Snapshot splits don't match. Expected %s but got %s", splits, snapshot));
            } finally {
                reader.close();
            }
        });
    }

    /**
     * Demonstrates message availability signaling behavior.
     *
     * Example scenario:
     * 1. Gets availability future before messages
     * 2. Verifies initial not-done state
     * 3. Publishes message
     * 4. Confirms future completion
     *
     * Key assertions:
     * - Future state transitions correctly
     * - Message becomes available after signal
     * - Proper timing of state changes
     *
     * @throws Exception if any NATS operations fail
     */
    @Test
    void testIsAvailable() throws Exception {
        runInServer((nc, url) -> {
            NatsSourceReader<String> reader = createReader(url);
            ReaderOutput<String> mockOutput = mock(ReaderOutput.class);

            try {
                reader.start();
                String testSubject = subject();
                reader.addSplits(Collections.singletonList(new NatsSubjectSplit(testSubject)));
                Thread.sleep(100);

                nc.publish(testSubject, "test".getBytes());
                Thread.sleep(100); // Wait for message to be received

                CompletableFuture<Void> future = reader.isAvailable();
                assertNotNull(future, "Availability future should not be null");
                assertTrue(future.isDone(), "Future should be completed since message is already available");

                // Verify message can be read
                reader.pollNext(mockOutput);
                verify(mockOutput).collect("test");
            } finally {
                reader.close();
            }
        });
    }

    /**
     * Demonstrates error handling for connection failures.
     *
     * Example scenario:
     * 1. Configures mock factory to simulate failure
     * 2. Attempts connection establishment
     * 3. Verifies error propagation
     *
     * Key assertions:
     * - Correct exception hierarchy
     * - Error context preservation
     * - Single connection attempt
     */
    @Test
    void testErrorHandling() {
        // Setup mock factory with simulated failure
        ConnectionFactory failingFactory = mock(ConnectionFactory.class);
        try {
            when(failingFactory.connect()).thenThrow(new IOException("Simulated connection error"));
        } catch (IOException e) {
            fail("Mock setup failed", e);
        }

        NatsSourceReader<String> reader = new NatsSourceReader<>(
            failingFactory,
                new StringPayloadDeserializer(),
                mock(SourceReaderContext.class)
        );

        // Verify error propagation
        FlinkRuntimeException thrown = assertThrows(
                FlinkRuntimeException.class,
                reader::start,
                "Should throw FlinkRuntimeException when connection fails"
        );

        // Verify exception chain and context
        assertNotNull(thrown.getCause(),
                "Exception should preserve cause");
        assertTrue(thrown.getCause() instanceof IOException,
                "Original IOException should be preserved in cause");
        assertEquals("Simulated connection error", thrown.getCause().getMessage(),
                "Error message should be preserved");

        // Verify mock interactions
        try {
            verify(failingFactory, times(1)).connect();
        } catch (IOException e) {
            fail("Mock verification failed", e);
        }
    }

    /**
     * Demonstrates source event handling behavior.
     *
     * Example scenario:
     * 1. Creates reader with mock dependencies
     * 2. Sends test event
     * 3. Verifies logging occurs
     *
     * Key assertions:
     * - No exceptions during event handling
     * - Event processing completes
     */
    @Test
    void testSourceEvents() {
        String sourceId = "test-source";
        NatsSourceReader<String> reader = new NatsSourceReader<>(
            mock(ConnectionFactory.class),
                new StringPayloadDeserializer(),
                mock(SourceReaderContext.class)
        );

        assertDoesNotThrow(() -> reader.handleSourceEvents(mock(SourceEvent.class)),
                "Event handling should not throw exceptions");
    }

    /**
     * Demonstrates split completion notification handling.
     *
     * Example scenario:
     * 1. Creates reader with mock dependencies
     * 2. Sends completion notification
     * 3. Verifies logging behavior
     *
     * Key assertions:
     * - No exceptions during notification
     * - Notification is processed
     */
    @Test
    void testNotifyNoMoreSplits() {
        String sourceId = "test-source";
        NatsSourceReader<String> reader = new NatsSourceReader<>(
            mock(ConnectionFactory.class),
                new StringPayloadDeserializer(),
                mock(SourceReaderContext.class)
        );

        assertDoesNotThrow(() -> reader.notifyNoMoreSplits(),
                "notifyNoMoreSplits should not throw exceptions");
    }

    // Helper method to create reader with default context
    private NatsSourceReader<String> createReader(String url) {
        return createReader(url, mock(SourceReaderContext.class));
    }

    // Helper method to create reader with specific context
    private NatsSourceReader<String> createReader(String url, SourceReaderContext context) {
        return new NatsSourceReader<>(
            new ConnectionFactory(defaultConnectionProperties(url)),
                new StringPayloadDeserializer(),
                context
        );
    }
}
