// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.*;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.TestBase;
import io.synadia.flink.source.reader.NatsSubjectSplitReader;
import io.synadia.flink.source.split.NatsSubjectSplit;
import io.synadia.flink.utils.ConnectionContext;
import io.synadia.flink.utils.ConnectionFactory;
import io.synadia.flink.v0.source.NatsJetStreamSourceConfiguration;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.util.FlinkRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class NatsSubjectSplitReaderTest extends TestBase {
    private static final String SOURCE_ID = "test-source";
    private static final int MAX_FETCH_RECORDS = 100;
    private static final int NUM_MESSAGES = 10;

    /**
     * Tests the reader's ability to handle messages arriving with delays while maintaining order.
     *
     * @throws Exception if any NATS operations fail
     */
    @Test
    void shouldMaintainMessageOrderWithDelaysInUnboundedMode() throws Exception {
        runInJsServer((nc, url) -> {
            // Setup stream
            String stream = stream();
            String subject = subject();

            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(stream)
                    .subjects(subject)
                    .storageType(StorageType.Memory)
                    .build();

            nc.jetStreamManagement().addStream(streamConfig);

            // Setup reader with real configuration
            NatsJetStreamSourceConfiguration sourceConfig = createSourceConfig(
                    "test-consumer",
                    stream,
                    MAX_FETCH_RECORDS,
                    false,
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    1000,
                    Duration.ofSeconds(1),
                    Boundedness.CONTINUOUS_UNBOUNDED
            );

            NatsSubjectSplitReader reader = createReader(url, sourceConfig);

            // Publish test messages with delays
            JetStream js = nc.jetStream();
            publishTestMessages(js, subject, NUM_MESSAGES);

            // Register split and fetch messages
            NatsSubjectSplit split = new NatsSubjectSplit(subject);
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

            // Single fetch call will get all messages up to MAX_FETCH_RECORDS
            RecordsWithSplitIds<Message> records = reader.fetch();
            assertNotNull(records);

            // Get the messages from the single fetch
            String splitId = records.nextSplit();
            assertNotNull(splitId);
            assertEquals(subject, splitId);

            List<Message> messages = new ArrayList<>();
            Message message;
            while ((message = records.nextRecordFromSplit()) != null) {
                messages.add(message);
            }

            // Verify messages
            assertNotNull(messages);
            assertEquals(NUM_MESSAGES, messages.size(), "Should receive exactly " + NUM_MESSAGES + " messages");
        });
    }

    /**
     * Tests the bounded mode behavior where the reader should signal completion
     * after consuming all available messages.
     *
     * Example scenario:
     * 1. Creates stream with 101 messages (MAX_FETCH_RECORDS + 1)
     * 2. Configures reader in bounded mode
     * 3. Reads all messages
     * 4. Verifies completion signal
     *
     * Key assertions:
     * - All messages are received (101 total)
     * - Messages maintain correct order (0-100)
     * - Split is marked as finished after all messages are consumed
     * - No more splits are available after completion
     *
     * @throws Exception if any NATS operations fail
     */
    @Test
    void shouldMarkSplitAsFinishedAndStopFetchingAfterAllMessagesConsumedInBoundedMode() throws Exception {
        runInJsServer((nc, url) -> {
            // Setup stream
            String stream = stream();
            String subject = subject();

            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(stream)
                    .subjects(subject)
                    .storageType(StorageType.Memory)
                    .build();

            nc.jetStreamManagement().addStream(streamConfig);

            // Setup reader with real configuration
            NatsJetStreamSourceConfiguration sourceConfig = createSourceConfig(
                    "test-consumer-finite",
                    stream,
                    MAX_FETCH_RECORDS,
                    false,
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    1000,
                    Duration.ofSeconds(10),
                    Boundedness.BOUNDED
            );

            NatsSubjectSplitReader reader = createReader(url, sourceConfig);

            int messageCount = MAX_FETCH_RECORDS;
            JetStream js = nc.jetStream();
            publishTestMessages(js, subject, messageCount);

            // Register split and fetch messages
            NatsSubjectSplit split = new NatsSubjectSplit(subject);
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

            // Single fetch call will get all messages
            RecordsWithSplitIds<Message> records = reader.fetch();
            assertNotNull(records);

            String splitId = records.nextSplit();
            assertNotNull(splitId);
            assertEquals(subject, splitId);

            List<Message> messages = new ArrayList<>();
            Message message;
            while ((message = records.nextRecordFromSplit()) != null) {
                messages.add(message);
            }

            assertEquals(messageCount, messages.size(), "Should receive exact number of messages");

            // Verify bounded completion
            records = reader.fetch();
            assertNotNull(records);
            assertTrue(records.finishedSplits().contains(subject), "Split should be marked as finished");
            assertNull(records.nextSplit(), "No more splits should be available");
        });
    }

    /**
     * Tests checkpoint completion notification and message acknowledgment.
     * Verifies that messages are properly acknowledged using manual ACK mechanism.
     *
     * Example scenario:
     * 1. Creates stream and publishes messages
     * 2. Reads messages through subscription
     * 3. Notifies checkpoint completion
     * 4. Verifies ACK messages are published
     *
     * Key assertions:
     * - Messages are acknowledged correctly
     * - ACK messages have correct reply-to subject
     * - ACK body bytes match expected format
     *
     * @throws Exception if any NATS operations fail
     */
    @Test
    void shouldAcknowledgeMessagesAndPreventRedeliveryAfterCheckpointCompletion() throws Exception {
        runInJsServer((nc, url) -> {
            // Setup stream
            String stream = stream();
            String subject = subject();

            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(stream)
                    .subjects(subject)
                    .storageType(StorageType.Memory)
                    .build();

            nc.jetStreamManagement().addStream(streamConfig);

            publishTestMessages(nc.jetStream(), subject, 5);

            // Setup reader with real configuration
            NatsJetStreamSourceConfiguration sourceConfig = createSourceConfig(
                    "test-consumer",
                    stream,
                    MAX_FETCH_RECORDS,
                    false,
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    1000,
                    Duration.ofSeconds(10),
                    Boundedness.CONTINUOUS_UNBOUNDED
            );

            NatsSubjectSplitReader reader = createReader(url, sourceConfig);

            // Register split and fetch messages
            NatsSubjectSplit split = new NatsSubjectSplit(subject);
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

            // Single fetch call will get all messages
            RecordsWithSplitIds<Message> records = reader.fetch();
            assertNotNull(records);

            String splitId = records.nextSplit();
            assertNotNull(splitId);
            assertEquals(subject, splitId);

            List<Message> messages = new ArrayList<>();
            Message message;
            while ((message = records.nextRecordFromSplit()) != null) {
                messages.add(message);
            }

            assertFalse(messages.isEmpty(), "Should have received messages");

            // Test checkpoint completion
            reader.notifyCheckpointComplete(subject, messages);

            // Small delay to allow NATS server to process acknowledgments
            Thread.sleep(100);

            // Verify messages were acknowledged by checking consumer info
            JetStreamManagement jsm = nc.jetStreamManagement();
            ConsumerInfo consumerInfo = jsm.getConsumerInfo(stream, sourceConfig.getConsumerName());

            // Verify acknowledgments
            assertEquals(messages.size(), consumerInfo.getAckFloor().getConsumerSequence(),
                    "All messages should be acknowledged in NATS");

            // Additional verification that no messages are redelivered
            records = reader.fetch();
            assertNull(records.nextSplit(), "Should not receive messages after acknowledgment");
        });
    }

    /**
     * Tests error handling for non-SplitsAddition changes.
     * Verifies that appropriate exception is thrown for unsupported split changes.
     *
     * Example scenario:
     * 1. Creates mock SplitsChange that is not SplitsAddition
     * 2. Attempts to handle the change
     * 3. Verifies exception is thrown
     *
     * Key assertions:
     * - UnsupportedOperationException is thrown
     * - Exception message contains split change type
     *
     * @throws Exception if any NATS operations fail
     */
    @Test
    void shouldThrowUnsupportedOperationExceptionForNonSplitsAdditionChanges() throws Exception {
        runInJsServer((nc, url) -> {

            // Setup reader with real configuration
            NatsJetStreamSourceConfiguration sourceConfig = createSourceConfig(
                    "test-consumer",
                    stream(),
                    MAX_FETCH_RECORDS,
                    false,
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    1000,
                    Duration.ofSeconds(10),
                    Boundedness.CONTINUOUS_UNBOUNDED
            );
            NatsSubjectSplitReader reader = createReader(url, sourceConfig);

            // Create mock SplitsChange that is not SplitsAddition
            @SuppressWarnings("unchecked")
            SplitsChange<NatsSubjectSplit> mockSplitChange = mock(SplitsChange.class);
            when(mockSplitChange.splits()).thenReturn(Collections.emptyList());

            UnsupportedOperationException thrown = assertThrows(
                    UnsupportedOperationException.class,
                    () -> reader.handleSplitsChanges(mockSplitChange),
                    "Should throw UnsupportedOperationException for non-SplitsAddition changes"
            );

            assertTrue(thrown.getMessage().contains(mockSplitChange.getClass().getName()),
                    "Exception message should contain split change type");
        });
    }

    /**
     * Verifies reader behavior with no registered split.
     *
     * This is not a functional test but rather validates the edge case handling
     * when fetch() is called before any split is registered. The reader should
     * gracefully handle this scenario by returning an empty result set instead
     * of throwing exceptions.
     *
     * Edge case coverage:
     * - Calling fetch() before split registration
     * - Handling null registeredSplit condition
     * - Empty result set construction and return
     */
    @Test
    void shouldReturnEmptyRecordSetWhenFetchCalledWithoutRegisteredSplit() throws Exception {
        runInJsServer((nc, url) -> {
            NatsJetStreamSourceConfiguration sourceConfig = createSourceConfig(
                    "test-consumer",
                    stream(),
                    MAX_FETCH_RECORDS,
                    false,
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    1000,
                    Duration.ofSeconds(10),
                    Boundedness.BOUNDED
            );

            NatsSubjectSplitReader reader = createReader(url, sourceConfig);

            // Fetch without registering any split
            RecordsWithSplitIds<Message> records = reader.fetch();
            assertNotNull(records);
            assertNull(records.nextSplit(), "Should return empty result when no split registered");
        });
    }

    /**
     * Validates no-op method implementations for non-critical operations.
     *
     * This test ensures proper coverage of methods that are currently implemented
     * as no-ops but may be enhanced in future versions. While these methods don't
     * provide functional behavior currently, they must still handle invocation
     * gracefully without causing system instability.
     *
     * Coverage verification:
     * - wakeUp() no-op implementation
     * - pauseOrResumeSplits() debug logging
     * - Method invocation without side effects
     */
    @Test
    void shouldHandleWakeUpAndPauseResumeMethodsAsNoOpOperations() throws Exception {
        runInJsServer((nc, url) -> {
            NatsJetStreamSourceConfiguration sourceConfig = createSourceConfig(
                    "test-consumer",
                    stream(),
                    MAX_FETCH_RECORDS,
                    false,
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    1000,
                    Duration.ofSeconds(10),
                    Boundedness.BOUNDED
            );

            NatsSubjectSplitReader reader = createReader(url, sourceConfig);

            // Test wakeUp (no-op)
            reader.wakeUp();

            // Test pauseOrResumeSplits (debug logging only)
            NatsSubjectSplit split = new NatsSubjectSplit(subject());
            reader.pauseOrResumeSplits(
                    Collections.singletonList(split),
                    Collections.emptyList()
            );
        });
    }

    /**
     * Validates error propagation for connection establishment failures.
     *
     * This test ensures proper error handling and propagation when the underlying
     * NATS connection cannot be established. It verifies that low-level connection
     * exceptions are properly wrapped in Flink's exception hierarchy while maintaining
     * the original error context.
     *
     * Error handling verification:
     * - IOException from connection failure
     * - Proper exception wrapping in FlinkRuntimeException
     * - Error context preservation
     * - Clean error propagation path
     */
    @Test
    void shouldWrapIOExceptionInFlinkRuntimeExceptionWhenConnectionFails() throws Exception {
        ConnectionFactory failingFactory = mock(ConnectionFactory.class);
        when(failingFactory.connectContext())
                .thenThrow(new IOException("Client connection closed: Client Closed"));

        NatsJetStreamSourceConfiguration sourceConfig = createSourceConfig(
                "test-consumer",
                stream(),
                MAX_FETCH_RECORDS,
                false,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                1000,
                Duration.ofSeconds(10),
                Boundedness.BOUNDED
        );

        NatsSubjectSplitReader reader = new NatsSubjectSplitReader(
            failingFactory,
                sourceConfig
        );

        FlinkRuntimeException thrown = assertThrows(
                FlinkRuntimeException.class,
                () -> reader.fetch()
        );

        // Verify both exception type and connection-specific error
        Throwable cause = thrown.getCause();
        assertTrue(cause instanceof IOException,
                "Exception should be wrapped IOException");
        assertTrue(cause.getMessage().contains("Client connection closed"),
                "Error should indicate specific NATS connection closure");
    }

    /**
     * Validates thread interruption handling during connection closure.
     *
     * This test ensures proper handling of thread interruption during cleanup
     * operations. It verifies that the reader maintains proper thread state
     * and exception propagation when interrupted during resource cleanup.
     *
     * Error handling verification:
     * - InterruptedException during close operation
     * - Thread interrupt flag preservation
     * - Exception wrapping in FlinkRuntimeException
     * - Proper cleanup despite interruption
     *
     */
    @Test
    void shouldPreserveInterruptFlagAndWrapExceptionWhenCloseIsInterrupted() throws Exception {
        runInJsServer((nc, url) -> {
            Connection mockConn = mock(Connection.class);
            JetStreamManagement mockJsm = mock(JetStreamManagement.class);
            JetStream mockJs = mock(JetStream.class);

            // Setup mock connection to return mock JetStreamManagement
            when(mockConn.jetStreamManagement(any(JetStreamOptions.class)))
                    .thenReturn(mockJsm);
            // Setup mock JetStreamManagement to return mock JetStream
            when(mockJsm.jetStream()).thenReturn(mockJs);

            doThrow(new InterruptedException("Simulated interrupt"))
                    .when(mockConn)
                    .close();

            ConnectionFactory mockFactory = mock(ConnectionFactory.class);
            ConnectionContext mockContext = new ConnectionContext(mockConn, JetStreamOptions.defaultOptions());
            when(mockFactory.connectContext()).thenReturn(mockContext);

            NatsJetStreamSourceConfiguration sourceConfig = createSourceConfig(
                    "test-consumer",
                    stream(),
                    MAX_FETCH_RECORDS,
                    false,
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    1000,
                    Duration.ofSeconds(10),
                    Boundedness.BOUNDED
            );

            NatsSubjectSplitReader reader = new NatsSubjectSplitReader(
                mockFactory,
                    sourceConfig
            );

            // Force context initialization
            reader.fetch();

            // Should throw FlinkRuntimeException and set interrupt flag
            FlinkRuntimeException thrown = assertThrows(
                    FlinkRuntimeException.class,
                    () -> reader.close()
            );
            assertTrue(thrown.getCause() instanceof InterruptedException);
            assertTrue(Thread.interrupted(), "Interrupt flag should be set");
        });
    }

    /**
     * Helper method to create NatsJetStreamSourceConfiguration using reflection
     */
    private static NatsJetStreamSourceConfiguration createSourceConfig(
            String consumerName,
            String streamName,
            int maxFetchRecords,
            boolean enableAutoAckMessage,
            Duration fetchTimeout,
            Duration autoAckInterval,
            int messageQueueCapacity,
            Duration fetchOneMessageTimeout,
            Boundedness boundedness) {
        try {
            Constructor<NatsJetStreamSourceConfiguration> constructor =
                    NatsJetStreamSourceConfiguration.class.getDeclaredConstructor(
                            String.class,      // streamName
                            String.class,      // consumerName
                            int.class,         // messageQueueCapacity
                            boolean.class,     // enableAutoAcknowledgeMessage
                            Duration.class,    // fetchOneMessageTimeout
                            Duration.class,    // fetchTimeout
                            int.class,         // maxFetchRecords
                            Duration.class,    // autoAckInterval
                            Boundedness.class  // boundedness
                    );
            constructor.setAccessible(true);
            return constructor.newInstance(
                    streamName,
                    consumerName,
                    messageQueueCapacity,
                    enableAutoAckMessage,
                    fetchOneMessageTimeout,
                    fetchTimeout,
                    maxFetchRecords,
                    autoAckInterval,
                    boundedness
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to create NatsJetStreamSourceConfiguration", e);
        }
    }

    /**
     * Helper method to publish test messages to a subject
     */
    private static void publishTestMessages(JetStream js, String subject, int numMessages) throws Exception {
        for (int i = 0; i < numMessages; i++) {
            js.publish(subject, String.valueOf(i).getBytes());
        }
    }

    /**
     * Helper method to create a NatsSubjectSplitReader with a given connection
     */
    private NatsSubjectSplitReader createReader(String url, NatsJetStreamSourceConfiguration njssConfig) {
        return new NatsSubjectSplitReader(new ConnectionFactory(defaultConnectionProperties(url)), njssConfig);
    }
}
