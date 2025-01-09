package io.synadia.io.synadia.flink.v0;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.synadia.flink.v0.source.NatsJetStreamSourceConfiguration;
import io.synadia.flink.v0.source.reader.Callback;
import io.synadia.flink.v0.source.reader.NatsSubjectSplitReader;
import io.synadia.flink.v0.source.split.NatsSubjectSplit;
import io.synadia.flink.v0.utils.ConnectionContext;
import io.synadia.io.synadia.flink.v0.testutils.NatsSourceTestEnv;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class NatsSubjectSplitReaderTest extends NatsSourceTestEnv {
    private static final String STREAM_NAME = "test-stream";
    private static final String SOURCE_ID = "test-source";
    private static final String SUBJECT_PREFIX = "test-subject";
    private static final int MAX_FETCH_RECORDS = 100;

    private Callback<String, ConnectionContext> callback;

    private Map<String, ConnectionContext> connections;
    private NatsJetStreamSourceConfiguration sourceConfig;
    private NatsSubjectSplitReader reader;

    @BeforeAll
    static void createStream() throws Exception {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
            .name(STREAM_NAME)
            .subjects(SUBJECT_PREFIX + "-0")
            .storageType(StorageType.Memory)
            .build();

        try {
            // Check if stream exists
            StreamInfo streamInfo = jsm.getStreamInfo(STREAM_NAME);
            
            // If exists but config differs, update it
            if (!streamInfo.getConfiguration().equals(streamConfig)) {
                jsm.updateStream(streamConfig);
            }
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() == 404) { // Stream not found
                jsm.addStream(streamConfig);
            } else {
                throw e;
            }
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        // Create mock callback
        callback = mock(Callback.class);

        try {
            // Purge the stream but keep the stream configuration
            jsm.purgeStream(STREAM_NAME);
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() != 404) { // Ignore if stream doesn't exist
                throw e;
            }
        }
    }

    @AfterAll
    static void cleanUp() throws Exception {
        try {
            jsm.deleteStream(STREAM_NAME);
        } catch (JetStreamApiException e) {
            if (e.getErrorCode() != 404) { // Ignore if stream doesn't exist
                throw e;
            }
        }
    }

    /**
     * Sets up a test reader with specified parameters.
     * 
     * Setup process:
     * 1. Configures mock behavior for connection callbacks
     * 2. Creates source configuration with provided parameters
     * 3. Initializes reader with test configuration
     * 
     * @param maxFetchRecords Maximum records to fetch per call
     * @param boundedness Stream boundedness setting
     * @param consumerName Name for the JetStream consumer
     * @throws Exception if reader setup fails
     */
    private void setupReader(int maxFetchRecords, Boundedness boundedness, String consumerName) throws Exception {
        // Setup mock
        JetStreamOptions options = JetStreamOptions.builder().build();
        ConnectionContext context = new ConnectionContext(natsConnection, options);
        lenient().when(callback.newConnection(any())).thenReturn(context);

        connections = new HashMap<>();
        sourceConfig = createSourceConfig(
            consumerName,
            maxFetchRecords,
            false,
            Duration.ofSeconds(1),
            Duration.ofSeconds(1),
            1000,
            Duration.ofSeconds(10),
            boundedness
        );

        reader = new NatsSubjectSplitReader(
            SOURCE_ID,
            connections,
            sourceConfig,
            callback
        );
    }

    /**
     * Tests basic split assignment and message fetching functionality.
     * 
     * Test scenario:
     * 1. Sets up a reader with specified fetch limit and boundedness
     * 2. Creates a stream with a single subject
     * 3. Publishes test messages to the subject
     * 4. Assigns split to the reader
     * 5. Fetches and verifies messages
     * 
     * Expected behavior:
     * - Reader should successfully connect to the stream
     * - All published messages should be fetched
     * - Message contents should match what was published
     * 
     * @throws Exception if any NATS operations fail
     */
    @Test
    void testBasicSplitAssignmentAndFetch() throws Exception {
        // Setup reader and test environment
        setupReader(MAX_FETCH_RECORDS, Boundedness.CONTINUOUS_UNBOUNDED, "test-consumer");

        // Create stream with subject
        String subject = SUBJECT_PREFIX + "-0";
        StreamConfiguration streamConfig = StreamConfiguration.builder()
            .name(STREAM_NAME)
            .subjects(subject)
            .storageType(StorageType.Memory)
            .build();

        jsm.addStream(streamConfig);

        // Publish test messages
        publishTestMessages(subject, NUM_MESSAGES_PER_SUBJECT);

        try {
            // Create split for the subject
            NatsSubjectSplit split = new NatsSubjectSplit(subject);

            // Assign split directly
            reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(split))
            );

            // Wait a bit for messages to be available
            Thread.sleep(1000);

            // Fetch and verify records
            RecordsWithSplitIds<Message> records = reader.fetch();
            assertNotNull(records);

            List<Message> messages = new ArrayList<>();

            while ((records.nextSplit()) != null) {
                Message message;
                while ((message = records.nextRecordFromSplit()) != null) {
                    messages.add(message);
                }
            }

            // Verify messages
            assertNotNull(messages);
            assertEquals(NUM_MESSAGES_PER_SUBJECT, messages.size());

            for (int i = 0; i < messages.size(); i++) {
                Message msg = messages.get(i);
                assertNotNull(msg);
                assertEquals(String.valueOf(i), new String(msg.getData()));
            }

        } catch (Exception e) {
            throw e;
        }
    }


    /**
     * Tests the reader's auto-reconnection behavior when connection failure occurs.
     *
     * Test scenario:
     * 1. Sets up a reader with mocked NATS components
     * 2. Adds a split and verifies initial connection creation
     * 3. Performs first fetch with working connection
     * 4. Simulates connection failure by setting connection status to CLOSED
     * 5. Performs second fetch which should trigger reconnection
     *
     * Expected behavior:
     * - Initial connection and fetch should succeed
     * - When connection is closed, fetch() should:
     *   a. Detect the closed connection
     *   b. Create a new connection via callback
     *   c. Create new subscription with new connection
     *   d. Successfully continue fetching messages
     * - Verifies that auto-reconnection is properly implemented
     *
     * @throws Exception if any operations fail
     */
    @Test
    void testConnectionReconnectionFailure() throws Exception {

        // Mock all the components
        Connection mockConnection = mock(Connection.class);
        JetStream mockJs = mock(JetStream.class);
        JetStreamManagement mockJsm = mock(JetStreamManagement.class);
        JetStreamSubscription mockSubscription = mock(JetStreamSubscription.class);

        // Setup initial connection state
        when(mockConnection.getStatus()).thenReturn(Connection.Status.CONNECTED);
        when(mockConnection.jetStream()).thenReturn(mockJs);
        when(mockConnection.jetStreamManagement(any(JetStreamOptions.class))).thenReturn(mockJsm);
        when(mockJsm.jetStream()).thenReturn(mockJs);
        when(mockJs.subscribe(anyString(), any(PullSubscribeOptions.class))).thenReturn(mockSubscription);

        // Create real context with mocked components
        ConnectionContext context = new ConnectionContext(mockConnection, JetStreamOptions.builder().build());

        // Setup reader
        connections = new HashMap<>();
        sourceConfig = createSourceConfig(
                "test-consumer-reconnecting",
                1,
                false,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                1000,
                Duration.ofSeconds(10),
                Boundedness.CONTINUOUS_UNBOUNDED
        );

        // Setup callback to return our context
        when(callback.newConnection(any())).thenReturn(context);

        reader = new NatsSubjectSplitReader(
                SOURCE_ID,
                connections,
                sourceConfig,
                callback
        );

        try{
            // Add a split
            String subject = "test-subject";
            NatsSubjectSplit split = new NatsSubjectSplit(subject);
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

            // First fetch - should succeed
            when(mockSubscription.fetch(anyInt(), any())).thenReturn(Collections.singletonList(mock(Message.class)));
            RecordsWithSplitIds<Message> records = reader.fetch();
            assertNotNull(records);

            // Simulate connection failure
            when(mockConnection.getStatus()).thenReturn(Connection.Status.CLOSED);

            // Setup new connection that will be returned after failure
            Connection mockConnection2 = mock(Connection.class);
            JetStream mockJs2 = mock(JetStream.class);
            JetStreamManagement mockJsm2 = mock(JetStreamManagement.class);
            JetStreamSubscription mockSubscription2 = mock(JetStreamSubscription.class);

            when(mockConnection2.getStatus()).thenReturn(Connection.Status.CONNECTED);
            when(mockConnection2.jetStream()).thenReturn(mockJs2);
            when(mockConnection2.jetStreamManagement(any(JetStreamOptions.class))).thenReturn(mockJsm2);
            when(mockJsm2.jetStream()).thenReturn(mockJs2);
            when(mockJs2.subscribe(anyString(), any(PullSubscribeOptions.class))).thenReturn(mockSubscription2);

            // Create second context with mocked components
            ConnectionContext context2 = new ConnectionContext(mockConnection2, JetStreamOptions.builder().build());
            when(callback.newConnection(any())).thenReturn(context2);

            // Second fetch - should get new connection and succeed
            when(mockSubscription2.fetch(anyInt(), any())).thenReturn(Collections.singletonList(mock(Message.class)));
            records = reader.fetch();
            assertNotNull(records);

            // Verify that we got a new connection three times:
            // 1. During handleSplitsChanges
            // 2. During first fetch
            // 3. During second fetch after connection failure
            verify(callback, times(3)).newConnection(any());

            // Verify the subscription was created:
            // - Twice with first connection (in handleSplitsChanges and first fetch)
            // - Once with second connection (in second fetch)
            verify(mockJs, times(2)).subscribe(eq(subject), any(PullSubscribeOptions.class));
            verify(mockJs2, times(1)).subscribe(eq(subject), any(PullSubscribeOptions.class));
        }
        catch(Exception e){
            throw e;
        }
    }



    /**
     * Helper method to create NatsJetStreamSourceConfiguration using reflection
     */
    private static NatsJetStreamSourceConfiguration createSourceConfig(
            String consumerName,
            int maxFetchRecords,
            boolean enableAutoAckMessage,
            Duration fetchTimeout,
            Duration autoAckInterval,
            int maxPendingMessages,
            Duration maxWaitingTime,
            Boundedness boundedness) {
        try {
            Constructor<NatsJetStreamSourceConfiguration> constructor = 
                NatsJetStreamSourceConfiguration.class.getDeclaredConstructor(
                    String.class,
                    int.class,
                    boolean.class,
                    Duration.class,
                    Duration.class,
                    int.class,
                    Duration.class,
                    Boundedness.class
                );
            constructor.setAccessible(true);
            return constructor.newInstance(
                consumerName,
                maxFetchRecords,
                enableAutoAckMessage,
                fetchTimeout,
                autoAckInterval,
                maxPendingMessages,
                maxWaitingTime,
                boundedness
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to create NatsJetStreamSourceConfiguration", e);
        }
    }
}