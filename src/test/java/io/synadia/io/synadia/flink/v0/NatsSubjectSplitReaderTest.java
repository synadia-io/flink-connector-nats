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
import io.synadia.io.synadia.flink.TestBase;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.jupiter.api.*;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class NatsSubjectSplitReaderTest extends TestBase {
    private static final String STREAM_NAME = "test-stream";
    private static final String SOURCE_ID = "test-source";
    private static final String SUBJECT_PREFIX = "test-subject";
    private static final int MAX_FETCH_RECORDS = 100;
    private static final int NUM_MESSAGES_PER_SUBJECT = 10;

    @Test
    void testBasicSplitAssignmentAndFetch() throws Exception {
        runInServer(true, (nc, url) -> {
            // Setup connection and stream
            Connection natsConnection = nc;
            JetStreamManagement jsm = nc.jetStreamManagement();
            
            // Create stream
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(STREAM_NAME)
                .subjects(SUBJECT_PREFIX + "-0")
                .storageType(StorageType.Memory)
                .build();

            try {
                jsm.addStream(streamConfig);
            } catch (JetStreamApiException e) {
                if (e.getErrorCode() != 404) {
                    throw e;
                }
            }

            // Setup reader
            Callback<String, ConnectionContext> callback = mock(Callback.class);
            JetStreamOptions options = JetStreamOptions.builder().build();
            ConnectionContext context = new ConnectionContext(natsConnection, options);
            lenient().when(callback.newConnection(any())).thenReturn(context);

            Map<String, ConnectionContext> connections = new HashMap<>();
            NatsJetStreamSourceConfiguration sourceConfig = createSourceConfig(
                "test-consumer",
                MAX_FETCH_RECORDS,
                false,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                1000,
                Duration.ofSeconds(10),
                Boundedness.CONTINUOUS_UNBOUNDED
            );

            NatsSubjectSplitReader reader = new NatsSubjectSplitReader(
                SOURCE_ID,
                connections,
                sourceConfig,
                callback
            );

            // Publish test messages
            String subject = SUBJECT_PREFIX + "-0";
            publishTestMessages(natsConnection, subject, NUM_MESSAGES_PER_SUBJECT);

            // Create split for the subject
            NatsSubjectSplit split = new NatsSubjectSplit(subject);

            // Assign split directly
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

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
        });
    }

    @Test
    void testConnectionReconnectionFailure() throws Exception {
        runInServer(true, (nc, url) -> {
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
            Map<String, ConnectionContext> connections = new HashMap<>();
            NatsJetStreamSourceConfiguration sourceConfig = createSourceConfig(
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
            Callback<String, ConnectionContext> callback = mock(Callback.class);
            when(callback.newConnection(any())).thenReturn(context);

            NatsSubjectSplitReader reader = new NatsSubjectSplitReader(
                    SOURCE_ID,
                    connections,
                    sourceConfig,
                    callback
            );

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
        });
    }

    private static void publishTestMessages(Connection nc, String subject, int numMessages) throws Exception {
        JetStream js = nc.jetStream();
        for (int i = 0; i < numMessages; i++) {
            js.publish(subject, String.valueOf(i).getBytes());
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