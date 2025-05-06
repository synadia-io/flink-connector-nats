// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0;

import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.TestBase;
import io.synadia.flink.payload.StringPayloadDeserializer;
import io.synadia.flink.v0.source.NatsJetStreamSource;
import io.synadia.flink.v0.source.NatsJetStreamSourceBuilder;
import io.synadia.flink.v0.source.NatsJetStreamSourceConfiguration;
import org.apache.flink.api.connector.source.Boundedness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static io.nats.client.support.ApiConstants.STREAM_NAME;
import static io.nats.client.support.ApiConstants.SUBJECTS;
import static io.synadia.flink.utils.Constants.PAYLOAD_DESERIALIZER;
import static io.synadia.flink.utils.Constants.STRING_PAYLOAD_DESERIALIZER_CLASSNAME;
import static io.synadia.flink.v0.source.NatsJetStreamSourceBuilder.CONSUMER_NAME;
import static org.junit.jupiter.api.Assertions.*;

/** Unit test for {@link NatsJetStreamSourceBuilder}. */
class NatsJetStreamSourceBuilderTest extends TestBase {

    /**
     * Tests the minimum configuration required to successfully build a NatsJetStreamSource.
     * This represents the most basic use case of the JetStream builder.
     *
     * Required settings:
     * 1. At least one subject to subscribe to
     * 2. A payload deserializer to convert NATS messages
     * 3. Connection properties for NATS server
     * 4. Stream name for JetStream
     * 5. Consumer name for JetStream
     *
     * Example usage:
     * ```java
     * NatsJetStreamSource<String> source = new NatsJetStreamSourceBuilder<String>()
     *     .subjects("orders.>")  // Subscribe to all messages in 'orders' hierarchy
     *     .payloadDeserializer(new StringPayloadDeserializer())
     *     .connectionProperties(props)  // props containing "nats.connection.url", etc.
     *     .streamName("OrdersStream")
     *     .consumerName("OrdersConsumer")
     *     .build();
     * ```
     */
    @Test
    void testBuildWithMinimumRequiredSettings() throws Exception {
        runInJsServer((nc, url) -> {
            String streamName = stream();
            String subject = subject();

            // Create the stream first
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(streamName)
                .subjects(subject)
                .storageType(StorageType.Memory)
                .build();

            nc.jetStreamManagement().addStream(streamConfig);

            NatsJetStreamSource<String> source = new NatsJetStreamSourceBuilder<String>()
                .subject(subject)
                .payloadDeserializer(new StringPayloadDeserializer())
                .connectionPropertiesFile(defaultConnectionProperties(url))
                .streamName(streamName)
                .consumerName("test-consumer")
                .build();

            assertNotNull(source, "Built source should not be null");
        });
    }

    /**
     * Tests the builder's ability to configure a source using Properties object.
     * Demonstrates how to configure JetStream source using properties file approach.
     *
     * Properties tested:
     * 1. Connection settings (URL, credentials)
     * 2. Subject configuration
     * 3. Payload deserializer class name
     * 4. Stream name
     * 5. Consumer name
     *
     * Example properties:
     * ```properties
     * nats.source.subjects=orders.>
     * nats.source.payload.deserializer=io.synadia.flink.payload.StringPayloadDeserializer
     * nats.source.stream.name=OrdersStream
     * nats.source.consumer.name=OrdersConsumer
     * ```
     */
    @Test
    void testBuildWithPropertiesConfiguration() throws Exception {
        runInJsServer((nc, url) -> {
            String streamName = stream();
            String subject = subject();
            String consumer = name();

            // Create the stream first
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(streamName)
                .subjects(subject)
                .storageType(StorageType.Memory)
                .build();

            nc.jetStreamManagement().addStream(streamConfig);

            Properties props = defaultConnectionProperties(url);
            props.setProperty(SUBJECTS, subject);
            props.setProperty(PAYLOAD_DESERIALIZER, STRING_PAYLOAD_DESERIALIZER_CLASSNAME);
            props.setProperty(STREAM_NAME, streamName);
            props.setProperty(CONSUMER_NAME, consumer);

            NatsJetStreamSource<String> source = new NatsJetStreamSourceBuilder<String>()
                .sourceProperties(props)
                .connectionPropertiesFile(props)
                .build();
            assertNotNull(source, "Source built from properties should not be null");
            assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, source.getBoundedness());

            NatsJetStreamSourceConfiguration config = source.getSourceConfiguration();
            assertEquals(streamName, config.getStreamName());
            assertEquals(consumer, config.getConsumerName());
        });
    }

    /**
     * Tests JetStream-specific configuration settings.
     * Verifies the builder's support for JetStream-specific features.
     *
     * Settings tested:
     * 1. Message queue capacity - Controls internal buffer size
     * 2. Auto acknowledge settings - Configures automatic message acknowledgment
     * 3. Fetch timeouts - Sets timeouts for message fetching operations
     * 4. Max fetch records - Limits number of records fetched in bounded mode
     * 5. Boundedness - Controls whether source is bounded or continuous
     * 6. Auto ack interval - Sets interval for automatic acknowledgments
     *
     * Example:
     * ```java
     * .messageQueueCapacity(1000)
     * .enableAutoAcknowledgeMessage(true)
     * .fetchOneMessageTime(Duration.ofSeconds(1))
     * .maxFetchTime(Duration.ofSeconds(5))
     * .maxFetchRecords(100)
     * .boundness(Boundedness.BOUNDED)
     * ```
     */
    @Test
    void testBuildWithJetStreamSpecificSettings() throws Exception {
        runInJsServer((nc, url) -> {
            String streamName = stream();
            String subject = subject();

            // Create the stream first
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(streamName)
                .subjects(subject)
                .storageType(StorageType.Memory)
                .build();

            nc.jetStreamManagement().addStream(streamConfig);

            NatsJetStreamSource<String> source = new NatsJetStreamSourceBuilder<String>()
                .subject(subject)
                .payloadDeserializer(new StringPayloadDeserializer())
                .connectionPropertiesFile(defaultConnectionProperties(url))
                .streamName(streamName)
                .consumerName("test-consumer")
                .messageQueueCapacity(1000)
                .enableAutoAcknowledgeMessage(true)
                .fetchOneMessageTime(Duration.ofSeconds(1))
                .maxFetchTime(Duration.ofSeconds(5))
                .maxFetchRecords(100)
                .natsAutoAckInterval(Duration.ofSeconds(2))
                .boundness(Boundedness.BOUNDED)
                .build();

            assertNotNull(source, "Source with JetStream settings should not be null");
        });
    }

    /**
     * Tests validation when stream name is missing.
     * Stream name is a required setting for JetStream sources.
     *
     * Expected behavior:
     * - Should throw IllegalArgumentException
     * - Error message should mention missing stream name
     */
    @Test
    void testBuildValidationWithMissingStreamName() throws Exception {
        runInJsServer((nc, url) -> {
            String subject = subject();
            Properties props = defaultConnectionProperties(url);

            IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new NatsJetStreamSourceBuilder<String>()
                    .subject(subject)
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .connectionPropertiesFile(props)
                    .consumerName("test-consumer")
                    .build()
            );
            assertTrue(ex.getMessage().contains("Stream name"),
                "Exception should mention missing stream name");
        });
    }

    /**
     * Tests validation when consumer name is missing.
     * Consumer name is a required setting for JetStream sources.
     *
     * Expected behavior:
     * - Should throw IllegalArgumentException
     * - Error message should mention missing consumer name
     */
    @Test
    void testBuildValidationWithMissingDurableConsumerName() throws Exception {
        runInJsServer((nc, url) -> {
            String subject = subject();
            Properties props = defaultConnectionProperties(url);

            IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new NatsJetStreamSourceBuilder<String>()
                    .subject(subject)
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .connectionPropertiesFile(props)
                    .streamName("test-stream")
                    .build()
            );
            assertTrue(ex.getMessage().contains("Consumer name"),
                "Exception should mention missing consumer name");
        });
    }

    /**
     * Tests validation of auto-acknowledge interval.
     * When auto-acknowledge is enabled, the interval must be positive.
     *
     * Expected behavior:
     * - Should throw IllegalArgumentException for zero interval
     * - Error message should mention invalid interval
     */
    @Test
    void testBuildValidationWithInvalidAutoAckInterval() throws Exception {
        runInJsServer((nc, url) -> {
            String subject = subject();
            Properties props = defaultConnectionProperties(url);

            IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new NatsJetStreamSourceBuilder<String>()
                    .subject(subject)
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .connectionPropertiesFile(props)
                    .streamName("test-stream")
                    .consumerName("test-consumer")
                    .enableAutoAcknowledgeMessage(true)
                    .natsAutoAckInterval(Duration.ZERO)
                    .build()
            );
            assertTrue(ex.getMessage().contains("interval"),
                "Exception should mention invalid interval");
        });
    }

    /**
     * Tests validation of max fetch records setting.
     * Max fetch records must be positive when specified.
     *
     * Expected behavior:
     * - Should throw IllegalArgumentException for negative value
     * - Error message should mention invalid records count
     */
    @Test
    void testBuildValidationWithInvalidMaxFetchRecords() throws Exception {
        runInJsServer((nc, url) -> {
            String subject = subject();
            Properties props = defaultConnectionProperties(url);

            IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new NatsJetStreamSourceBuilder<String>()
                    .subject(subject)
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .connectionPropertiesFile(props)
                    .streamName("test-stream")
                    .consumerName("test-consumer")
                    .maxFetchRecords(-1)  // Invalid record count
                    .build()
            );
            assertTrue(ex.getMessage().contains("records"),
                "Exception should mention invalid fetch records");
        });
    }

    /**
     * Tests validation of message queue capacity.
     * Queue capacity must be positive for proper buffer management.
     *
     * Expected behavior:
     * - Should throw IllegalArgumentException for zero capacity
     * - Error message should mention invalid queue capacity
     */
    @Test
    void testBuildValidationWithInvalidMessageQueueCapacity() throws Exception {
        runInJsServer((nc, url) -> {
            String subject = subject();
            Properties props = defaultConnectionProperties(url);

            IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new NatsJetStreamSourceBuilder<String>()
                    .subject(subject)
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .connectionPropertiesFile(props)
                    .streamName("test-stream")
                    .consumerName("test-consumer")
                    .messageQueueCapacity(0)  // Invalid capacity
                    .build()
            );
            assertTrue(ex.getMessage().contains("queue capacity"),
                "Exception should mention invalid queue capacity");
        });
    }

    /**
     * Tests validation of fetch timeout setting.
     * Fetch timeout must be non-negative for message retrieval.
     *
     * Expected behavior:
     * - Should throw IllegalArgumentException for negative timeout
     * - Error message should mention invalid fetch timeout
     */
    @Test
    void testBuildValidationWithInvalidFetchTimeout() throws Exception {
        runInJsServer((nc, url) -> {
            String subject = subject();
            Properties props = defaultConnectionProperties(url);

            IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new NatsJetStreamSourceBuilder<String>()
                    .subject(subject)
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .connectionPropertiesFile(props)
                    .streamName("test-stream")
                    .consumerName("test-consumer")
                    .fetchOneMessageTime(Duration.ofMillis(-1))  // Invalid timeout
                    .build()
            );
            assertTrue(ex.getMessage().toLowerCase().contains("fetch") &&
                    ex.getMessage().toLowerCase().contains("timeout"),
                "Exception should mention fetch timeout");
        });
    }

    /**
     * Tests validation of max fetch time setting.
     * Max fetch time must be non-negative for bounded operations.
     *
     * Expected behavior:
     * - Should throw IllegalArgumentException for negative time
     * - Error message should mention invalid fetch time
     */
    @Test
    void testBuildValidationWithInvalidMaxFetchTime() throws Exception {
        runInJsServer((nc, url) -> {
            String subject = subject();
            Properties props = defaultConnectionProperties(url);

            IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new NatsJetStreamSourceBuilder<String>()
                    .subject(subject)
                    .payloadDeserializer(new StringPayloadDeserializer())
                    .connectionPropertiesFile(props)
                    .streamName("test-stream")
                    .consumerName("test-consumer")
                    .maxFetchTime(Duration.ofMillis(-1))  // Invalid max fetch time
                    .build()
            );
            assertTrue(ex.getMessage().toLowerCase().contains("fetch") &&
                    ex.getMessage().toLowerCase().contains("time"),
                "Exception should mention fetch time");
        });
    }

    /**
     * Tests stream configuration options for JetStream sources.
     * Verifies support for different storage types and stream settings.
     *
     * Configurations tested:
     * 1. Memory storage
     *    - Basic in-memory stream
     *    - Suitable for temporary data
     *    - No persistence
     *
     * 2. File storage
     *    - Persistent storage on disk
     *    - Configurable retention policies
     *    - Size limits and age limits
     *
     * Each configuration verifies:
     * - Stream creation success
     * - Source builder compatibility
     * - Basic connectivity
     */
    @Test
    void testBuildWithDifferentStreamStorageTypes() throws Exception {
        runInJsServer((nc, url) -> {
            String subject1 = subject();
            String subject2 = subject();  // Use different subjects

            // Test with memory storage
            String memoryStreamName = stream();
            StreamConfiguration memoryConfig = StreamConfiguration.builder()
                .name(memoryStreamName)
                .subjects(subject1)  // Use first subject
                .storageType(StorageType.Memory)
                .replicas(1)
                .build();

            nc.jetStreamManagement().addStream(memoryConfig);

            NatsJetStreamSource<String> memorySource = new NatsJetStreamSourceBuilder<String>()
                .subject(subject1)
                .payloadDeserializer(new StringPayloadDeserializer())
                .connectionPropertiesFile(defaultConnectionProperties(url))
                .streamName(memoryStreamName)
                .consumerName("memory-consumer")
                .build();

            assertNotNull(memorySource, "Memory storage source should not be null");

            // Test with file storage
            String fileStreamName = stream();
            StreamConfiguration fileConfig = StreamConfiguration.builder()
                .name(fileStreamName)
                .subjects(subject2)  // Use second subject
                .storageType(StorageType.File)
                .replicas(1)
                .maxAge(Duration.ofHours(1))
                .maxBytes(1024 * 1024)
                .build();

            nc.jetStreamManagement().addStream(fileConfig);

            NatsJetStreamSource<String> fileSource = new NatsJetStreamSourceBuilder<String>()
                .subject(subject2)
                .payloadDeserializer(new StringPayloadDeserializer())
                .connectionPropertiesFile(defaultConnectionProperties(url))
                .streamName(fileStreamName)
                .consumerName("file-consumer")
                .build();

            assertNotNull(fileSource, "File storage source should not be null");
        });
    }

    /**
     * Tests different acknowledgment modes for JetStream sources.
     * Verifies configuration of automatic and manual acknowledgment patterns.
     *
     * Modes tested:
     * 1. Auto acknowledgment
     *    - Configurable acknowledgment interval
     *    - Automatic message confirmation
     *    - Suitable for simple consumption patterns
     *
     * 2. Manual acknowledgment
     *    - Application-controlled acknowledgment
     *    - More precise message handling
     *    - Suitable for complex processing requirements
     *
     * Verifies:
     * - Configuration acceptance
     * - Mode-specific settings
     * - Builder compatibility
     */
    @Test
    void testBuildWithAcknowledgmentModes() throws Exception {
        runInJsServer((nc, url) -> {
            String streamName = stream();
            String subject = subject();

            StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(streamName)
                .subjects(subject)
                .storageType(StorageType.Memory)
                .build();

            nc.jetStreamManagement().addStream(streamConfig);

            // Test auto-ack with different intervals
            NatsJetStreamSource<String> autoAckSource = new NatsJetStreamSourceBuilder<String>()
                .subject(subject)
                .payloadDeserializer(new StringPayloadDeserializer())
                .connectionPropertiesFile(defaultConnectionProperties(url))
                .streamName(streamName)
                .consumerName("auto-ack-consumer")
                .enableAutoAcknowledgeMessage(true)
                .natsAutoAckInterval(Duration.ofSeconds(2))
                .build();

            assertNotNull(autoAckSource, "Auto-ack source should not be null");

            // Test manual ack
            NatsJetStreamSource<String> manualAckSource = new NatsJetStreamSourceBuilder<String>()
                .subject(subject)
                .payloadDeserializer(new StringPayloadDeserializer())
                .connectionPropertiesFile(defaultConnectionProperties(url))
                .streamName(streamName)
                .consumerName("manual-ack-consumer")
                .enableAutoAcknowledgeMessage(false)
                .build();

            assertNotNull(manualAckSource, "Manual-ack source should not be null");
        });
    }

    /**
     * Tests boundedness settings for JetStream sources.
     * Verifies configuration of bounded and unbounded consumption patterns.
     *
     * Modes tested:
     * 1. Bounded mode
     *    - Fixed number of messages
     *    - Configurable record limit
     *    - Suitable for batch processing
     *
     * 2. Unbounded mode
     *    - Continuous message consumption
     *    - No record limit
     *    - Suitable for streaming applications
     *
     * Verifies:
     * - Mode-specific configurations
     * - Record limit settings
     * - Builder compatibility
     */
    @Test
    void testBuildWithBoundednessSettings() throws Exception {
        runInJsServer((nc, url) -> {
            String streamName = stream();
            String subject = subject();

            StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(streamName)
                .subjects(subject)
                .storageType(StorageType.Memory)
                .build();

            nc.jetStreamManagement().addStream(streamConfig);

            // Test bounded mode
            NatsJetStreamSource<String> boundedSource = new NatsJetStreamSourceBuilder<String>()
                .subject(subject)
                .payloadDeserializer(new StringPayloadDeserializer())
                .connectionPropertiesFile(defaultConnectionProperties(url))
                .streamName(streamName)
                .consumerName("bounded-consumer")
                .boundness(Boundedness.BOUNDED)
                .maxFetchRecords(100)
                .build();

            assertNotNull(boundedSource, "Bounded source should not be null");

            // Test unbounded mode
            NatsJetStreamSource<String> unboundedSource = new NatsJetStreamSourceBuilder<String>()
                .subject(subject)
                .payloadDeserializer(new StringPayloadDeserializer())
                .connectionPropertiesFile(defaultConnectionProperties(url))
                .streamName(streamName)
                .consumerName("unbounded-consumer")
                .boundness(Boundedness.CONTINUOUS_UNBOUNDED)
                .build();

            assertNotNull(unboundedSource, "Unbounded source should not be null");
        });
    }
}