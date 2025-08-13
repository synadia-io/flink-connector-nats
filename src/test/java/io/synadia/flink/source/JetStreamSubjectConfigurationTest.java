// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.api.DeliverPolicy;
import io.nats.client.support.JsonParseException;
import io.synadia.flink.TestBase;
import org.apache.flink.api.connector.source.Boundedness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamSubjectConfigurationTest extends TestBase {

    private static final String TEST_STREAM = "test-stream";
    private static final String TEST_SUBJECT = "test.subject";

    @Test
    public void testBasicConfiguration() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .build();

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertEquals(-1, config.startSequence);
        assertNull(config.startTime);
        assertEquals(-1, config.maxMessagesToRead);
        assertEquals(AckBehavior.NoAck, config.ackBehavior);
        assertNull(config.ackWait);
        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, config.boundedness);
        assertNull(config.deliverPolicy);
    }

    @Test
    public void testAckWaitWithValidAckBehavior() {
        Duration ackWait = Duration.ofSeconds(30);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .ackWait(ackWait)
                .build();

        assertEquals(AckBehavior.AckAll, config.ackBehavior);
        assertEquals(ackWait, config.ackWait);
    }

    @Test
    public void testAckWaitWithAllButDoNotAck() {
        Duration ackWait = Duration.ofMinutes(2);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AllButDoNotAck)
                .ackWait(ackWait)
                .build();

        assertEquals(AckBehavior.AllButDoNotAck, config.ackBehavior);
        assertEquals(ackWait, config.ackWait);
    }

    @Test
    public void testAckWaitMillisWithAllButDoNotAck() {
        Duration ackWait = Duration.ofMinutes(2);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AllButDoNotAck)
                .ackWait(ackWait.toMillis())
                .build();

        assertEquals(AckBehavior.AllButDoNotAck, config.ackBehavior);
        assertEquals(ackWait, config.ackWait);
    }

    @Test
    public void testAckWaitWithExplicitButDoNotAck() {
        Duration ackWait = Duration.ofSeconds(45);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.ExplicitButDoNotAck)
                .ackWait(ackWait)
                .build();

        assertEquals(AckBehavior.ExplicitButDoNotAck, config.ackBehavior);
        assertEquals(ackWait, config.ackWait);
    }

    @Test
    public void testAckWaitMillisWithExplicitButDoNotAck() {
        Duration ackWait = Duration.ofSeconds(45);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.ExplicitButDoNotAck)
                .ackWait(ackWait.toMillis())
                .build();

        assertEquals(AckBehavior.ExplicitButDoNotAck, config.ackBehavior);
        assertEquals(ackWait, config.ackWait);
    }

    @Test
    public void testAckWaitWithNoAckThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            JetStreamSubjectConfiguration.builder()
                    .streamName(TEST_STREAM)
                    .subject(TEST_SUBJECT)
                    .ackBehavior(AckBehavior.NoAck)
                    .ackWait(Duration.ofSeconds(30))
                    .build();
        });
    }

    @Test
    public void testAckWaitMillisWithNoAckThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            JetStreamSubjectConfiguration.builder()
                    .streamName(TEST_STREAM)
                    .subject(TEST_SUBJECT)
                    .ackBehavior(AckBehavior.NoAck)
                    .ackWait(Duration.ofSeconds(30).toMillis())
                    .build();
        });
    }

    @Test
    public void testAckWaitZeroValueAllowed() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.NoAck)
                .ackWait(Duration.ZERO)
                .build();

        assertNull(config.ackWait);
        assertEquals(AckBehavior.NoAck, config.ackBehavior);
    }

    @Test
    public void testAckWaitMillisZeroValueAllowed() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.NoAck)
                .ackWait(Duration.ZERO.toMillis())
                .build();

        assertNull(config.ackWait);
        assertEquals(AckBehavior.NoAck, config.ackBehavior);
    }

    @Test
    public void testAckWaitMillisNegativeValueBecomesZero() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .ackWait(Duration.ofMillis(-1000).toMillis())
                .build();

        assertNull(config.ackWait);
    }

    @Test
    public void testJsonSerializationWithAckWait() {
        Duration ackWait = Duration.ofSeconds(60);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .ackWait(ackWait)
                .build();

        String json = config.toJson();
        System.out.println(json);
        assertTrue(json.contains("\"ack_wait\":" + ackWait.toNanos()));
        assertTrue(json.contains("\"ack_behavior\":\"AckAll\""));
    }

    @Test
    public void testJsonSerializationWithoutAckWait() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.NoAck)
                .build();

        String json = config.toJson();
        assertFalse(json.contains("ack_wait"));
        assertFalse(json.contains("ack_behavior")); // NoAck is default, so not included
    }

    @Test
    public void testYamlSerializationWithAckWait() {
        Duration ackWait = Duration.ofMinutes(5);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.ExplicitButDoNotAck)
                .ackWait(ackWait)
                .build();

        String yaml = config.toYaml(0);
        assertTrue(yaml.contains("ack_wait: " + ackWait.toNanos()));
        assertTrue(yaml.contains("ack_behavior: ExplicitButDoNotAck"));
    }

    @Test
    public void testJsonDeserializationWithAckWait() throws JsonParseException {
        String json = "{"
                + "\"stream_name\":\"" + TEST_STREAM + "\","
                + "\"subject\":\"" + TEST_SUBJECT + "\","
                + "\"ack_behavior\":\"AckAll\","
                + "\"ack_wait\":" + Duration.ofSeconds(45).toNanos()
                + "}";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromJson(json);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertEquals(AckBehavior.AckAll, config.ackBehavior);
        assertEquals(Duration.ofSeconds(45), config.ackWait);
    }

    @Test
    public void testMapDeserializationWithAckWait() {
        Map<String, Object> map = new HashMap<>();
        map.put("stream_name", TEST_STREAM);
        map.put("subject", TEST_SUBJECT);
        map.put("ack_behavior", "AllButDoNotAck");
        map.put("ack_wait", Duration.ofSeconds(12).toNanos());

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromMap(map);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertEquals(AckBehavior.AllButDoNotAck, config.ackBehavior);
        assertEquals(Duration.ofSeconds(12), config.ackWait);
    }

    @Test
    public void testCopyMethodPreservesAckWait() {
        Duration originalAckWait = Duration.ofSeconds(30);

        JetStreamSubjectConfiguration original = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject("original.subject")
                .ackBehavior(AckBehavior.AckAll)
                .ackWait(originalAckWait)
                .build();

        JetStreamSubjectConfiguration copy = original.copy("new.subject");

        assertEquals("new.subject", copy.subject);
        assertEquals(TEST_STREAM, copy.streamName);
        assertEquals(AckBehavior.AckAll, copy.ackBehavior);
        assertEquals(originalAckWait, copy.ackWait);
    }

    @Test
    public void testBuilderCopyMethodPreservesAckWait() {
        Duration originalAckWait = Duration.ofMinutes(1);

        JetStreamSubjectConfiguration original = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject("original.subject")
                .ackBehavior(AckBehavior.ExplicitButDoNotAck)
                .ackWait(originalAckWait)
                .maxMessagesToRead(100)
                .build();

        JetStreamSubjectConfiguration copy = JetStreamSubjectConfiguration.builder()
                .copy(original)
                .subject("copied.subject")
                .build();

        assertEquals("copied.subject", copy.subject);
        assertEquals(TEST_STREAM, copy.streamName);
        assertEquals(AckBehavior.ExplicitButDoNotAck, copy.ackBehavior);
        assertEquals(originalAckWait, copy.ackWait);
        assertEquals(100, copy.maxMessagesToRead);
    }

    @Test
    public void testEqualsAndHashCodeWithAckWait() {
        Duration ackWait = Duration.ofSeconds(30);

        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .ackWait(ackWait)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .ackWait(ackWait)
                .build();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    public void testNotEqualsWithDifferentAckWait() {
        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .ackWait(Duration.ofSeconds(30))
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .ackWait(Duration.ofSeconds(60))
                .build();

        assertNotEquals(config1, config2);
    }

    @Test
    public void testCompleteConfigurationWithAckWait() {
        ZonedDateTime startTime = ZonedDateTime.now().minusHours(1);
        Duration ackWait = Duration.ofSeconds(90);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .startTime(startTime)
                .maxMessagesToRead(1000)
                .ackBehavior(AckBehavior.AllButDoNotAck)
                .ackWait(ackWait)
                .batchSize(50)
                .thresholdPercent(80)
                .build();

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertEquals(startTime, config.startTime);
        assertEquals(1000, config.maxMessagesToRead);
        assertEquals(AckBehavior.AllButDoNotAck, config.ackBehavior);
        assertEquals(ackWait, config.ackWait);
        assertEquals(Boundedness.BOUNDED, config.boundedness);
        assertEquals(DeliverPolicy.ByStartTime, config.deliverPolicy);
        assertEquals(50, config.serializableConsumeOptions.getConsumeOptions().getBatchSize());
        assertEquals(80, config.serializableConsumeOptions.getConsumeOptions().getThresholdPercent());
    }

    @Test
    public void testRequiredFieldsValidation() {
        // Missing subject
        assertThrows(IllegalArgumentException.class, () -> {
            JetStreamSubjectConfiguration.builder()
                    .streamName(TEST_STREAM)
                    .build();
        });

        // Missing stream name
        assertThrows(IllegalArgumentException.class, () -> {
            JetStreamSubjectConfiguration.builder()
                    .subject(TEST_SUBJECT)
                    .build();
        });
    }

    @Test
    public void testStartSequenceAndStartTimeExclusive() {
        ZonedDateTime startTime = ZonedDateTime.now();

        // Setting start sequence after start time should throw
        assertThrows(IllegalArgumentException.class, () -> {
            JetStreamSubjectConfiguration.builder()
                    .streamName(TEST_STREAM)
                    .subject(TEST_SUBJECT)
                    .startTime(startTime)
                    .startSequence(100)
                    .build();
        });

        // Setting start time after start sequence should throw
        assertThrows(IllegalArgumentException.class, () -> {
            JetStreamSubjectConfiguration.builder()
                    .streamName(TEST_STREAM)
                    .subject(TEST_SUBJECT)
                    .startSequence(100)
                    .startTime(startTime)
                    .build();
        });
    }

    @Test
    public void testDeliverPolicyBasedOnStartConfiguration() {
        // No start configuration
        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .build();
        assertNull(config1.deliverPolicy);

        // Start sequence
        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .startSequence(100)
                .build();
        assertEquals(DeliverPolicy.ByStartSequence, config2.deliverPolicy);

        // Start time
        JetStreamSubjectConfiguration config3 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .startTime(ZonedDateTime.now())
                .build();
        assertEquals(DeliverPolicy.ByStartTime, config3.deliverPolicy);
    }

    @Test
    public void testBoundednessBasedOnMaxMessages() {
        // Unbounded (default)
        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .build();
        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, config1.boundedness);

        // Bounded
        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .maxMessagesToRead(500)
                .build();
        assertEquals(Boundedness.BOUNDED, config2.boundedness);
    }

    @Test
    public void testConsumerNameWithValidAckBehavior() {
        String consumerName = "test-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(consumerName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertEquals(consumerName, config.consumerName);
        assertEquals(AckBehavior.AckAll, config.ackBehavior);
    }

    @Test
    public void testConsumerNameWithAllButDoNotAck() {
        String consumerName = "my-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(consumerName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AllButDoNotAck)
                .build();

        assertEquals(consumerName, config.consumerName);
        assertEquals(AckBehavior.AllButDoNotAck, config.ackBehavior);
    }

    @Test
    public void testConsumerNameWithExplicitButDoNotAck() {
        String consumerName = "explicit-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(consumerName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.ExplicitButDoNotAck)
                .build();

        assertEquals(consumerName, config.consumerName);
        assertEquals(AckBehavior.ExplicitButDoNotAck, config.ackBehavior);
    }

    @Test
    public void testConsumerNameWithNoAckThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            JetStreamSubjectConfiguration.builder()
                    .streamName(TEST_STREAM)
                    .subject(TEST_SUBJECT)
                    .consumerName("test-consumer")
                    .ackBehavior(AckBehavior.NoAck)
                    .build();
        }, "Consumer Name cannot be set when Ack Behavior is NoAck.");
    }

    @Test
    public void testConsumerNameWithDefaultNoAckThrowsException() {
        // Default ack behavior is NoAck
        assertThrows(IllegalArgumentException.class, () -> {
            JetStreamSubjectConfiguration.builder()
                    .streamName(TEST_STREAM)
                    .subject(TEST_SUBJECT)
                    .consumerName("test-consumer")
                    .build();
        }, "Consumer Name cannot be set when Ack Behavior is NoAck.");
    }

    @Test
    public void testConsumerNameNullWithNoAckAllowed() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(null)
                .ackBehavior(AckBehavior.NoAck)
                .build();

        assertNull(config.consumerName);
        assertEquals(AckBehavior.NoAck, config.ackBehavior);
    }

    @Test
    public void testConsumerNameEmptyWithNoAckAllowed() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName("")
                .ackBehavior(AckBehavior.NoAck)
                .build();

        assertEquals("", config.consumerName);
        assertEquals(AckBehavior.NoAck, config.ackBehavior);
    }

    @Test
    public void testJsonSerializationWithConsumerName() {
        String consumerName = "json-test-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(consumerName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        String json = config.toJson();
        assertTrue(json.contains("\"consumer_name\":\"" + consumerName + "\""));
        assertTrue(json.contains("\"ack_behavior\":\"AckAll\""));
    }

    @Test
    public void testJsonSerializationWithoutConsumerName() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        String json = config.toJson();
        assertTrue(json.contains("\"consumer_name\":null") || !json.contains("consumer_name"));
    }

    @Test
    public void testYamlSerializationWithConsumerName() {
        String consumerName = "yaml-test-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(consumerName)
                .inactiveThreshold(Duration.ofSeconds(10))
                .ackBehavior(AckBehavior.ExplicitButDoNotAck)
                .build();

        String yaml = config.toYaml(0);
        assertTrue(yaml.contains("consumer_name: " + consumerName));
        assertTrue(yaml.contains("ack_behavior: ExplicitButDoNotAck"));
    }

    @Test
    public void testJsonDeserializationWithConsumerName() throws JsonParseException {
        String consumerName = "deserialized-consumer";
        String json = "{"
                + "\"stream_name\":\"" + TEST_STREAM + "\","
                + "\"subject\":\"" + TEST_SUBJECT + "\","
                + "\"consumer_name\":\"" + consumerName + "\","
                + "\"inactive_threshold\":10000,"
                + "\"ack_behavior\":\"AckAll\""
                + "}";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromJson(json);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertEquals(consumerName, config.consumerName);
        assertEquals(AckBehavior.AckAll, config.ackBehavior);
    }

    @Test
    public void testJsonDeserializationWithoutConsumerName() throws JsonParseException {
        String json = "{"
                + "\"stream_name\":\"" + TEST_STREAM + "\","
                + "\"subject\":\"" + TEST_SUBJECT + "\","
                + "\"ack_behavior\":\"AckAll\""
                + "}";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromJson(json);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertNull(config.consumerName);
        assertEquals(AckBehavior.AckAll, config.ackBehavior);
    }

    @Test
    public void testMapDeserializationWithConsumerName() {
        String consumerName = "map-test-consumer";
        Map<String, Object> map = new HashMap<>();
        map.put("stream_name", TEST_STREAM);
        map.put("subject", TEST_SUBJECT);
        map.put("consumer_name", consumerName);
        map.put("ack_behavior", "AllButDoNotAck");
        map.put("inactive_threshold", 5000);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromMap(map);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertEquals(consumerName, config.consumerName);
        assertEquals(AckBehavior.AllButDoNotAck, config.ackBehavior);
    }

    @Test
    public void testMapDeserializationWithoutConsumerName() {
        Map<String, Object> map = new HashMap<>();
        map.put("stream_name", TEST_STREAM);
        map.put("subject", TEST_SUBJECT);
        map.put("ack_behavior", "AckAll");

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromMap(map);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertNull(config.consumerName);
        assertEquals(AckBehavior.AckAll, config.ackBehavior);
    }

    @Test
    public void testCopyMethodNotPreservesConsumerName() {
        String originalConsumerName = "original-consumer";

        JetStreamSubjectConfiguration original = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject("original.subject")
                .consumerName(originalConsumerName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration copy = original.copy("new.subject");

        assertEquals("new.subject", copy.subject);
        assertEquals(TEST_STREAM, copy.streamName);
        assertNull(copy.consumerName);
        assertEquals(AckBehavior.AckAll, copy.ackBehavior);
    }

    @Test
    public void testBuilderCopyMethodExcludesConsumerName() {
        String originalConsumerName = "builder-copy-consumer";

        JetStreamSubjectConfiguration original = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject("original.subject")
                .consumerName(originalConsumerName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.ExplicitButDoNotAck)
                .maxMessagesToRead(100)
                .build();

        JetStreamSubjectConfiguration copy = JetStreamSubjectConfiguration.builder()
                .copy(original)
                .subject("copied.subject")
                .build();

        assertEquals("copied.subject", copy.subject);
        assertEquals(TEST_STREAM, copy.streamName);
        assertNull(copy.consumerName);
        assertEquals(AckBehavior.ExplicitButDoNotAck, copy.ackBehavior);
        assertEquals(100, copy.maxMessagesToRead);
    }

    @Test
    public void testEqualsAndHashCodeWithConsumerName() {
        String consumerName = "equals-test-consumer";

        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(consumerName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(consumerName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    public void testNotEqualsWithDifferentConsumerName() {
        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName("consumer-1")
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName("consumer-2")
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertNotEquals(config1, config2);
    }

    @Test
    public void testNotEqualsWithOneNullConsumerName() {
        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName("test-consumer")
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(null)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertNotEquals(config1, config2);
    }

    @Test
    public void testCompleteConfigurationWithConsumerName() {
        ZonedDateTime startTime = ZonedDateTime.now().minusHours(1);
        Duration ackWait = Duration.ofSeconds(90);
        String consumerName = "complete-test-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(consumerName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .startTime(startTime)
                .maxMessagesToRead(1000)
                .ackBehavior(AckBehavior.AllButDoNotAck)
                .ackWait(ackWait)
                .batchSize(50)
                .thresholdPercent(80)
                .build();

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertEquals(consumerName, config.consumerName);
        assertEquals(startTime, config.startTime);
        assertEquals(1000, config.maxMessagesToRead);
        assertEquals(AckBehavior.AllButDoNotAck, config.ackBehavior);
        assertEquals(ackWait, config.ackWait);
        assertEquals(Boundedness.BOUNDED, config.boundedness);
        assertEquals(DeliverPolicy.ByStartTime, config.deliverPolicy);
        assertEquals(50, config.serializableConsumeOptions.getConsumeOptions().getBatchSize());
        assertEquals(80, config.serializableConsumeOptions.getConsumeOptions().getThresholdPercent());
    }

    @Test
    public void testConsumerNameIsIncludedInChecksum() {
        String consumerName1 = "consumer-1";
        String consumerName2 = "consumer-2";

        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(consumerName1)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(consumerName2)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        // Different consumer names should result in different IDs (checksums)
        assertNotEquals(config1.id, config2.id);
    }

    @Test
    public void testInactiveThresholdBasicFunctionality() {
        Duration inactiveThreshold = Duration.ofHours(24);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(inactiveThreshold)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertEquals(inactiveThreshold, config.inactiveThreshold);
    }

    @Test
    public void testInactiveThresholdWithNullValue() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(null)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertNull(config.inactiveThreshold);
    }

    @Test
    public void testInactiveThresholdWithZeroDuration() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(Duration.ZERO)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertNull(config.inactiveThreshold);
    }

    @Test
    public void testInactiveThresholdWithNegativeDuration() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(Duration.ofMinutes(-30))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertNull(config.inactiveThreshold);
    }

    @Test
    public void testInactiveThresholdVariousDurations() {
        // Test minutes
        Duration minutesThreshold = Duration.ofMinutes(30);
        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(minutesThreshold)
                .ackBehavior(AckBehavior.AckAll)
                .build();
        assertEquals(minutesThreshold, config1.inactiveThreshold);

        // Test hours
        Duration hoursThreshold = Duration.ofHours(12);
        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(hoursThreshold)
                .ackBehavior(AckBehavior.AckAll)
                .build();
        assertEquals(hoursThreshold, config2.inactiveThreshold);

        // Test days
        Duration daysThreshold = Duration.ofDays(7);
        JetStreamSubjectConfiguration config3 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(daysThreshold)
                .ackBehavior(AckBehavior.AckAll)
                .build();
        assertEquals(daysThreshold, config3.inactiveThreshold);
    }

    @Test
    public void testJsonSerializationWithInactiveThreshold() {
        Duration inactiveThreshold = Duration.ofHours(6);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(inactiveThreshold)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        String json = config.toJson();
        assertTrue(json.contains("\"inactive_threshold\":" + inactiveThreshold.toNanos()));
    }

    @Test
    public void testJsonSerializationWithoutInactiveThreshold() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        String json = config.toJson();
        assertTrue(json.contains("\"inactive_threshold\":null") || !json.contains("inactive_threshold"));
    }

    @Test
    public void testYamlSerializationWithInactiveThreshold() {
        Duration inactiveThreshold = Duration.ofDays(1);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(inactiveThreshold)
                .ackBehavior(AckBehavior.AllButDoNotAck)
                .build();

        String yaml = config.toYaml(0);
        assertTrue(yaml.contains("inactive_threshold: " + inactiveThreshold.toNanos()));
    }

    @Test
    public void testJsonDeserializationWithInactiveThreshold() throws JsonParseException {
        Duration inactiveThreshold = Duration.ofHours(8);
        String json = "{"
                + "\"stream_name\":\"" + TEST_STREAM + "\","
                + "\"subject\":\"" + TEST_SUBJECT + "\","
                + "\"inactive_threshold\":" + inactiveThreshold.toNanos() + ","
                + "\"ack_behavior\":\"AckAll\""
                + "}";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromJson(json);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertEquals(inactiveThreshold, config.inactiveThreshold);
        assertEquals(AckBehavior.AckAll, config.ackBehavior);
    }

    @Test
    public void testJsonDeserializationWithoutInactiveThreshold() throws JsonParseException {
        String json = "{"
                + "\"stream_name\":\"" + TEST_STREAM + "\","
                + "\"subject\":\"" + TEST_SUBJECT + "\","
                + "\"ack_behavior\":\"AckAll\""
                + "}";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromJson(json);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertNull(config.inactiveThreshold);
        assertEquals(AckBehavior.AckAll, config.ackBehavior);
    }

    @Test
    public void testMapDeserializationWithInactiveThreshold() {
        Duration inactiveThreshold = Duration.ofMinutes(45);
        Map<String, Object> map = new HashMap<>();
        map.put("stream_name", TEST_STREAM);
        map.put("subject", TEST_SUBJECT);
        map.put("inactive_threshold", inactiveThreshold.toNanos());
        map.put("ack_behavior", "ExplicitButDoNotAck");

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromMap(map);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertEquals(inactiveThreshold, config.inactiveThreshold);
        assertEquals(AckBehavior.ExplicitButDoNotAck, config.ackBehavior);
    }

    @Test
    public void testMapDeserializationWithoutInactiveThreshold() {
        Map<String, Object> map = new HashMap<>();
        map.put("stream_name", TEST_STREAM);
        map.put("subject", TEST_SUBJECT);
        map.put("ack_behavior", "AckAll");

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromMap(map);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertNull(config.inactiveThreshold);
        assertEquals(AckBehavior.AckAll, config.ackBehavior);
    }

    @Test
    public void testCopyMethodPreservesInactiveThreshold() {
        Duration originalInactiveThreshold = Duration.ofHours(2);

        JetStreamSubjectConfiguration original = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject("original.subject")
                .inactiveThreshold(originalInactiveThreshold)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration copy = original.copy("new.subject");

        assertEquals("new.subject", copy.subject);
        assertEquals(TEST_STREAM, copy.streamName);
        assertEquals(originalInactiveThreshold, copy.inactiveThreshold);
        assertEquals(AckBehavior.AckAll, copy.ackBehavior);
    }

    @Test
    public void testBuilderCopyMethodPreservesInactiveThreshold() {
        Duration originalInactiveThreshold = Duration.ofDays(3);

        JetStreamSubjectConfiguration original = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject("original.subject")
                .inactiveThreshold(originalInactiveThreshold)
                .ackBehavior(AckBehavior.AllButDoNotAck)
                .maxMessagesToRead(500)
                .build();

        JetStreamSubjectConfiguration copy = JetStreamSubjectConfiguration.builder()
                .copy(original)
                .subject("copied.subject")
                .build();

        assertEquals("copied.subject", copy.subject);
        assertEquals(TEST_STREAM, copy.streamName);
        assertEquals(originalInactiveThreshold, copy.inactiveThreshold);
        assertEquals(AckBehavior.AllButDoNotAck, copy.ackBehavior);
        assertEquals(500, copy.maxMessagesToRead);
    }

    @Test
    public void testEqualsAndHashCodeWithInactiveThreshold() {
        Duration inactiveThreshold = Duration.ofHours(4);

        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(inactiveThreshold)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(inactiveThreshold)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    public void testNotEqualsWithDifferentInactiveThreshold() {
        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(Duration.ofHours(1))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(Duration.ofHours(2))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertNotEquals(config1, config2);
    }

    @Test
    public void testNotEqualsWithOneNullInactiveThreshold() {
        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(Duration.ofMinutes(30))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(null)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertNotEquals(config1, config2);
    }

    @Test
    public void testCompleteConfigurationWithInactiveThreshold() {
        ZonedDateTime startTime = ZonedDateTime.now().minusHours(1);
        Duration ackWait = Duration.ofSeconds(30);
        Duration inactiveThreshold = Duration.ofHours(12);
        String consumerName = "complete-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(consumerName)
                .startTime(startTime)
                .maxMessagesToRead(1000)
                .ackBehavior(AckBehavior.ExplicitButDoNotAck)
                .ackWait(ackWait)
                .inactiveThreshold(inactiveThreshold)
                .batchSize(100)
                .thresholdPercent(75)
                .build();

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertEquals(consumerName, config.consumerName);
        assertEquals(startTime, config.startTime);
        assertEquals(1000, config.maxMessagesToRead);
        assertEquals(AckBehavior.ExplicitButDoNotAck, config.ackBehavior);
        assertEquals(ackWait, config.ackWait);
        assertEquals(inactiveThreshold, config.inactiveThreshold);
        assertEquals(Boundedness.BOUNDED, config.boundedness);
        assertEquals(DeliverPolicy.ByStartTime, config.deliverPolicy);
        assertEquals(100, config.serializableConsumeOptions.getConsumeOptions().getBatchSize());
        assertEquals(75, config.serializableConsumeOptions.getConsumeOptions().getThresholdPercent());
    }

    @Test
    public void testInactiveThresholdWithConsumerNameCompatibility() {
        Duration inactiveThreshold = Duration.ofHours(6);
        String consumerName = "threshold-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .consumerName(consumerName)
                .inactiveThreshold(inactiveThreshold)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertEquals(consumerName, config.consumerName);
        assertEquals(inactiveThreshold, config.inactiveThreshold);
        assertEquals(AckBehavior.AckAll, config.ackBehavior);
    }

    @Test
    public void testInactiveThresholdIsIncludedInChecksum() {
        Duration inactiveThreshold1 = Duration.ofHours(1);
        Duration inactiveThreshold2 = Duration.ofHours(2);

        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(inactiveThreshold1)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(inactiveThreshold2)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        // Different inactive thresholds should result in different IDs (checksums)
        assertNotEquals(config1.id, config2.id);
    }
}
