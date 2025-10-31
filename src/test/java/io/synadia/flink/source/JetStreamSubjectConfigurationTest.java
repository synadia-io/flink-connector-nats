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
    private static final Duration VALIDATION_ACK_WAIT = Duration.ofMillis(5000);
    private static final ZonedDateTime VALIDATION_DATE_TIME = ZonedDateTime.now();

    @Test
    public void testBasicConfiguration() {
        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
            .streamName(TEST_STREAM)
            .subject(TEST_SUBJECT)
            .build();

        assertEquals(TEST_STREAM, config1.streamName);
        assertEquals(TEST_SUBJECT, config1.subject);
        assertEquals(-1, config1.startSequence);
        assertNull(config1.startTime);
        assertEquals(-1, config1.maxMessagesToRead);
        assertEquals(AckBehavior.NoAck, config1.ackBehavior);
        assertNull(config1.ackWait);
        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, config1.boundedness);
        assertNull(config1.deliverPolicy);

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
            .streamName(TEST_STREAM)
            .subject(TEST_SUBJECT)
            .startTime(VALIDATION_DATE_TIME)
            .maxMessagesToRead(1000)
            .ackBehavior(AckBehavior.AllButDoNotAck)
            .ackWait(0) // coverage
            .ackWait(VALIDATION_ACK_WAIT)
            .batchSize(50)
            .thresholdPercent(80)
            .build();

        assertEquals(TEST_STREAM, config2.streamName);
        assertEquals(TEST_SUBJECT, config2.subject);
        assertEquals(VALIDATION_DATE_TIME, config2.startTime);
        assertEquals(1000, config2.maxMessagesToRead);
        assertEquals(AckBehavior.AllButDoNotAck, config2.ackBehavior);
        assertEquals(VALIDATION_ACK_WAIT, config2.ackWait);
        assertEquals(Boundedness.BOUNDED, config2.boundedness);
        assertEquals(DeliverPolicy.ByStartTime, config2.deliverPolicy);
        assertEquals(50, config2.serializableConsumeOptions.getConsumeOptions().getBatchSize());
        assertEquals(80, config2.serializableConsumeOptions.getConsumeOptions().getThresholdPercent());

        //noinspection EqualsWithItself
        assertEquals(config1, config1);
        assertNotEquals(config1, config2);
        //noinspection SimplifiableAssertion
        assertFalse(config1.equals(new Object()));

        JetStreamSubjectConfiguration configN = JetStreamSubjectConfiguration.builder()
            .streamName(TEST_STREAM)
            .subject(TEST_SUBJECT)
            .durableName("name")
            .ackBehavior(AckBehavior.AllButDoNotAck)
            .build();
        assertEquals("name", configN.durableName);

        configN = JetStreamSubjectConfiguration.builder()
            .streamName(TEST_STREAM)
            .subject(TEST_SUBJECT)
            .consumerNamePrefix("name")
            .build();
        assertEquals("name", configN.consumerNamePrefix);
    }

    @Test
    public void testCoverageAndDeprecated() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
            .streamName(TEST_STREAM)
            .subject(TEST_SUBJECT)
            .build();
        assertEquals(config.toJson(), config.toString());
    }

    @Test
    public void testAckWait() {
        for (AckBehavior ab : AckBehavior.values()) {
            JetStreamSubjectConfiguration.Builder b1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(ab)
                .ackWait(VALIDATION_ACK_WAIT);

            // coverage for ackWait
            JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(ab)
                .ackWait(VALIDATION_ACK_WAIT.toMillis());

            if (ab.isNoAck) {
                assertThrows(IllegalArgumentException.class, b1::build);
            }
            else {
                JetStreamSubjectConfiguration config = b1.build();
                assertEquals(ab, config.ackBehavior);
                assertEquals(VALIDATION_ACK_WAIT, config.ackWait);
            }
        }
    }

    @Test
    public void testAckWaitWillBeNull() {
        validateAckWaitIsNull(null);
        validateAckWaitIsNull(Duration.ZERO);
        validateAckWaitIsNull(Duration.ofMillis(-1));
    }

    private static void validateAckWaitIsNull(Duration aw) {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
            .streamName(TEST_STREAM)
            .subject(TEST_SUBJECT)
            .ackWait(aw)
            .build();
        assertNull(config.ackWait);
    }

    @Test
    public void testSerializationConsideringAckWait() {
        for (AckBehavior ab : AckBehavior.values()) {
            if (ab.isNoAck) {
                JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                    .streamName(TEST_STREAM)
                    .subject(TEST_SUBJECT)
                    .ackBehavior(ab)
                    .build();

                String json = config.toJson();
                if (ab == AckBehavior.NoAck) {
                    assertFalse(json.contains("ack_behavior"));
                }
                else {
                    assertTrue(json.contains("\"ack_behavior\":\"" + ab.behavior + "\""));
                }
                assertFalse(json.contains("ack_wait"));

                String yaml = config.toYaml(0);
                if (ab == AckBehavior.NoAck) {
                    assertFalse(yaml.contains("ack_behavior"));
                }
                else {
                    assertTrue(yaml.contains("ack_behavior: " + ab.behavior));
                }
                assertFalse(yaml.contains("ack_wait"));
            }
            else {
                JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                    .streamName(TEST_STREAM)
                    .subject(TEST_SUBJECT)
                    .ackBehavior(ab)
                    .ackWait(VALIDATION_ACK_WAIT)
                    .build();

                String json = config.toJson();
                assertTrue(json.contains("\"ack_behavior\":\"" + ab.behavior + "\""));
                assertTrue(json.contains("\"ack_wait\":" + VALIDATION_ACK_WAIT.toNanos()));

                String yaml = config.toYaml(0);
                assertTrue(yaml.contains("ack_behavior: " + ab.behavior));
                assertTrue(yaml.contains("ack_wait: " + VALIDATION_ACK_WAIT.toNanos()));
            }
        }
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
    public void testBuildValidation() {
        // Missing subject
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .build());
        assertTrue(iae.getMessage().contains("Subject is required."));

        // Missing stream name
        iae = assertThrows(IllegalArgumentException.class,
            () -> JetStreamSubjectConfiguration.builder()
                .subject(TEST_SUBJECT)
                .build());
        assertTrue(iae.getMessage().contains("Stream name is required."));

        // Setting start sequence and start time should throw
        iae = assertThrows(IllegalArgumentException.class,
            () -> JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .startTime(VALIDATION_DATE_TIME)
                .startSequence(100)
                .build());
        assertTrue(iae.getMessage().contains("Cannot set both start sequence and start time."));

        // Setting ack wait and no ack
        iae = assertThrows(IllegalArgumentException.class,
            () -> JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.NoAck)
                .ackWait(VALIDATION_ACK_WAIT)
                .build());
        assertTrue(iae.getMessage().contains("Ack Wait cannot be set when Ack Behavior does not ack."));

        iae = assertThrows(IllegalArgumentException.class,
            () -> JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.NoAckUnordered)
                .ackWait(VALIDATION_ACK_WAIT)
                .build());
        assertTrue(iae.getMessage().contains("Ack Wait cannot be set when Ack Behavior does not ack."));

        iae = assertThrows(IllegalArgumentException.class,
            () -> JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.NoAck)
                .durableName("D")
                .build());
        assertTrue(iae.getMessage().contains("Durable name cannot be set when Ack Behavior does not ack."));

        iae = assertThrows(IllegalArgumentException.class,
            () -> JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.NoAckUnordered)
                .durableName("D")
                .build());
        assertTrue(iae.getMessage().contains("Durable name cannot be set when Ack Behavior does not ack."));

        iae = assertThrows(IllegalArgumentException.class,
            () -> JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .consumerNamePrefix("P")
                .durableName("D")
                .build());
        assertTrue(iae.getMessage().contains("Durable Name and Consumer Name Prefix cannot both be set."));
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
    public void testDurableNameWithValidAckBehavior() {
        String durableName = "test-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName(durableName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertEquals(durableName, config.durableName);
        assertEquals(AckBehavior.AckAll, config.ackBehavior);
    }

    @Test
    public void testDurableNameWithAllButDoNotAck() {
        String durableName = "my-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName(durableName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AllButDoNotAck)
                .build();

        assertEquals(durableName, config.durableName);
        assertEquals(AckBehavior.AllButDoNotAck, config.ackBehavior);
    }

    @Test
    public void testDurableNameWithExplicitButDoNotAck() {
        String durableName = "explicit-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName(durableName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.ExplicitButDoNotAck)
                .build();

        assertEquals(durableName, config.durableName);
        assertEquals(AckBehavior.ExplicitButDoNotAck, config.ackBehavior);
    }

    @Test
    public void testDurableNameWithNoAckThrowsException() {
        assertThrows(IllegalArgumentException.class,
            () -> JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName("test-consumer")
                .ackBehavior(AckBehavior.NoAck)
                .build(), "Consumer Name cannot be set when Ack Behavior is NoAck.");
    }

    @Test
    public void testDurableNameWithDefaultNoAckThrowsException() {
        // Default ack behavior is NoAck
        assertThrows(IllegalArgumentException.class,
            () -> JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName("test-consumer")
                .build(), "Consumer Name cannot be set when Ack Behavior is NoAck.");
    }

    @Test
    public void testDurableNameNullWithNoAckAllowed() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName(null)
                .ackBehavior(AckBehavior.NoAck)
                .build();

        assertNull(config.durableName);
        assertEquals(AckBehavior.NoAck, config.ackBehavior);
    }

    @Test
    public void testDurableNameEmptyWithNoAckAllowed() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName("")
                .ackBehavior(AckBehavior.NoAck)
                .build();

        assertNull(config.durableName);
        assertEquals(AckBehavior.NoAck, config.ackBehavior);
    }

    @Test
    public void testJsonSerializationWithNonDurablePrefix() {
        String prefix = "prefix";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
            .streamName(TEST_STREAM)
            .subject(TEST_SUBJECT)
            .consumerNamePrefix(prefix)
            .inactiveThreshold(Duration.ofMinutes(10))
            .build();

        String json = config.toJson();
        assertTrue(json.contains("\"consumer_name_prefix\":\"" + prefix + "\""));
    }

    @Test
    public void testJsonSerializationWithDurableName() {
        String durableName = "json-test-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
            .streamName(TEST_STREAM)
            .subject(TEST_SUBJECT)
            .durableName(durableName)
            .inactiveThreshold(Duration.ofMinutes(10))
            .ackBehavior(AckBehavior.AckAll)
            .build();

        String json = config.toJson();
        assertTrue(json.contains("\"durable_name\":\"" + durableName + "\""));
        assertTrue(json.contains("\"ack_behavior\":\"AckAll\""));
    }

    @Test
    public void testJsonSerializationWithoutNameOrPrefix() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        String json = config.toJson();
        assertFalse(json.contains("\"consumer_name_prefix\""));
        assertFalse(json.contains("\"durable_name\""));
    }

    @Test
    public void testYamlSerializationWithNonDurablePrefix() {
        String prefix = "prefix";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
            .streamName(TEST_STREAM)
            .subject(TEST_SUBJECT)
            .consumerNamePrefix(prefix)
            .inactiveThreshold(Duration.ofMinutes(10))
            .build();

        String yaml = config.toYaml(0);
        assertTrue(yaml.contains("consumer_name_prefix: " + prefix));
    }

    @Test
    public void testYamlSerializationWithDurableName() {
        String durableName = "yaml-test-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
            .streamName(TEST_STREAM)
            .subject(TEST_SUBJECT)
            .durableName(durableName)
            .inactiveThreshold(Duration.ofMinutes(10))
            .ackBehavior(AckBehavior.ExplicitButDoNotAck)
            .build();

        String yaml = config.toYaml(0);
        assertTrue(yaml.contains("durable_name: " + durableName));
        assertTrue(yaml.contains("ack_behavior: ExplicitButDoNotAck"));
    }

    @Test
    public void testJsonDeserializationWithDurableName() throws JsonParseException {
        String durableName = "deserialized-consumer";
        String json = "{"
                + "\"stream_name\":\"" + TEST_STREAM + "\","
                + "\"subject\":\"" + TEST_SUBJECT + "\","
                + "\"durable_name\":\"" + durableName + "\","
                + "\"inactive_threshold\":" + Duration.ofMinutes(10).toNanos() +","
                + "\"ack_behavior\":\"AckAll\""
                + "}";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromJson(json);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertEquals(durableName, config.durableName);
        assertEquals(AckBehavior.AckAll, config.ackBehavior);
    }

    @Test
    public void testJsonDeserializationWithoutDurableName() throws JsonParseException {
        String json = "{"
                + "\"stream_name\":\"" + TEST_STREAM + "\","
                + "\"subject\":\"" + TEST_SUBJECT + "\","
                + "\"ack_behavior\":\"AckAll\""
                + "}";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromJson(json);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertNull(config.durableName);
        assertEquals(AckBehavior.AckAll, config.ackBehavior);
    }

    @Test
    public void testMapDeserializationWithDurableName() {
        String durableName = "map-test-consumer";
        Map<String, Object> map = new HashMap<>();
        map.put("stream_name", TEST_STREAM);
        map.put("subject", TEST_SUBJECT);
        map.put("durable_name", durableName);
        map.put("ack_behavior", "AllButDoNotAck");
        map.put("inactive_threshold", Duration.ofMinutes(10).toNanos());

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromMap(map);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertEquals(durableName, config.durableName);
        assertEquals(AckBehavior.AllButDoNotAck, config.ackBehavior);
    }

    @Test
    public void testMapDeserializationWithoutDurableName() {
        Map<String, Object> map = new HashMap<>();
        map.put("stream_name", TEST_STREAM);
        map.put("subject", TEST_SUBJECT);
        map.put("ack_behavior", "AckAll");

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromMap(map);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertNull(config.durableName);
        assertEquals(AckBehavior.AckAll, config.ackBehavior);
    }

    @Test
    public void testCopyMethodNotPreservesDurableName() {
        String originalDurableName = "original-consumer";

        JetStreamSubjectConfiguration original = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject("original.subject")
                .durableName(originalDurableName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration copy = original.copy("new.subject");

        assertEquals("new.subject", copy.subject);
        assertEquals(TEST_STREAM, copy.streamName);
        assertNull(copy.durableName);
        assertEquals(AckBehavior.AckAll, copy.ackBehavior);
    }

    @Test
    public void testBuilderCopyMethodExcludesDurableName() {
        String originalDurableName = "builder-copy-consumer";

        JetStreamSubjectConfiguration original = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject("original.subject")
                .durableName(originalDurableName)
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
        assertNull(copy.durableName);
        assertEquals(AckBehavior.ExplicitButDoNotAck, copy.ackBehavior);
        assertEquals(100, copy.maxMessagesToRead);
    }

    @Test
    public void testEqualsAndHashCodeWithDurableName() {
        String durableName = "equals-test-consumer";

        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName(durableName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName(durableName)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    public void testNotEqualsWithDifferentDurableName() {
        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName("consumer-1")
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName("consumer-2")
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertNotEquals(config1, config2);
    }

    @Test
    public void testNotEqualsWithOneNullDurableName() {
        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName("test-consumer")
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName(null)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertNotEquals(config1, config2);
    }

    @Test
    public void testCompleteConfigurationWithDurableName() {
        ZonedDateTime startTime = ZonedDateTime.now().minusHours(1);
        Duration ackWait = Duration.ofSeconds(90);
        String durableName = "complete-test-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName(durableName)
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
        assertEquals(durableName, config.durableName);
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
    public void testDurableNameIsIncludedInChecksum() {
        String durableName1 = "consumer-1";
        String durableName2 = "consumer-2";

        JetStreamSubjectConfiguration config1 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName(durableName1)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName(durableName2)
                .inactiveThreshold(Duration.ofMinutes(10))
                .ackBehavior(AckBehavior.AckAll)
                .build();

        // Different consumer names should result in different IDs (checksums)
        assertNotEquals(config1.id, config2.id);
    }

    @Test
    public void testInactiveThresholdBasicFunctionality() {
        Duration inactiveThreshold = Duration.ofHours(2);

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
        Duration hoursThreshold = Duration.ofHours(2);
        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .inactiveThreshold(hoursThreshold)
                .ackBehavior(AckBehavior.AckAll)
                .build();
        assertEquals(hoursThreshold, config2.inactiveThreshold);

        // Test days
        Duration daysThreshold = Duration.ofHours(1);
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
        Duration inactiveThreshold = Duration.ofHours(1);

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
        Duration inactiveThreshold = Duration.ofMinutes(20);

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
        Duration inactiveThreshold = Duration.ofHours(1);
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
        Duration originalInactiveThreshold = Duration.ofMinutes(30);

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
        Duration inactiveThreshold = Duration.ofHours(1);

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
        Duration inactiveThreshold = Duration.ofHours(1);
        String durableName = "complete-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName(durableName)
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
        assertEquals(durableName, config.durableName);
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
    public void testInactiveThresholdWithDurableNameCompatibility() {
        Duration inactiveThreshold = Duration.ofHours(1);
        String durableName = "threshold-consumer";

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .durableName(durableName)
                .inactiveThreshold(inactiveThreshold)
                .ackBehavior(AckBehavior.AckAll)
                .build();

        assertEquals(durableName, config.durableName);
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

    @Test
    public void testBuildValidationCoverage() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
            .streamName(TEST_STREAM)
            .subject(TEST_SUBJECT)
            .durableName("")
            .ackBehavior(AckBehavior.NoAck)
            .build();

        assertNull(config.durableName);
        assertEquals(AckBehavior.NoAck, config.ackBehavior);
    }
}
