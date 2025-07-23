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
        assertEquals(Duration.ZERO, config.ackWait);
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
                .ackWait(ackWait.toMillis())
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
                    .ackWait(30000) // 30 seconds
                    .build();
        });
    }

    @Test
    public void testAckWaitZeroValueAllowed() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.NoAck)
                .ackWait(0)
                .build();

        assertEquals(Duration.ZERO, config.ackWait);
        assertEquals(AckBehavior.NoAck, config.ackBehavior);
    }

    @Test
    public void testAckWaitNegativeValueBecomesZero() {
        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .ackWait(-1000)
                .build();

        assertEquals(Duration.ZERO, config.ackWait);
    }

    @Test
    public void testJsonSerializationWithAckWait() {
        Duration ackWait = Duration.ofSeconds(60);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .ackWait(ackWait.toMillis())
                .build();

        String json = config.toJson();
        assertTrue(json.contains("\"ack_wait\":" + ackWait.toMillis()));
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
                .ackWait(ackWait.toMillis())
                .build();

        String yaml = config.toYaml(0);
        assertTrue(yaml.contains("ack_wait: " + ackWait.toMillis()));
        assertTrue(yaml.contains("ack_behavior: ExplicitButDoNotAck"));
    }

    @Test
    public void testJsonDeserializationWithAckWait() throws JsonParseException {
        String json = "{"
                + "\"stream_name\":\"" + TEST_STREAM + "\","
                + "\"subject\":\"" + TEST_SUBJECT + "\","
                + "\"ack_behavior\":\"AckAll\","
                + "\"ack_wait\":45000"
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
        map.put("ack_wait", 120000L);

        JetStreamSubjectConfiguration config = JetStreamSubjectConfiguration.fromMap(map);

        assertEquals(TEST_STREAM, config.streamName);
        assertEquals(TEST_SUBJECT, config.subject);
        assertEquals(AckBehavior.AllButDoNotAck, config.ackBehavior);
        assertEquals(Duration.ofMinutes(2), config.ackWait);
    }

    @Test
    public void testCopyMethodPreservesAckWait() {
        Duration originalAckWait = Duration.ofSeconds(30);

        JetStreamSubjectConfiguration original = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject("original.subject")
                .ackBehavior(AckBehavior.AckAll)
                .ackWait(originalAckWait.toMillis())
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
                .ackWait(originalAckWait.toMillis())
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
                .ackWait(ackWait.toMillis())
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .ackWait(ackWait.toMillis())
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
                .ackWait(30000)
                .build();

        JetStreamSubjectConfiguration config2 = JetStreamSubjectConfiguration.builder()
                .streamName(TEST_STREAM)
                .subject(TEST_SUBJECT)
                .ackBehavior(AckBehavior.AckAll)
                .ackWait(60000)
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
                .ackWait(ackWait.toMillis())
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
}


