// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.BaseConsumeOptions;
import io.nats.client.ConsumeOptions;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.support.*;
import io.synadia.flink.utils.MiscUtils;
import io.synadia.flink.utils.YamlUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.shaded.guava31.com.google.common.base.Strings;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;

import static io.nats.client.BaseConsumeOptions.DEFAULT_MESSAGE_COUNT;
import static io.nats.client.BaseConsumeOptions.DEFAULT_THRESHOLD_PERCENT;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.synadia.flink.utils.Constants.*;
import static io.synadia.flink.utils.MiscUtils.checksum;

/**
 * It takes more than a subject to consume.
 */
public class JetStreamSubjectConfiguration implements JsonSerializable, Serializable {
    private static final long serialVersionUID = 1L;

    public final String id;
    public final String streamName;
    public final String subject;
    public final String consumerName;
    public final long startSequence;
    public final ZonedDateTime startTime;
    public final long maxMessagesToRead;
    public final AckBehavior ackBehavior;
    public final Duration ackWait;
    public final SerializableConsumeOptions serializableConsumeOptions;

    public final Boundedness boundedness;
    public final DeliverPolicy deliverPolicy;

    private JetStreamSubjectConfiguration(Builder b, ConsumeOptions consumeOptions) {
        serializableConsumeOptions = new SerializableConsumeOptions(consumeOptions);
        subject = b.subject;
        streamName = b.streamName;
        consumerName = b.consumerName;
        startSequence = b.startSequence;
        startTime = b.startTime;
        maxMessagesToRead = b.maxMessagesToRead;
        ackBehavior = b.ackBehavior;
        ackWait = b.ackWait;

        boundedness = maxMessagesToRead > 0 ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
        deliverPolicy = startSequence != -1
            ? DeliverPolicy.ByStartSequence
            : startTime != null
                ? DeliverPolicy.ByStartTime
                : null;

        id = checksum(subject,
            streamName,
            consumerName,
            startSequence,
            startTime,
            maxMessagesToRead,
            ackBehavior,
            ackWait,
            serializableConsumeOptions.getConsumeOptions()
        );
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, STREAM_NAME, streamName);
        JsonUtils.addField(sb, SUBJECT, subject);
        JsonUtils.addField(sb, CONSUMER_NAME, consumerName);
        JsonUtils.addField(sb, START_SEQUENCE, startSequence);
        JsonUtils.addField(sb, START_TIME, startTime);
        JsonUtils.addField(sb, MAX_MESSAGES_TO_READ, maxMessagesToRead);
        JsonUtils.addEnumWhenNot(sb, ACK_BEHAVIOR, ackBehavior, AckBehavior.NoAck);
        JsonUtils.addFieldAsNanos(sb, ACK_WAIT, ackWait);

        ConsumeOptions co = serializableConsumeOptions.getConsumeOptions();
        if (co.getBatchSize() != DEFAULT_MESSAGE_COUNT) {
            JsonUtils.addFieldWhenGtZero(sb, BATCH_SIZE, co.getBatchSize());
        }
        if (co.getThresholdPercent() != DEFAULT_THRESHOLD_PERCENT) {
            JsonUtils.addFieldWhenGtZero(sb, THRESHOLD_PERCENT, co.getThresholdPercent());
        }

        return endJson(sb).toString();
    }

    public String toYaml(int indentLevel) {
        StringBuilder sb = YamlUtils.beginChild(indentLevel, STREAM_NAME, streamName);
        indentLevel++;
        YamlUtils.addField(sb, indentLevel, SUBJECT, subject);
        YamlUtils.addField(sb, indentLevel, CONSUMER_NAME, consumerName);
        YamlUtils.addField(sb, indentLevel, START_SEQUENCE, startSequence);
        YamlUtils.addField(sb, indentLevel, START_TIME, startTime);
        YamlUtils.addField(sb, indentLevel, MAX_MESSAGES_TO_READ, maxMessagesToRead);
        YamlUtils.addEnumWhenNot(sb, indentLevel, ACK_BEHAVIOR, ackBehavior, AckBehavior.NoAck);
        YamlUtils.addFieldAsNanos(sb, indentLevel, ACK_WAIT, ackWait);

        ConsumeOptions co = serializableConsumeOptions.getConsumeOptions();
        if (co.getBatchSize() != DEFAULT_MESSAGE_COUNT) {
            YamlUtils.addFieldGtZero(sb, indentLevel, BATCH_SIZE, co.getBatchSize());
        }
        if (co.getThresholdPercent() != DEFAULT_THRESHOLD_PERCENT) {
            YamlUtils.addFieldGtZero(sb, indentLevel, THRESHOLD_PERCENT, co.getThresholdPercent());
        }

        return sb.toString();
    }

    public JetStreamSubjectConfiguration copy(String subject) {
        return builder().copy(this).subject(subject).build();
    }

    @Override
    public String toString() {
        return toJson();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static JetStreamSubjectConfiguration fromJson(String json) throws JsonParseException {
        return fromJsonValue(JsonParser.parse(json));
    }

    public static JetStreamSubjectConfiguration fromJsonValue(JsonValue jv) {
        return new Builder()
            .streamName(JsonValueUtils.readString(jv, STREAM_NAME))
            .subject(JsonValueUtils.readString(jv, SUBJECT))
            .consumerName(JsonValueUtils.readString(jv, CONSUMER_NAME))
            .startSequence(JsonValueUtils.readLong(jv, START_SEQUENCE, -1))
            .startTime(JsonValueUtils.readDate(jv, START_TIME))
            .maxMessagesToRead(JsonValueUtils.readLong(jv, MAX_MESSAGES_TO_READ, -1))
            .batchSize(JsonValueUtils.readInteger(jv, BATCH_SIZE, -1))
            .thresholdPercent(JsonValueUtils.readInteger(jv, THRESHOLD_PERCENT, -1))
            .ackBehavior(AckBehavior.get(JsonValueUtils.readString(jv, ACK_BEHAVIOR)))
            .ackWait(JsonValueUtils.readNanos(jv, ACK_WAIT))
            .build();
    }

    public static JetStreamSubjectConfiguration fromMap(Map<String, Object> map) {
        return new Builder()
            .streamName(YamlUtils.readString(map, STREAM_NAME))
            .subject(YamlUtils.readString(map, SUBJECT))
            .consumerName(YamlUtils.readString(map, CONSUMER_NAME))
            .startSequence(YamlUtils.readLong(map, START_SEQUENCE, -1))
            .startTime(YamlUtils.readDate(map, START_TIME))
            .maxMessagesToRead(YamlUtils.readLong(map, MAX_MESSAGES_TO_READ, -1))
            .batchSize(YamlUtils.readInteger(map, BATCH_SIZE, -1))
            .thresholdPercent(YamlUtils.readInteger(map, THRESHOLD_PERCENT, -1))
            .ackBehavior(AckBehavior.get(YamlUtils.readString(map, ACK_BEHAVIOR)))
            .ackWait(YamlUtils.readNanos(map, ACK_WAIT))
            .build();
    }

    public static class Builder {
        private String streamName;
        private String subject;
        private String consumerName;
        private long startSequence = -1;
        private ZonedDateTime startTime;
        private long maxMessagesToRead = -1;
        private AckBehavior ackBehavior = AckBehavior.NoAck;
        private Duration ackWait;
        private int batchSize = -1;
        private int thresholdPercent = -1;

        /**
         * Copies all the configuration except the subject and consumer name.
         * @param config the config to use as a basis for the new config
         * @return the builder
         */
        public Builder copy(JetStreamSubjectConfiguration config) {
            return streamName(config.streamName)
                .startSequence(config.startSequence)
                .startTime(config.startTime)
                .maxMessagesToRead(config.maxMessagesToRead)
                .ackBehavior(config.ackBehavior)
                .ackWait(config.ackWait)
                .batchSize(config.serializableConsumeOptions.getConsumeOptions().getBatchSize())
                .thresholdPercent(config.serializableConsumeOptions.getConsumeOptions().getThresholdPercent());
        }

        /**
         * Sets the stream name
         * @param streamName the stream name
         * @return the builder
         */
        public Builder streamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        /**
         * Sets the subject
         * @param subject the subject
         * @return the builder
         */
        public Builder subject(String subject) {
            this.subject = subject;
            return this;
        }

        /**
         * Sets the consumer name
         * @param consumerName the consumer name
         * @return the builder
         */
        public Builder consumerName(String consumerName) {
            this.consumerName = consumerName;
            return this;
        }

        /**
         * Set the initial Consume batch size in messages.
         * <p>Less than 1 means default of {@value BaseConsumeOptions#DEFAULT_MESSAGE_COUNT} when bytes are not specified.
         * @param batchSize the batch size in messages.
         * @return the builder
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Set the threshold percentage of messages that will trigger issuing pull requests to keep messages flowing.
         * <p>Must be between 1 and 100 inclusive.
         * Less than 1 will assume the default of {@value BaseConsumeOptions#DEFAULT_THRESHOLD_PERCENT}.
         * Greater than 100 will assume 100. </p>
         * @param thresholdPercent the threshold percent
         * @return the builder
         */
        public Builder thresholdPercent(int thresholdPercent) {
            this.thresholdPercent = thresholdPercent;
            return this;
        }

        /**
         * Sets the start sequence of the JetStreamSubjectConfiguration.
         * @param startSequence the start sequence
         * @return the builder
         */
        public Builder startSequence(long startSequence) {
            if (startSequence < 1) {
                this.startSequence = -1;
            }
            else if (startTime != null) {
                throw new IllegalArgumentException("Cannot set both start sequence and start time.");
            }
            else {
                this.startSequence = startSequence;
            }
            return this;
        }

        /**
         * Sets the start time of the JetStreamSubjectConfiguration.
         * @param startTime the start time
         * @return the builder
         */
        public Builder startTime(ZonedDateTime startTime) {
            if (startTime != null && startSequence != -1) {
                throw new IllegalArgumentException("Cannot set both start sequence and start time.");
            }
            this.startTime = startTime;
            return this;
        }

        /**
         * Set the maximum number of messages to read.
         * This makes this configuration Boundedness BOUNDED if the value is greater than zero.
         * @param maxMessagesToRead set the bound of max messages to read
         * @return the builder
         */
        public Builder maxMessagesToRead(long maxMessagesToRead) {
            this.maxMessagesToRead = maxMessagesToRead < 1 ? -1 : maxMessagesToRead;
            return this;
        }

        /**
         * Sets the ack policy for the consumer.
         * Ack will occur when a checkpoint is complete via ack all.
         * AckBehavior.NoAck is the default. AckBehavior.All is slower than NoAck. AckBehavior.ExplicitButNoAck is the slowest.
         * It is not recommended to set ackMode unless your stream is a work queue,
         * but even then, be sure of why you are running this against a work queue.
         * @param ackBehavior the ack behavior or null for AckPolicy.None
         * @return the builder
         */
        public Builder ackBehavior(AckBehavior ackBehavior) {
            this.ackBehavior = ackBehavior == null ? AckBehavior.NoAck : ackBehavior;
            return this;
        }

        /**
         * Sets the ack wait for the consumer.
         * Note: This is not applicable when ackBehavior is set to AckBehavior.NoAck
         * @param ackWaitMillis the ack wait in milliseconds
         * @return the builder
         */
        public Builder ackWait(long ackWaitMillis) {
            this.ackWait = ackWaitMillis <= 0L ? null : Duration.ofMillis(ackWaitMillis);
            return this;
        }

        /**
         * Sets the ack wait for the consumer.
         * Note: This is not applicable when ackBehavior is set to AckBehavior.NoAck
         * @param ackWait the ack wait in Duration
         * @return the builder
         */
        public Builder ackWait(Duration ackWait) {
            if (ackWait == null || ackWait.isZero() || ackWait.isNegative()) {
                this.ackWait = null;
            }
            else {
                this.ackWait = ackWait;
            }

            return this;
        }

        /**
         * @deprecated Use {@link #ackBehavior(AckBehavior)} instead.
         * This sets the AckBehavior to AckBehavior.All
         * @return the builder
         */
        @Deprecated
        public Builder ackMode() {
            this.ackBehavior = AckBehavior.AckAll;
            return this;
        }

        /**
         /**
         * @deprecated Use {@link #ackBehavior(AckBehavior)} instead.
         * @param ackMode false sets the policy to AckBehavior.NoAck. true sets the policy to AckBehavior.All
         * @return the builder
         */
        @Deprecated
        public Builder ackMode(boolean ackMode) {
            this.ackBehavior = ackMode ? AckBehavior.AckAll : AckBehavior.NoAck;
            return this;
        }

        public JetStreamSubjectConfiguration build() {
            if (MiscUtils.notProvided(subject)) {
                throw new IllegalArgumentException("Subject is required.");
            }
            if (MiscUtils.notProvided(streamName)) {
                throw new IllegalArgumentException("Stream name is required.");
            }
            if (ackBehavior == AckBehavior.NoAck && ackWait != null) {
                throw new IllegalArgumentException("Ack Wait cannot be set when Ack Behavior is NoAck.");
            }
            if (ackBehavior == AckBehavior.NoAck && !Strings.isNullOrEmpty(consumerName)) {
                throw new IllegalArgumentException("Consumer Name cannot be set when Ack Behavior is NoAck.");
            }

            ConsumeOptions co = batchSize == -1 && thresholdPercent == -1
                ? ConsumeOptions.DEFAULT_CONSUME_OPTIONS
                : ConsumeOptions.builder().batchSize(batchSize).thresholdPercent(thresholdPercent).build();
            return new JetStreamSubjectConfiguration(this, co);
        }
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof JetStreamSubjectConfiguration)) return false;
        return id.equals(((JetStreamSubjectConfiguration) o).id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
