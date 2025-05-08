// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.BaseConsumeOptions;
import io.nats.client.ConsumeOptions;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.support.*;
import io.synadia.flink.utils.MiscUtils;
import io.synadia.flink.utils.YamlUtils;
import org.apache.flink.api.connector.source.Boundedness;

import java.io.Serializable;
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
    public final long startSequence;
    public final ZonedDateTime startTime;
    public final long maxMessagesToRead;
    public final boolean ackMode;
    public final SerializableConsumeOptions serializableConsumeOptions;

    public final Boundedness boundedness;
    public final DeliverPolicy deliverPolicy;

    private JetStreamSubjectConfiguration(Builder b, ConsumeOptions consumeOptions) {
        serializableConsumeOptions = new SerializableConsumeOptions(consumeOptions);
        subject = b.subject;
        streamName = b.streamName;
        startSequence = b.startSequence;
        startTime = b.startTime;
        maxMessagesToRead = b.maxMessagesToRead;
        ackMode = b.ackMode;

        boundedness = maxMessagesToRead > 0 ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
        deliverPolicy = startSequence != -1
            ? DeliverPolicy.ByStartSequence
            : startTime != null
                ? DeliverPolicy.ByStartTime
                : null;

        id = checksum(subject,
            streamName,
            startSequence,
            startTime,
            maxMessagesToRead,
            ackMode,
            serializableConsumeOptions.getConsumeOptions().toJson()
        );
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, STREAM_NAME, streamName);
        JsonUtils.addField(sb, SUBJECT, subject);
        JsonUtils.addField(sb, START_SEQUENCE, startSequence);
        JsonUtils.addField(sb, START_TIME, startTime);
        JsonUtils.addField(sb, MAX_MESSAGES_TO_READ, maxMessagesToRead);
        JsonUtils.addFldWhenTrue(sb, ACK_MODE, ackMode);
        ConsumeOptions co = serializableConsumeOptions.getConsumeOptions();
        if (co.getBatchSize() != DEFAULT_MESSAGE_COUNT) {
            JsonUtils.addField(sb, BATCH_SIZE, co.getBatchSize());
        }
        if (co.getThresholdPercent() != DEFAULT_THRESHOLD_PERCENT) {
            JsonUtils.addField(sb, THRESHOLD_PERCENT, co.getThresholdPercent());
        }
        return endJson(sb).toString();
    }

    public String toYaml(int indentLevel) {
        StringBuilder sb = YamlUtils.beginChild(indentLevel, STREAM_NAME, streamName);
        indentLevel++;
        YamlUtils.addField(sb, indentLevel, SUBJECT, subject);
        YamlUtils.addField(sb, indentLevel, START_SEQUENCE, startSequence);
        YamlUtils.addField(sb, indentLevel, START_TIME, startTime);
        YamlUtils.addField(sb, indentLevel, MAX_MESSAGES_TO_READ, maxMessagesToRead);
        YamlUtils.addFldWhenTrue(sb, indentLevel, ACK_MODE, ackMode);
        ConsumeOptions co = serializableConsumeOptions.getConsumeOptions();
        if (co.getBatchSize() != DEFAULT_MESSAGE_COUNT) {
            YamlUtils.addFieldGtZero(sb, indentLevel, BATCH_SIZE, co.getBatchSize());
        }
        if (co.getThresholdPercent() != DEFAULT_THRESHOLD_PERCENT) {
            YamlUtils.addField(sb, indentLevel, THRESHOLD_PERCENT, co.getThresholdPercent());
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
            .startSequence(JsonValueUtils.readLong(jv, START_SEQUENCE, -1))
            .startTime(JsonValueUtils.readDate(jv, START_TIME))
            .maxMessagesToRead(JsonValueUtils.readLong(jv, MAX_MESSAGES_TO_READ, -1))
            .batchSize(JsonValueUtils.readInteger(jv, BATCH_SIZE, -1))
            .thresholdPercent(JsonValueUtils.readInteger(jv, THRESHOLD_PERCENT, -1))
            .ackMode(JsonValueUtils.readBoolean(jv, ACK_MODE, false))
            .build();
    }

    public static JetStreamSubjectConfiguration fromMap(Map<String, Object> map) {
        return new Builder()
            .streamName(YamlUtils.readString(map, STREAM_NAME))
            .subject(YamlUtils.readString(map, SUBJECT))
            .startSequence(YamlUtils.readLong(map, START_SEQUENCE, -1))
            .startTime(YamlUtils.readDate(map, START_TIME))
            .maxMessagesToRead(YamlUtils.readLong(map, MAX_MESSAGES_TO_READ, -1))
            .batchSize(YamlUtils.readInteger(map, BATCH_SIZE, -1))
            .thresholdPercent(YamlUtils.readInteger(map, THRESHOLD_PERCENT, -1))
            .ackMode(YamlUtils.readBoolean(map, ACK_MODE, false))
            .build();
    }

    public static class Builder {
        private String streamName;
        private String subject;
        private long startSequence = -1;
        private ZonedDateTime startTime;
        private long maxMessagesToRead = -1;
        private boolean ackMode = false;
        private int batchSize = -1;
        private int thresholdPercent = -1;

        /**
         * Copies all the configuration except the subject
         * @param config the config to use as a basis for the new config
         * @return the builder
         */
        public Builder copy(JetStreamSubjectConfiguration config) {
            return streamName(config.streamName)
                .startSequence(config.startSequence)
                .startTime(config.startTime)
                .maxMessagesToRead(config.maxMessagesToRead)
                .ackMode(config.ackMode)
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
         * @return the builder
         */
        public Builder maxMessagesToRead(long maxMessagesToRead) {
            this.maxMessagesToRead = maxMessagesToRead < 1 ? -1 : maxMessagesToRead;
            return this;
        }

        /**
         * Set that the stream is a work queue and messages must be acked.
         * Ack will occur when a checkpoint is complete via ack all.
         * It's not recommended to set ackMode unless your stream is a work queue,
         * but even then, be sure of why you are running this against a work queue.
         * @return the builder
         */
        public Builder ackMode() {
            this.ackMode = true;
            return this;
        }

        /**
         * Set whether to be in ack mode queue where must be acked.
         * Ack will occur when a checkpoint is complete via ack all.
         * This is MUCH slower and is not recommended to set ackMode
         * unless your stream is a work queue, but even then,
         * be sure of why you are running this against a work queue.
         */
        public Builder ackMode(boolean ackMode) {
            this.ackMode = ackMode;
            return this;
        }

        public JetStreamSubjectConfiguration build() {
            if (MiscUtils.notProvided(subject)) {
                throw new IllegalArgumentException("Subject is required.");
            }
            if (MiscUtils.notProvided(streamName)) {
                throw new IllegalArgumentException("Stream name is required.");
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
