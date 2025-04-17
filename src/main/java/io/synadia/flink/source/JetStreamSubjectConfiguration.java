// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.ConsumeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.support.*;
import io.synadia.flink.utils.MiscUtils;
import io.synadia.flink.utils.YamlUtils;
import org.apache.flink.api.connector.source.Boundedness;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.nats.client.BaseConsumeOptions.*;
import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValue.EMPTY_MAP;
import static io.synadia.flink.utils.MiscUtils.checksum;
import static io.synadia.flink.utils.PropertyConstants.*;

/**
 * It takes more than a subject to consume.
 * This tells us the way to start consuming.
 */
public class JetStreamSubjectConfiguration implements JsonSerializable, Serializable {
    private static final long serialVersionUID = 1L;

    public final String id;
    public final String streamName;
    public final String subject;
    public final Long startSequence;
    public final ZonedDateTime startTime;
    public final long maxMessagesToRead;
    public final boolean ack;
    public final SerializableConsumeOptions serializableConsumeOptions;

    public final Boundedness boundedness;
    public final DeliverPolicy deliverPolicy;

    private JetStreamSubjectConfiguration(Builder b, String subject) {
        this.subject = subject;
        streamName = b.streamName;
        startSequence = b.startSequence;
        startTime = b.startTime;
        maxMessagesToRead = b.maxMessagesToRead;
        ack = b.ack;
        serializableConsumeOptions = b.serializableConsumeOptions;

        boundedness = maxMessagesToRead > 0 ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
        deliverPolicy = startSequence != ConsumerConfiguration.LONG_UNSET
            ? DeliverPolicy.ByStartSequence
            : startTime != null
                ? DeliverPolicy.ByStartTime
                : null;

        id = checksum(subject,
            streamName,
            startSequence,
            startTime,
            maxMessagesToRead,
            ack,
            serializableConsumeOptions == null ? null : serializableConsumeOptions.getConsumeOptions().toJson()
        );
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, STREAM_NAME, streamName);
        JsonUtils.addField(sb, SUBJECT, subject);
        JsonUtils.addField(sb, START_SEQ, startSequence);
        JsonUtils.addField(sb, START_TIME, startTime);
        if (serializableConsumeOptions != null) {
            ConsumeOptions co = serializableConsumeOptions.getConsumeOptions();
            JsonValueUtils.MapBuilder bm = JsonValueUtils.mapBuilder();
            if (co.getBatchSize() != DEFAULT_MESSAGE_COUNT) {
                bm.put(MESSAGES, co.getBatchSize());
            }
            if (co.getBatchBytes() > 0) {
                bm.put(BYTES, co.getBatchBytes());
            }
            if (co.getExpiresInMillis() != DEFAULT_EXPIRES_IN_MILLIS) {
                bm.put(EXPIRES_IN, co.getExpiresInMillis());
            }
            if (co.getThresholdPercent() != DEFAULT_THRESHOLD_PERCENT) {
                bm.put(THRESHOLD_PERCENT, co.getThresholdPercent());
            }
            if (co.getThresholdPercent() != DEFAULT_THRESHOLD_PERCENT) {
                bm.put(THRESHOLD_PERCENT, co.getThresholdPercent());
            }
            if (co.getGroup() != null) {
                bm.put(GROUP, co.getGroup());
            }
            if (co.getMinPending() > 0) {
                bm.put(MIN_PENDING, co.getMinPending());
            }
            if (co.getMinPending() > 0) {
                bm.put(MIN_ACK_PENDING, co.getMinAckPending());
            }
            if (co.raiseStatusWarnings()) {
                bm.put(RAISE_STATUS_WARNINGS, true);
            }
            JsonUtils.addField(sb, CONSUME_OPTIONS, bm.jv);
        }
        JsonUtils.addField(sb, MAX_MSGS, maxMessagesToRead);
        JsonUtils.addFldWhenTrue(sb, ACK, ack);
        return endJson(sb).toString();
    }

    public String toYaml(int indentLevel) {
        StringBuilder sb = YamlUtils.beginChild(indentLevel, STREAM_NAME, streamName);
        indentLevel++;
        YamlUtils.addField(sb, indentLevel, SUBJECT, subject);
        YamlUtils.addField(sb, indentLevel, START_SEQ, startSequence);
        YamlUtils.addField(sb, indentLevel, START_TIME, startTime);
        if (serializableConsumeOptions != null) {
            ConsumeOptions co = serializableConsumeOptions.getConsumeOptions();
            YamlUtils.addField(sb, indentLevel, CONSUME_OPTIONS);
            int coIndent = indentLevel + 1;
            if (co.getBatchSize() != DEFAULT_MESSAGE_COUNT) {
                YamlUtils.addFieldGtZero(sb, coIndent, MESSAGES, co.getBatchSize());
            }
            YamlUtils.addFieldGtZero(sb, coIndent, BYTES, co.getBatchBytes());
            if (co.getExpiresInMillis() != DEFAULT_EXPIRES_IN_MILLIS) {
                YamlUtils.addField(sb, coIndent, EXPIRES_IN, co.getExpiresInMillis());
            }
            if (co.getThresholdPercent() != DEFAULT_THRESHOLD_PERCENT) {
                YamlUtils.addField(sb, coIndent, THRESHOLD_PERCENT, co.getThresholdPercent());
            }
            YamlUtils.addField(sb, coIndent, GROUP, co.getGroup());
            YamlUtils.addField(sb, coIndent, MIN_PENDING, co.getMinPending());
            YamlUtils.addField(sb, coIndent, MIN_ACK_PENDING, co.getMinAckPending());
            YamlUtils.addFldWhenTrue(sb, coIndent, RAISE_STATUS_WARNINGS, co.raiseStatusWarnings());
        }
        YamlUtils.addField(sb, indentLevel, MAX_MSGS, maxMessagesToRead);
        YamlUtils.addFldWhenTrue(sb, indentLevel, ACK, ack);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JetStreamSubjectConfiguration)) return false;
        JetStreamSubjectConfiguration that = (JetStreamSubjectConfiguration) o;
        return id.equals(that.id); // id is a checksum
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static JetStreamSubjectConfiguration fromJson(String json) throws JsonParseException {
        return fromJsonValue(JsonParser.parse(json));
    }

    public static JetStreamSubjectConfiguration fromJsonValue(JsonValue jv) {
        Builder b = new Builder()
            .streamName(JsonValueUtils.readString(jv, STREAM_NAME))
            .startSequence(JsonValueUtils.readLong(jv, START_SEQ, ConsumerConfiguration.LONG_UNSET))
            .startTime(JsonValueUtils.readDate(jv, START_TIME))
            .maxMessagesToRead(JsonValueUtils.readLong(jv, MAX_MSGS, -1))
            .ack(JsonValueUtils.readBoolean(jv, ACK, false));

        JsonValue jvCo = JsonValueUtils.readObject(jv, CONSUME_OPTIONS);
        if (jvCo != null && jvCo != EMPTY_MAP) {
            b.consumeOptions(ConsumeOptions.builder().jsonValue(jvCo).build());
        }

        String subject = JsonValueUtils.readString(jv, SUBJECT);
        return new JetStreamSubjectConfiguration(b, subject);
    }

    public static JetStreamSubjectConfiguration fromMap(Map<String, Object> map) {
        Builder b = new Builder()
            .streamName(YamlUtils.readString(map, STREAM_NAME))
            .startSequence(YamlUtils.readLong(map, START_SEQ, ConsumerConfiguration.LONG_UNSET))
            .startTime(YamlUtils.readDate(map, START_TIME))
            .maxMessagesToRead(YamlUtils.readLong(map, MAX_MSGS, -1))
            .ack(YamlUtils.readBoolean(map, ACK, false));

        Map<String, Object> mapCo = YamlUtils.readMap(map, CONSUME_OPTIONS);
        if (mapCo != null) {
            ConsumeOptions.Builder cob = ConsumeOptions.builder()
                .expiresIn(YamlUtils.readLong(mapCo, EXPIRES_IN, DEFAULT_EXPIRES_IN_MILLIS))
                .thresholdPercent(YamlUtils.readInteger(mapCo, THRESHOLD_PERCENT, -1))
                .raiseStatusWarnings(YamlUtils.readBoolean(mapCo, RAISE_STATUS_WARNINGS, false))
                .group(YamlUtils.readStringEmptyAsNull(mapCo, GROUP))
                .minPending(YamlUtils.readLong(mapCo, MIN_PENDING, -1))
                .minAckPending(YamlUtils.readLong(mapCo, MIN_ACK_PENDING, -1));

            int i = YamlUtils.readInteger(mapCo, MESSAGES, -1);
            if (i != -1) {
                cob.batchSize(i);
            }
            else {
                i = YamlUtils.readInteger(mapCo, BYTES, -1);
                if (i != -1) {
                    cob.batchBytes(i);
                }
            }
            b.consumeOptions(cob.build());
        }

        String subject = YamlUtils.readString(map, SUBJECT);
        return new JetStreamSubjectConfiguration(b, subject);
    }

    public static class Builder {
        private String streamName;
        private Long startSequence = ConsumerConfiguration.LONG_UNSET;
        private ZonedDateTime startTime;
        private SerializableConsumeOptions serializableConsumeOptions;
        private long maxMessagesToRead = -1;
        private boolean ack = false;

        public Builder streamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        /**
         * Sets the start sequence of the JetStreamSubjectConfiguration.
         * @param startSequence the start sequence
         * @return The Builder
         */
        public Builder startSequence(Long startSequence) {
            if (startSequence < 1) {
                this.startSequence = ConsumerConfiguration.LONG_UNSET;
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
         * @return The Builder
         */
        public Builder startTime(ZonedDateTime startTime) {
            if (startTime != null && startSequence != ConsumerConfiguration.LONG_UNSET) {
                throw new IllegalArgumentException("Cannot set both start sequence and start time.");
            }
            this.startTime = startTime;
            return this;
        }

        /**
         * Set the consume options for finer control of the simplified consume
         * @param consumeOptions the consume options
         * @return The Builder
         */
        public Builder consumeOptions(ConsumeOptions consumeOptions) {
            this.serializableConsumeOptions = consumeOptions == null
                ? null
                : new SerializableConsumeOptions(consumeOptions);
            return this;
        }

        /**
         * Set the maximum number of messages to read.
         * This makes this configuration Boundedness BOUNDED if the value is greater than zero.
         * @return The Builder
         */
        public Builder maxMessagesToRead(long maxMessagesToRead) {
            this.maxMessagesToRead = maxMessagesToRead < 1 ? -1 : maxMessagesToRead;
            return this;
        }

        /**
         * Set whether to ack messages. If acking is on,
         * ack will occur when a checkpoint is complete via ack all
         * It's not recommend to set ack unless your stream is a workqueue
         * but even then, be sure of why you are running this against a workqueue
         * @param ack whether to ack or not
         * @return The Builder
         */
        public Builder ack(boolean ack) {
            this.ack = ack;
            return this;
        }

        public JetStreamSubjectConfiguration buildWithSubject(String subject) {
            if (MiscUtils.notProvided(subject)) {
                throw new IllegalArgumentException("Subject is required.");
            }
            if (MiscUtils.notProvided(streamName)) {
                throw new IllegalArgumentException("Stream name is required.");
            }
            return new JetStreamSubjectConfiguration(this, subject);
        }

        public List<JetStreamSubjectConfiguration> buildWithSubjects(String... subjects) {
            if (subjects == null || subjects.length == 0) {
                throw new IllegalArgumentException("Subjects are required.");
            }
            List<JetStreamSubjectConfiguration> list = new ArrayList<>();
            for (String subject : subjects) {
                list.add(buildWithSubject(subject));
            }
            return list;
        }

        public List<JetStreamSubjectConfiguration> buildWithSubjects(List<String> subjects) {
            if (subjects == null || subjects.isEmpty()) {
                throw new IllegalArgumentException("Subjects are required.");
            }
            List<JetStreamSubjectConfiguration> list = new ArrayList<>();
            for (String subject : subjects) {
                list.add(buildWithSubject(subject));
            }
            return list;
        }
    }
}
