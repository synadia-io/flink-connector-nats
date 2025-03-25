// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.ConsumeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.support.*;
import io.synadia.flink.utils.MiscUtils;
import org.apache.flink.api.connector.source.Boundedness;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.ConsumeOptions.DEFAULT_CONSUME_OPTIONS;
import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValueUtils.*;
import static io.synadia.flink.utils.ManagedUtils.toSplitId;

/**
 * It takes more than a subject to consume.
 * This tells us the way to start consuming.
 */
public class JetStreamSubjectConfiguration implements JsonSerializable, Serializable {
    private static final long serialVersionUID = 1L;
    private static final String CONSUME_OPTIONS = "consume_options";

    public final String configId;
    public final String streamName;
    public final String subject;
    public final DeliverPolicy deliverPolicy;
    public final Long startSequence;
    public final ZonedDateTime startTime;
    public final SerializableConsumeOptions consumeOptions;
    public final long maxMessagesToRead;
    public final Boundedness boundedness;

    private JetStreamSubjectConfiguration(Builder b, String subject, String configId) {
        this.configId = configId;
        this.subject = subject;
        streamName = b.streamName;
        deliverPolicy = b.deliverPolicy;
        startSequence = b.startSequence;
        startTime = b.startTime;
        consumeOptions = b.consumeOptions;
        maxMessagesToRead = b.maxMessagesToRead;
        boundedness = maxMessagesToRead > 0 ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, ID, configId);
        JsonUtils.addField(sb, STREAM_NAME, streamName);
        JsonUtils.addField(sb, SUBJECT, subject);
        if (deliverPolicy != null) {
            JsonUtils.addField(sb, DELIVER_POLICY, deliverPolicy.toString());
        }
        JsonUtils.addField(sb, OPT_START_SEQ, startSequence);
        JsonUtils.addField(sb, OPT_START_TIME, startTime);
        JsonUtils.addField(sb, CONSUME_OPTIONS, consumeOptions.getConsumeOptions().toJsonValue());
        JsonUtils.addField(sb, MAX_MSGS, maxMessagesToRead);
        return endJson(sb).toString();
    }

    @Override
    public String toString() {
        return toJson();
    }

    public String getConfigId() {
        return configId;
    }

    public String getSubject() {
        return subject;
    }

    public String getStreamName() {
        return streamName;
    }

    public DeliverPolicy getDeliverPolicy() {
        return deliverPolicy;
    }

    public Long getStartSequence() {
        return startSequence;
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }

    public Boundedness getBoundedness() {
        return boundedness;
    }

    public SerializableConsumeOptions getConsumeOptions() {
        return consumeOptions;
    }

    public long getMaxMessagesToRead() {
        return maxMessagesToRead;
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
            .startSequence(readLong(jv, OPT_START_SEQ, ConsumerConfiguration.LONG_UNSET))
            .startTime(readDate(jv, OPT_START_TIME))
            .maxMessagesToRead(readLong(jv, MAX_MSGS, -1))
            ;

        String temp = readString(jv, DELIVER_POLICY); // not required
        if (temp != null) {
            b.deliverPolicy(DeliverPolicy.get(temp));
        }

        JsonValue jvCo = readObject(jv, CONSUME_OPTIONS);
        if (jvCo != null) {
            b.consumeOptions(ConsumeOptions.builder().jsonValue(jvCo).build());
        }

        String subject = JsonValueUtils.readString(jv, SUBJECT);
        String configId = JsonValueUtils.readString(jv, ID);
        return new JetStreamSubjectConfiguration(b, subject, configId);
    }

    public static class Builder {
        private String streamName;
        private DeliverPolicy deliverPolicy;
        private Long startSequence = ConsumerConfiguration.LONG_UNSET;
        private ZonedDateTime startTime;
        private SerializableConsumeOptions consumeOptions;
        private long maxMessagesToRead = -1;

        public Builder() {
            consumeOptions(DEFAULT_CONSUME_OPTIONS);
        }

        public Builder streamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        /**
         * Sets the delivery policy of the JetStreamSubjectConfiguration.
         * @param deliverPolicy the delivery policy.
         * @return The Builder
         */
        public Builder deliverPolicy(DeliverPolicy deliverPolicy) {
            this.deliverPolicy = deliverPolicy;
            return this;
        }

        /**
         * Sets the start sequence of the JetStreamSubjectConfiguration.
         * @param startSequence the start sequence
         * @return The Builder
         */
        public Builder startSequence(Long startSequence) {
            this.startSequence = startSequence < 1 ? ConsumerConfiguration.LONG_UNSET : startSequence;
            return this;
        }

        /**
         * Sets the start time of the JetStreamSubjectConfiguration.
         * @param startTime the start time
         * @return The Builder
         */
        public Builder startTime(ZonedDateTime startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Set the consume options for finer control of the simplified consume
         * @param consumeOptions the consume options
         * @return The Builder
         */
        public Builder consumeOptions(ConsumeOptions consumeOptions) {
            this.consumeOptions = consumeOptions == null
                ? new SerializableConsumeOptions(DEFAULT_CONSUME_OPTIONS)
                : new SerializableConsumeOptions(consumeOptions);
            return this;
        }

        /**
         * Set the maximum number of records to read.
         * This makes this configuration Boundedness BOUNDED if the value is greater than zero.
         * @return The Builder
         */
        public Builder maxMessagesToRead(long maxMessagesToRead) {
            this.maxMessagesToRead = maxMessagesToRead < 1 ? -1 : maxMessagesToRead;
            return this;
        }

        public JetStreamSubjectConfiguration buildWithSubject(String subject) {
            if (MiscUtils.notProvided(subject)) {
                throw new IllegalStateException("Subject is required.");
            }
            if (MiscUtils.notProvided(streamName)) {
                throw new IllegalStateException("Stream name is required.");
            }
            String configId = toSplitId(streamName, subject, deliverPolicy, startSequence, startTime, maxMessagesToRead);
            return new JetStreamSubjectConfiguration(this, subject, configId);
        }

        public List<JetStreamSubjectConfiguration> buildWithSubjects(String... subjects) {
            if (subjects == null || subjects.length == 0) {
                throw new IllegalStateException("Subjects are required.");
            }
            List<JetStreamSubjectConfiguration> list = new ArrayList<>();
            for (String subject : subjects) {
                list.add(buildWithSubject(subject));
            }
            return list;
        }

        public List<JetStreamSubjectConfiguration> buildWithSubjects(List<String> subjects) {
            if (subjects == null || subjects.isEmpty()) {
                throw new IllegalStateException("Subjects are required.");
            }
            List<JetStreamSubjectConfiguration> list = new ArrayList<>();
            for (String subject : subjects) {
                list.add(buildWithSubject(subject));
            }
            return list;
        }
    }
}
