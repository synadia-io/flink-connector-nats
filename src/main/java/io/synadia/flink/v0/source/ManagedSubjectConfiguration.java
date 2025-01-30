// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source;

import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import io.synadia.flink.v0.utils.MiscUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValueUtils.*;
import static io.synadia.flink.v0.utils.ManagedUtils.toSplitId;

/**
 * It takes more than a subject to consume.
 * This tells us the way to start consuming.
 */
public class ManagedSubjectConfiguration implements JsonSerializable {

    public final String streamName;
    public final String subject;
    public final DeliverPolicy deliverPolicy;
    public final Long startSequence;
    public final ZonedDateTime startTime;
    public final ManagedConsumeOptions managedConsumeOptions;
    public final String configId;

    private ManagedSubjectConfiguration(Builder b) {
        this.streamName = b.streamName;
        this.subject = b.subject;
        this.deliverPolicy = b.deliverPolicy;
        this.startSequence = b.startSequence;
        this.startTime = b.startTime;
        this.managedConsumeOptions = b.managedConsumeOptions;
        if (MiscUtils.provided(b.configId)) {
            this.configId = b.configId;
        }
        else {
            this.configId = toSplitId(streamName, subject, deliverPolicy, startSequence, startTime);
        }
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
        return endJson(sb).toString();
    }

    public String getStreamName() {
        return streamName;
    }

    public String getSubject() {
        return subject;
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

    public ManagedConsumeOptions getManagedConsumeOptions() {
        return managedConsumeOptions;
    }

    public String getConfigId() {
        return configId;
    }

    public static class Builder {
        private String configId;
        private String streamName;
        private String subject;
        private DeliverPolicy deliverPolicy;
        private Long startSequence = ConsumerConfiguration.LONG_UNSET;
        private ZonedDateTime startTime;
        private ManagedConsumeOptions managedConsumeOptions;

        public Builder jsonValue(JsonValue jv) {
            configId(JsonValueUtils.readString(jv, ID));

            String temp = JsonValueUtils.readString(jv, STREAM_NAME);
            if (temp != null) { // stream name is required
                streamName(temp);
                temp = JsonValueUtils.readString(jv, SUBJECT);
                if (temp != null) { // subject is required
                    subject(temp);

                    temp = readString(jv, DELIVER_POLICY); // not required
                    if (temp != null) {
                        deliverPolicy(DeliverPolicy.get(temp));
                    }

                    startSequence(readLong(jv, OPT_START_SEQ, ConsumerConfiguration.LONG_UNSET));
                    startTime(readDate(jv, OPT_START_TIME));
                }
                return this;
            }
            throw new FlinkRuntimeException("Invalid ManagedSubjectConfiguration Json: " + jv.toJson());
        }

        public Builder configId(String configId) {
            this.configId = configId;
            return this;
        }

        public Builder streamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public Builder subject(String subject) {
            this.subject = subject;
            return this;
        }

        /**
         * Sets the delivery policy of the ManagedSubjectConfiguration.
         *
         * @param deliverPolicy the delivery policy.
         * @return Builder
         */
        public Builder deliverPolicy(DeliverPolicy deliverPolicy) {
            this.deliverPolicy = deliverPolicy;
            return this;
        }

        /**
         * Sets the start sequence of the ManagedSubjectConfiguration.
         *
         * @param startSequence the start sequence
         * @return Builder
         */
        public Builder startSequence(Long startSequence) {
            this.startSequence = startSequence < 1 ? ConsumerConfiguration.LONG_UNSET : startSequence;
            return this;
        }

        public Builder managedConsumeOptions(ManagedConsumeOptions managedConsumeOptions) {
            this.managedConsumeOptions = managedConsumeOptions;
            return this;
        }

        /**
         * Sets the start time of the ManagedSubjectConfiguration.
         *
         * @param startTime the start time
         * @return Builder
         */
        public Builder startTime(ZonedDateTime startTime) {
            this.startTime = startTime;
            return this;
        }

        public ManagedSubjectConfiguration build() {
            return new ManagedSubjectConfiguration(this);
        }
    }
}
