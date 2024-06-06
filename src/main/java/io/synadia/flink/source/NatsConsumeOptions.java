// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;
import java.io.Serializable;
import java.time.Duration;

public class NatsConsumeOptions implements Serializable {

    private final String consumerName;
    private final String streamName;
    private final int batchSize;
    private final Duration maxWait;

    private NatsConsumeOptions(Builder builder) {
        this.consumerName = builder.consumerName;
        this.streamName = builder.streamName;
        this.batchSize = builder.batchSize;
        this.maxWait = builder.maxWait;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public String getStreamName() {
        return streamName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Duration getMaxWait() {
        return maxWait;
    }

    public static class Builder {
        private String consumerName;
        private String streamName;
        private int batchSize;
        private Duration maxWait;

        public Builder consumer(String consumerName) {
            this.consumerName = consumerName;
            return this;
        }

        public Builder stream(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder maxWait(Duration maxWait) {
            this.maxWait = maxWait;
            return this;
        }

        public NatsConsumeOptions build() {
            return new NatsConsumeOptions(this);
        }
    }
}
