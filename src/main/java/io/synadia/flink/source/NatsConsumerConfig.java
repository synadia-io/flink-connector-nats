// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;
import java.io.Serializable;

public class NatsConsumerConfig implements Serializable {

    private final String consumerName;
    private final int batchSize;

    private NatsConsumerConfig(Builder builder) {
        this.consumerName = builder.consumerName;
        this.batchSize = builder.batchSize;

    }

    public String getConsumerName() {
        return consumerName;
    }

    public int getBatchSize() {
        return batchSize;
    }


    public static class Builder {
        private String consumerName;
        private int batchSize;

        public Builder() {
        }

        public Builder withConsumerName(String consumerName) {
            this.consumerName = consumerName;
            return this;
        }

        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public NatsConsumerConfig build() {
            return new NatsConsumerConfig(this);
        }
    }
}
