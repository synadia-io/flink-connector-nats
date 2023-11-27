package io.synadia.flink.source;
import java.io.Serializable;

public class NATSConsumerConfig implements Serializable {

    private final String consumerName;
    private final int batchSize;

    private NATSConsumerConfig(Builder builder) {
        this.consumerName = builder.consumerName;
        this.batchSize = builder.batchSize;

    }

    public String getConsumerName() {
        return consumerName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isCumulativeAck() {
        return true;
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

        public NATSConsumerConfig build() {
            return new NATSConsumerConfig(this);
        }
    }
}