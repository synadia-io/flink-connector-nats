// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.sink.js;

import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;

import java.io.Serializable;
import java.util.List;

public class NATSStreamConfig implements Serializable {
    private final String streamName;
    private final List<String> subjects;
    private final StorageType storageType;
    private final int replicas;
    private final RetentionPolicy retentionPolicy;

    private NATSStreamConfig(Builder builder) {
        this.streamName = builder.streamName;
        this.subjects = builder.subjects;
        this.storageType = builder.storageType;
        this.replicas = builder.replicas;
        this.retentionPolicy = builder.retentionPolicy;
    }

    // Getters
    public String getStreamName() {
        return streamName;
    }

    public List<String> getSubjects() {
        return subjects;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public int getReplicas() {
        return replicas;
    }

    public RetentionPolicy getRetentionPolicy() {
        return retentionPolicy;
    }

    // Builder class
    public static class Builder {
        private String streamName; // Mandatory
        private List<String> subjects; // Mandatory
        private StorageType storageType = StorageType.File; // Default
        private int replicas = 1; // Default
        private RetentionPolicy retentionPolicy = RetentionPolicy.Limits; // Default

        public Builder withStreamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public Builder withSubjects(List<String> subjects) {
            this.subjects = subjects;
            return this;
        }

        public Builder withStorageType(StorageType storageType) {
            this.storageType = storageType;
            return this;
        }

        public Builder withReplicas(int replicas) {
            this.replicas = replicas;
            return this;
        }

        public Builder withRetentionPolicy(RetentionPolicy retentionPolicy) {
            this.retentionPolicy = retentionPolicy;
            return this;
        }

        public NATSStreamConfig build() {
            if (this.streamName == null || this.streamName.isEmpty()) {
                throw new IllegalStateException("Stream name cannot be null or empty");
            }
            if (this.subjects == null || this.subjects.isEmpty()) {
                throw new IllegalStateException("Subjects cannot be null or empty");
            }
            return new NATSStreamConfig(this);
        }
    }
}

