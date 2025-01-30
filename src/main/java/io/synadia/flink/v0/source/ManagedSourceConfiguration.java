// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source;

import io.synadia.flink.v0.utils.MiscUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.synadia.flink.v0.utils.Constants.DEFAULT_ELEMENT_QUEUE_CAPACITY;

public class ManagedSourceConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_CONSUMER_NAME_PREFIX = "FCNMSC-"; // Flink Connector Nats - Managed Source Configuration

    public final ManagedConsumeOptions defaultManagedConsumeOptions;
    public final Map<String, ManagedSubjectConfiguration> configById;
    public final String consumerNamePrefix;
    public final Configuration configuration;

    private ManagedSourceConfiguration(Builder b) {
        this.defaultManagedConsumeOptions = b.defaultManagedConsumeOptions;
        this.configById = b.configById;
        this.consumerNamePrefix = b.consumerNamePrefix;
        this.configuration = new Configuration().set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, b.messageQueueCapacity);
    }

    public ManagedSubjectConfiguration getManagedSubjectConfigurationById(String configId) {
        return configById.get(configId);
    }

    public static class Builder {
        private int messageQueueCapacity = DEFAULT_ELEMENT_QUEUE_CAPACITY;
        private ManagedConsumeOptions defaultManagedConsumeOptions;
        private Configuration configuration;
        private String consumerNamePrefix;
        private final Map<String, ManagedSubjectConfiguration> configById = new HashMap<>();

        public Builder messageQueueCapacity(int messageQueueCapacity) {
            this.messageQueueCapacity = messageQueueCapacity < 1 ? DEFAULT_ELEMENT_QUEUE_CAPACITY : messageQueueCapacity;
            return this;
        }

        public Builder defaultManagedConsumeOptions(ManagedConsumeOptions defaultManagedConsumeOptions) {
            this.defaultManagedConsumeOptions = defaultManagedConsumeOptions;
            return this;
        }

        public Builder configuration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public Builder consumerNamePrefix(String consumerNamePrefix) {
            this.consumerNamePrefix = consumerNamePrefix;
            return this;
        }

        public Builder subjectConfigurations(List<ManagedSubjectConfiguration> subjectConfigurations) {
            configById.clear();
            if (subjectConfigurations != null && !subjectConfigurations.isEmpty()) {
                for (ManagedSubjectConfiguration subjectConfiguration : subjectConfigurations) {
                    configById.put(subjectConfiguration.configId, subjectConfiguration);
                }
            }
            return this;
        }

        public Builder subjectConfigurations(ManagedSubjectConfiguration... subjectConfigurations) {
            configById.clear();
            if (subjectConfigurations != null) {
                for (ManagedSubjectConfiguration subjectConfiguration : subjectConfigurations) {
                    configById.put(subjectConfiguration.configId, subjectConfiguration);
                }
            }
            return this;
        }

        public Builder addSubjectConfiguration(ManagedSubjectConfiguration subjectConfiguration) {
            if (subjectConfiguration != null) {
                configById.put(subjectConfiguration.configId, subjectConfiguration);
            }
            return this;
        }

        public ManagedSourceConfiguration build() {
            if (configById.isEmpty()) {
                throw new IllegalStateException("At least 1 managed subject configuration is required");
            }
            Boundedness bTarget = defaultManagedConsumeOptions == null ? null : defaultManagedConsumeOptions.getBoundedness();
            for (ManagedSubjectConfiguration msc : configById.values()) {
                Boundedness bMsc = msc.getManagedConsumeOptions() == null ? null : msc.getManagedConsumeOptions().getBoundedness();
                if (bMsc != null) {
                    if (bTarget == null) {
                        bTarget = bMsc;
                    }
                    else if (bMsc != bTarget) {
                        throw new IllegalStateException("All configuration Boundedness must be the same.");
                    }
                }
            }

            if (defaultManagedConsumeOptions == null) {
                defaultManagedConsumeOptions = new ManagedConsumeOptions(bTarget == null ? Boundedness.CONTINUOUS_UNBOUNDED : bTarget);
            }

            if (configuration == null) {
                configuration = new Configuration();
            }
            if (!configuration.contains(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY)) {
                configuration.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, messageQueueCapacity);
            }

            if (MiscUtils.notProvided(consumerNamePrefix)) {
                this.consumerNamePrefix = DEFAULT_CONSUMER_NAME_PREFIX;
            }

            return new ManagedSourceConfiguration(this);
        }
    }
}
