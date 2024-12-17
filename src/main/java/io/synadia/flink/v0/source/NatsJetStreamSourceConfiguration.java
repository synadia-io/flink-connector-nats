// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;

import java.io.Serializable;
import java.time.Duration;

public class NatsJetStreamSourceConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String consumerName;
    private final int messageQueueCapacity;
    private final boolean enableAutoAcknowledgeMessage;
    private final Duration fetchOneMessageTimeout;
    private final Duration fetchTimeout;
    private final int maxFetchRecords;
    private final Duration autoAckInterval;
    private final Configuration configuration;
    private final Boundedness boundedness;

    NatsJetStreamSourceConfiguration(String consumerName,
                                            int messageQueueCapacity,
                                            boolean enableAutoAcknowledgeMessage,
                                            Duration fetchOneMessageTimeout,
                                            Duration fetchTimeout,
                                            int maxFetchRecords,
                                            Duration autoAckInterval,
                                            Boundedness boundedness) {
        this.consumerName = consumerName;
        this.messageQueueCapacity = messageQueueCapacity;
        this.enableAutoAcknowledgeMessage = enableAutoAcknowledgeMessage;
        this.fetchOneMessageTimeout = fetchOneMessageTimeout;
        this.fetchTimeout = fetchTimeout;
        this.maxFetchRecords = maxFetchRecords;
        this.autoAckInterval = autoAckInterval;
        configuration = new Configuration();
        configuration.setInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY.key(), messageQueueCapacity);
        this.boundedness = boundedness;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public int getMessageQueueCapacity() {
        return messageQueueCapacity;
    }

    public boolean isEnableAutoAcknowledgeMessage() {
        return enableAutoAcknowledgeMessage;
    }

    public Duration getFetchOneMessageTimeout() {
        return fetchOneMessageTimeout;
    }

    public Duration getFetchTimeout() {
        return fetchTimeout;
    }

    public int getMaxFetchRecords() {
        return maxFetchRecords;
    }

    public Duration getAutoAckInterval() {
        return autoAckInterval;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public Boundedness getBoundedness() {
        return boundedness;
    }
}