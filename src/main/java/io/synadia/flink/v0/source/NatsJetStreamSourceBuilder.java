// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source;

import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.utils.BuilderBase;
import io.synadia.flink.utils.Constants;
import io.synadia.flink.utils.PropertiesUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static io.nats.client.support.ApiConstants.STREAM_NAME;

public class NatsJetStreamSourceBuilder<OutputT> extends BuilderBase<OutputT, NatsJetStreamSourceBuilder<OutputT>> {
    public static final String CONSUMER_NAME = "consumer_name";

    long DEFAULT_FETCH_ONE_MESSAGE_TIMEOUT_MS = 1000;
    long DEFAULT_FETCH_TIMEOUT_MS = 1000;
    long DEFAULT_AUTO_ACK_INTERVAL_MS = 5000;
    int DEFAULT_MAX_FETCH_RECORDS = 100;

    private String consumerName;
    private String streamName;
    private int messageQueueCapacity = SourceReaderOptions.ELEMENT_QUEUE_CAPACITY.defaultValue();
    private boolean enableAutoAcknowledgeMessage;
    private Duration fetchOneMessageTimeout = Duration.ofMillis(DEFAULT_FETCH_ONE_MESSAGE_TIMEOUT_MS);
    private Duration fetchTimeout = Duration.ofMillis(DEFAULT_FETCH_TIMEOUT_MS);
    private int maxFetchRecords = DEFAULT_MAX_FETCH_RECORDS;
    private Duration autoAckInterval = Duration.ofMillis(DEFAULT_AUTO_ACK_INTERVAL_MS);
    private Boundedness boundedness = Boundedness.CONTINUOUS_UNBOUNDED;

    public NatsJetStreamSourceBuilder() {
        super(true, false);
    }

    /**
     * Set source properties from a properties object
     * See the readme and {@link Constants} for property keys
     * @param properties the properties object
     * @return the builder
     */
    public NatsJetStreamSourceBuilder<OutputT> sourceProperties(Properties properties) {
        setBaseProperties(k -> PropertiesUtils.getStringProperty(properties, k));
        String name = properties.getProperty(STREAM_NAME);
        if (name != null) {
            streamName(name);
        }
        name = properties.getProperty(CONSUMER_NAME);
        if (name != null) {
            consumerName(name);
        }
        return this;
    }

    /**
     * Set one or more subjects for the source. Replaces all subjects previously set in the builder.
     * @param subjects the subjects
     * @return the builder
     */
    public NatsJetStreamSourceBuilder<OutputT> subject(String... subjects) {
        return super._subjects(subjects);
    }

    /**
     * Set the subjects for the source. Replaces all subjects previously set in the builder.
     * @param subjects the list of subjects
     * @return the builder
     */
    public NatsJetStreamSourceBuilder<OutputT> subject(List<String> subjects) {
        return super._subjects(subjects);
    }

    /**
     * Set the payload deserializer for the source.
     * @param payloadDeserializer the deserializer.
     * @return the builder
     */
    public NatsJetStreamSourceBuilder<OutputT> payloadDeserializer(PayloadDeserializer<OutputT> payloadDeserializer) {
        return super._payloadDeserializer(payloadDeserializer);
    }

    /**
     * Set the fully qualified name of the desired class payload deserializer for the source.
     * @param payloadDeserializerClass the serializer class name.
     * @return the builder
     */
    public NatsJetStreamSourceBuilder<OutputT> payloadDeserializerClass(String payloadDeserializerClass) {
        return super._payloadDeserializerClass(payloadDeserializerClass);
    }

    @Override
    protected NatsJetStreamSourceBuilder<OutputT> getThis() {
        return this;
    }

    public NatsJetStreamSourceBuilder<OutputT> messageQueueCapacity(int messageQueueCapacity) {
        this.messageQueueCapacity = messageQueueCapacity;
        return this;
    }

    public NatsJetStreamSourceBuilder<OutputT> enableAutoAcknowledgeMessage(boolean enableAutoAcknowledgeMessage) {
        this.enableAutoAcknowledgeMessage = enableAutoAcknowledgeMessage;
        return this;
    }

    public NatsJetStreamSourceBuilder<OutputT> fetchOneMessageTime(Duration fetchOneMessageTime) {
        this.fetchOneMessageTimeout = fetchOneMessageTime;
        return this;
    }

    public NatsJetStreamSourceBuilder<OutputT> maxFetchTime(Duration maxFetchTime) {
        this.fetchTimeout = maxFetchTime;
        return this;
    }

    public NatsJetStreamSourceBuilder<OutputT> maxFetchRecords(int maxFetchRecords) {
        this.maxFetchRecords = maxFetchRecords;
        return this;
    }

    public NatsJetStreamSourceBuilder<OutputT> natsAutoAckInterval(Duration natsAutoAckInterval) {
        this.autoAckInterval = natsAutoAckInterval;
        return this;
    }

    public NatsJetStreamSourceBuilder<OutputT> streamName(String streamName) {
        this.streamName = streamName;
        return this;
    }

    public NatsJetStreamSourceBuilder<OutputT> consumerName(String consumerName) {
        this.consumerName = consumerName;
        return this;
    }

    public NatsJetStreamSourceBuilder<OutputT> boundness(Boundedness boundedness){
        this.boundedness = boundedness;
        return this;
    }


    /**
     * Build a NatsJetStreamSource.
     * @return the source
     */
    public NatsJetStreamSource<OutputT> build() {
        // Validate consumer name
        if (consumerName == null || consumerName.isEmpty()) {
            throw new IllegalArgumentException("Consumer name must be provided.");
        }

        // Validate stream name
        if (streamName == null || streamName.isEmpty()) {
            throw new IllegalArgumentException("Stream name must be provided.");
        }

        // Validate auto ack interval when enabled
        if (enableAutoAcknowledgeMessage &&
            (autoAckInterval == null || autoAckInterval.isZero() || autoAckInterval.isNegative())) {
            throw new IllegalArgumentException("Auto acknowledge interval must be positive when auto acknowledge is enabled");
        }

        // Validate max fetch records
        if (maxFetchRecords <= 0) {
            throw new IllegalArgumentException("Maximum fetch records must be positive");
        }

        // Validate message queue capacity
        if (messageQueueCapacity <= 0) {
            throw new IllegalArgumentException("Message queue capacity must be positive");
        }

        // Add validation for fetch timeouts
        if (fetchOneMessageTimeout == null || fetchOneMessageTimeout.isNegative()) {
            throw new IllegalArgumentException("Fetch timeout must be non-negative");
        }

        if (fetchTimeout == null || fetchTimeout.isNegative()) {
            throw new IllegalArgumentException("Max fetch time must be non-negative");
        }

        beforeBuild();

        return new NatsJetStreamSource<>(
            payloadDeserializer,
            connectionFactory,
            subjects,
            new NatsJetStreamSourceConfiguration(streamName,
                consumerName,
                messageQueueCapacity,
                enableAutoAcknowledgeMessage,
                fetchOneMessageTimeout,
                fetchTimeout,
                maxFetchRecords,
                autoAckInterval, boundedness));
    }
}