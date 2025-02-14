// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source;

import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.utils.Constants;
import io.synadia.flink.v0.utils.PropertiesUtils;
import org.apache.flink.api.connector.source.Boundedness;

import java.time.Duration;
import java.util.Properties;

import static io.synadia.flink.v0.utils.Constants.*;

public class NatsJetStreamSourceBuilder<OutputT> extends NatsSinkOrSourceBuilder<NatsJetStreamSourceBuilder<OutputT>> {

    private PayloadDeserializer<OutputT> payloadDeserializer;
    private String payloadDeserializerClass;
    private String consumerName;
    private String streamName;
    private int messageQueueCapacity;
    private boolean enableAutoAcknowledgeMessage;
    private Duration fetchOneMessageTimeout;
    private Duration fetchTimeout;
    private int maxFetchRecords;
    private Duration autoAckInterval;
    private Boundedness boundedness;

    public NatsJetStreamSourceBuilder() {
        super(SOURCE_PREFIX);
        messageQueueCapacity = DEFAULT_ELEMENT_QUEUE_CAPACITY;
        enableAutoAcknowledgeMessage = DEFAULT_ENABLE_AUTO_ACK;
        fetchOneMessageTimeout = Duration.ofMillis(DEFAULT_FETCH_ONE_MESSAGE_TIMEOUT_MS);
        fetchTimeout = Duration.ofMillis(DEFAULT_FETCH_TIMEOUT_MS);
        maxFetchRecords = DEFAULT_MAX_FETCH_RECORDS;
        autoAckInterval = Duration.ofMillis(DEFAULT_AUTO_ACK_INTERVAL_MS);
        boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
    }

    /**
     * Set source properties from a properties object
     * See the readme and {@link Constants} for property keys
     * @param properties the properties object
     * @return the builder
     */
    public NatsJetStreamSourceBuilder<OutputT> sourceProperties(Properties properties) {
        baseProperties(properties);

        String s = PropertiesUtils.getStringProperty(properties, PAYLOAD_DESERIALIZER, prefixes);
        if (s != null) {
            payloadDeserializerClass(s);
        }

        return this;
    }

    /**
     * Set the payload deserializer for the source.
     * @param payloadDeserializer the deserializer.
     * @return the builder
     */
    public NatsJetStreamSourceBuilder<OutputT> payloadDeserializer(PayloadDeserializer<OutputT> payloadDeserializer) {
        this.payloadDeserializer = payloadDeserializer;
        this.payloadDeserializerClass = null;
        return this;
    }

    /**
     * Set the fully qualified name of the desired class payload deserializer for the source.
     * @param payloadDeserializerClass the serializer class name.
     * @return the builder
     */
    public NatsJetStreamSourceBuilder<OutputT> payloadDeserializerClass(String payloadDeserializerClass) {
        this.payloadDeserializer = null;
        this.payloadDeserializerClass = payloadDeserializerClass;
        return this;
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
            throw new IllegalStateException("Consumer name must be provided.");
        }

        // Validate stream name
        if (streamName == null || streamName.isEmpty()) {
            throw new IllegalStateException("Stream name must be provided.");
        }

        // Validate auto ack interval when enabled
        if (enableAutoAcknowledgeMessage &&
                (autoAckInterval == null || autoAckInterval.isZero() || autoAckInterval.isNegative())) {
            throw new IllegalStateException("Auto acknowledge interval must be positive when auto acknowledge is enabled");
        }

        // Validate max fetch records
        if (maxFetchRecords <= 0) {
            throw new IllegalStateException("Maximum fetch records must be positive");
        }

        // Validate message queue capacity
        if (messageQueueCapacity <= 0) {
            throw new IllegalStateException("Message queue capacity must be positive");
        }

        // Add validation for fetch timeouts
        if (fetchOneMessageTimeout == null || fetchOneMessageTimeout.isNegative()) {
            throw new IllegalStateException("Fetch timeout must be non-negative");
        }

        if (fetchTimeout == null || fetchTimeout.isNegative()) {
            throw new IllegalStateException("Max fetch time must be non-negative");
        }

        // Validate payload deserializer
        if (payloadDeserializer == null) {
            if (payloadDeserializerClass == null) {
                throw new IllegalStateException("Valid payload deserializer class must be provided.");
            }

            // so much can go wrong here... ClassNotFoundException, ClassCastException
            try {
                //noinspection unchecked
                payloadDeserializer = (PayloadDeserializer<OutputT>) Class.forName(payloadDeserializerClass).getDeclaredConstructor().newInstance();
            }
            catch (Exception e) {
                throw new IllegalStateException("Valid payload serializer class must be provided.", e);
            }
        }

        baseBuild();

        return new NatsJetStreamSource<>(payloadDeserializer,
            createConnectionFactory(),
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
