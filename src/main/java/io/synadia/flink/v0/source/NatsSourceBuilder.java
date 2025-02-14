// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.source;

import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.utils.Constants;
import io.synadia.flink.v0.utils.PropertiesUtils;

import java.util.Properties;

import static io.synadia.flink.v0.utils.Constants.PAYLOAD_DESERIALIZER;
import static io.synadia.flink.v0.utils.Constants.SOURCE_PREFIX;

/**
 * Builder to construct {@link NatsSource}.
 *
 * <p>The following example shows the minimum setup to create a NatsSource that reads String values
 * from one or more NATS subjects.
 *
 * <pre>{@code
 * NatsSource<String> source = NatsSource
 *     .<String>builder
 *     .subjects("subject1", "subject2")
 *     .connectionPropertiesFile("/path/to/jnats_client_connection.properties")
 *     .build();
 * }</pre>
 *
 * @see NatsSource
 * @param <OutputT> type of the records written
 */
public class NatsSourceBuilder<OutputT> extends NatsSinkOrSourceBuilder<NatsSourceBuilder<OutputT>> {
    private PayloadDeserializer<OutputT> payloadDeserializer;
    private String payloadDeserializerClass;

    public NatsSourceBuilder() {
        super(SOURCE_PREFIX);
    }

    @Override
    protected NatsSourceBuilder<OutputT> getThis() {
        return this;
    }

    /**
     * Set source properties from a properties object
     * See the readme and {@link Constants} for property keys
     * @param properties the properties object
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> sourceProperties(Properties properties) {
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
    public NatsSourceBuilder<OutputT> payloadDeserializer(PayloadDeserializer<OutputT> payloadDeserializer) {
        this.payloadDeserializer = payloadDeserializer;
        this.payloadDeserializerClass = null;
        return this;
    }

    /**
     * Set the fully qualified name of the desired class payload deserializer for the source.
     * @param payloadDeserializerClass the serializer class name.
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> payloadDeserializerClass(String payloadDeserializerClass) {
        this.payloadDeserializer = null;
        this.payloadDeserializerClass = payloadDeserializerClass;
        return this;
    }

    /**
     * Build a NatsSource
     * @return the source
     */
    public NatsSource<OutputT> build() {
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

        // Validate subjects
        if (subjects == null || subjects.isEmpty()) {
            throw new IllegalStateException("Subjects list is empty. At least one subject must be provided.");
        }

        baseBuild();
        return new NatsSource<>(payloadDeserializer, createConnectionFactory(), subjects);
    }
}