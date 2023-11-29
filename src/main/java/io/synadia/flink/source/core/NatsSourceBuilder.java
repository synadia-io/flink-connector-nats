// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.source.core;

import io.synadia.flink.Utils;
import io.synadia.flink.common.NatsSinkOrSourceBuilder;
import io.synadia.flink.payload.PayloadDeserializer;

import java.util.List;
import java.util.Properties;

import static io.synadia.flink.Constants.*;

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
 * @param <OutputT> type of the records written to Kafka
 */
public class NatsSourceBuilder<OutputT> extends NatsSinkOrSourceBuilder<NatsSourceBuilder<OutputT>> {
    private PayloadDeserializer<OutputT> payloadDeserializer;
    private String payloadDeserializerClass;

    @Override
    protected NatsSourceBuilder<OutputT> getThis() {
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
     * Set source properties from a properties object
     * See the readme and {@link io.synadia.flink.Constants} for property keys
     * @param properties the properties object
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> sourceProperties(Properties properties) {
        List<String> subjects = Utils.getPropertyAsList(properties, SOURCE_SUBJECTS);
        if (!subjects.isEmpty()) {
            subjects(subjects);
        }

        String s = properties.getProperty(SOURCE_PAYLOAD_DESERIALIZER);
        if (s != null) {
            payloadDeserializerClass(s);
        }

        long l = Utils.getLongProperty(properties, SOURCE_STARTUP_JITTER_MIN, -1);
        if (l != -1) {
            minConnectionJitter(l);
        }

        l = Utils.getLongProperty(properties, SOURCE_STARTUP_JITTER_MAX, -1);
        if (l != -1) {
            maxConnectionJitter(l);
        }

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
     * Build a NatsSource. Subject and
     * @return the source
     */
    public NatsSource<OutputT> build() {
        beforeBuild();

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

        return new NatsSource<>(subjects, payloadDeserializer, createConnectionFactory());
    }
}
