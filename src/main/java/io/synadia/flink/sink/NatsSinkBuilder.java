// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink;

import io.synadia.flink.payload.PayloadSerializer;
import io.synadia.flink.utils.Constants;
import io.synadia.flink.utils.PropertiesUtils;
import io.synadia.flink.utils.SinkOrSourceBuilderBase;

import java.util.Properties;

/**
 * Builder to construct {@link NatsSink}.
 *
 * <p>The following example shows the minimum setup to create a NatsSink that writes String values
 * to one or more NATS subjects.
 *
 * <pre>{@code
 * NatsSink<String> sink = NatsSink
 *     .<String>builder
 *     .subjects("subject1", "subject2")
 *     .connectionPropertiesFile("/path/to/jnats_client_connection.properties")
 *     .build();
 * }</pre>
 *
 * @see NatsSink
 * @param <InputT> type of the records written
 */
public class NatsSinkBuilder<InputT> extends SinkOrSourceBuilderBase<NatsSinkBuilder<InputT>> {
    private PayloadSerializer<InputT> payloadSerializer;
    private String payloadSerializerClass;

    @Override
    protected NatsSinkBuilder<InputT> getThis() {
        return this;
    }

    public NatsSinkBuilder() {
        super(Constants.SINK_PREFIX);
    }

    /**
     * Set the payload serializer for the sink.
     * @param payloadSerializer the serializer.
     * @return the builder
     */
    public NatsSinkBuilder<InputT> payloadSerializer(PayloadSerializer<InputT> payloadSerializer) {
        this.payloadSerializer = payloadSerializer;
        this.payloadSerializerClass = null;
        return this;
    }

    /**
     * Set the fully qualified name of the desired class payload serializer for the sink.
     * @param payloadSerializerClass the serializer class name.
     * @return the builder
     */
    public NatsSinkBuilder<InputT> payloadSerializerClass(String payloadSerializerClass) {
        this.payloadSerializer = null;
        this.payloadSerializerClass = payloadSerializerClass;
        return this;
    }

    /**
     * Set sink properties from a properties object
     * See the readme and {@link Constants} for property keys
     * @param properties the properties object
     * @return the builder
     */
    public NatsSinkBuilder<InputT> sinkProperties(Properties properties) {
        baseProperties(properties);

        String s = PropertiesUtils.getStringProperty(properties, Constants.PAYLOAD_SERIALIZER, prefixes);
        if (s != null) {
            payloadSerializerClass(s);
        }

        return this;
    }

    /**
     * Build a NatsSink. Subject and
     * @return the sink
     */
    public NatsSink<InputT> build() {
        if (payloadSerializer == null) {
            if (payloadSerializerClass == null) {
                throw new IllegalStateException("Valid payload serializer class must be provided.");
            }

            // so much can go wrong here... ClassNotFoundException, ClassCastException
            try {
                //noinspection unchecked
                payloadSerializer = (PayloadSerializer<InputT>) Class.forName(payloadSerializerClass).getDeclaredConstructor().newInstance();
            }
            catch (Exception e) {
                throw new IllegalStateException("Valid payload serializer class must be provided.", e);
            }
        }

        baseBuild(true);
        return new NatsSink<>(subjects, payloadSerializer, createConnectionFactory());
    }
}
