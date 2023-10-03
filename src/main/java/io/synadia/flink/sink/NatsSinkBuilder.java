// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink;

import io.synadia.flink.Utils;
import io.synadia.flink.common.NatsSinkOrSourceBuilder;
import io.synadia.flink.payload.PayloadSerializer;

import java.util.List;
import java.util.Properties;

import static io.synadia.flink.Constants.*;

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
 * @param <InputT> type of the records written to Kafka
 */
public class NatsSinkBuilder<InputT> extends NatsSinkOrSourceBuilder<NatsSinkBuilder<InputT>> {
    private PayloadSerializer<InputT> payloadSerializer;
    private String payloadSerializerClass;

    @Override
    protected NatsSinkBuilder<InputT> getThis() {
        return this;
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
     * See the readme and {@link io.synadia.flink.Constants} for property keys
     * @param properties the properties object
     * @return the builder
     */
    public NatsSinkBuilder<InputT> sinkProperties(Properties properties) {
        List<String> subjects = Utils.getPropertyAsList(properties, SINK_SUBJECTS);
        if (!subjects.isEmpty()) {
            subjects(subjects);
        }

        String s = properties.getProperty(SINK_PAYLOAD_SERIALIZER);
        if (s != null) {
            payloadSerializerClass(s);
        }

        long l = Utils.getLongProperty(properties, SINK_STARTUP_JITTER_MIN, -1);
        if (l != -1) {
            minConnectionJitter(l);
        }

        l = Utils.getLongProperty(properties, SINK_STARTUP_JITTER_MAX, -1);
        if (l != -1) {
            maxConnectionJitter(l);
        }

        return this;
    }

    /**
     * Build a NatsSink. Subject and
     * @return the sink
     */
    public NatsSink<InputT> build() {
        beforeBuild();

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

        return new NatsSink<>(subjects, payloadSerializer, createConnectionFactory());
    }
}
