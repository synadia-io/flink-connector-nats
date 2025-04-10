// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.source;

import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.utils.BuilderBase;
import io.synadia.flink.utils.Constants;

import java.util.List;
import java.util.Properties;

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
public class NatsSourceBuilder<OutputT> extends BuilderBase<OutputT, NatsSourceBuilder<OutputT>> {
    public NatsSourceBuilder() {
        super(true, false);
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
        return super.properties(properties);
    }

    /**
     * Set one or more subjects for the source. Replaces all subjects previously set in the builder.
     * @param subjects the subjects
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> subjects(String... subjects) {
        return super.subjects(subjects);
    }

    /**
     * Set the subjects for the source. Replaces all subjects previously set in the builder.
     * @param subjects the list of subjects
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> subjects(List<String> subjects) {
        return super.subjects(subjects);
    }

    /**
     * Set the payload deserializer for the source.
     * @param payloadDeserializer the deserializer.
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> payloadDeserializer(PayloadDeserializer<OutputT> payloadDeserializer) {
        return super.payloadDeserializer(payloadDeserializer);
    }

    /**
     * Set the fully qualified name of the desired class payload deserializer for the source.
     * @param payloadDeserializerClass the serializer class name.
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> payloadDeserializerClass(String payloadDeserializerClass) {
        return super.payloadDeserializerClass(payloadDeserializerClass);
    }

    /**
     * Build a NatsSource
     * @return the source
     */
    public NatsSource<OutputT> build() {
        beforeBuild();
        return new NatsSource<>(payloadDeserializer, connectionFactory, subjects);
    }
}
