// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink;

import io.synadia.flink.payload.PayloadSerializer;
import io.synadia.flink.utils.BuilderBase;

import java.io.IOException;
import java.util.List;

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
public class NatsSinkBuilder<InputT> extends BuilderBase<InputT, NatsSinkBuilder<InputT>> {
    public NatsSinkBuilder() {
        super(true, true);
    }

    @Override
    protected NatsSinkBuilder<InputT> getThis() {
        return this;
    }

    /**
     * Set one or more subjects for the sink. Replaces all subjects previously set in the builder.
     * @param subjects the subjects
     * @return the builder
     */
    public NatsSinkBuilder<InputT> subject(String... subjects) {
        return super._subjects(subjects);
    }

    /**
     * Set the subjects for the sink. Replaces all subjects previously set in the builder.
     * @param subjects the list of subjects
     * @return the builder
     */
    public NatsSinkBuilder<InputT> subject(List<String> subjects) {
        return super._subjects(subjects);
    }

    /**
     * Set the payload serializer for the sink.
     * @param payloadSerializer the serializer.
     * @return the builder
     */
    public NatsSinkBuilder<InputT> payloadSerializer(PayloadSerializer<InputT> payloadSerializer) {
        return super._payloadSerializer(payloadSerializer);
    }

    /**
     * Set the fully qualified name of the desired class payload serializer for the sink.
     * @param payloadSerializerClass the serializer class name.
     * @return the builder
     */
    public NatsSinkBuilder<InputT> payloadSerializerClass(String payloadSerializerClass) {
        return super._payloadSerializerClass(payloadSerializerClass);
    }

    /**
     * Set sink configuration from a properties file
     * @param propertiesFilePath the location of the file
     * @return the builder
     */
    public NatsSinkBuilder<InputT> sinkProperties(String propertiesFilePath) throws IOException {
        fromPropertiesFile(propertiesFilePath);
        return this;
    }

    /**
     * Set sink configuration from a json file
     * @param jsonFilePath the location of the file
     * @return the builder
     * @throws IOException if there is a problem loading or reading the file
     */
    public NatsSinkBuilder<InputT> sinkJson(String jsonFilePath) throws IOException {
        fromJsonFile(jsonFilePath);
        return this;
    }

    /**
     * Set sink configuration from a yaml file
     * @param yamlFilePath the location of the file
     * @return the builder
     * @throws IOException if there is a problem loading or reading the file
     */
    public NatsSinkBuilder<InputT> sinkYaml(String yamlFilePath) throws IOException {
        fromYamlFile(yamlFilePath);
        return this;
    }

    /**
     * Build a NatsSink. Subject and
     * @return the sink
     */
    public NatsSink<InputT> build() {
        beforeBuild();
        return new NatsSink<>(subjects, payloadSerializer, connectionFactory);
    }
}
