// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.source;

import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.utils.BuilderBase;

import java.io.IOException;
import java.util.List;

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
     * Set source configuration from a properties file
     * @param propertiesFilePath the location of the file
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> sourceProperties(String propertiesFilePath) throws IOException {
        setBaseFromPropertiesFile(propertiesFilePath);
        return this;
    }

    /**
     * Set source configuration from a JSON file
     * @param jsonFilePath the location of the file
     * @return the builder
     * @throws IOException if there is a problem loading or reading the file
     */
    public NatsSourceBuilder<OutputT> sourceJson(String jsonFilePath) throws IOException {
        setBaseFromJsonFile(jsonFilePath);
        return this;
    }

    /**
     * Set source configuration from a YAML file
     * @param yamlFilePath the location of the file
     * @return the builder
     * @throws IOException if there is a problem loading or reading the file
     */
    public NatsSourceBuilder<OutputT> sourceYaml(String yamlFilePath) throws IOException {
        setBaseFromYamlFile(yamlFilePath);
        return this;
    }

    /**
     * Set one or more subjects for the source. Replaces all subjects previously set in the builder.
     * @param subjects the subjects
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> subjects(String... subjects) {
        return super._subjects(subjects);
    }

    /**
     * Set the subjects for the source. Replaces all subjects previously set in the builder.
     * @param subjects the list of subjects
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> subjects(List<String> subjects) {
        return super._subjects(subjects);
    }

    /**
     * Set the payload deserializer for the source.
     * @param payloadDeserializer the deserializer.
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> payloadDeserializer(PayloadDeserializer<OutputT> payloadDeserializer) {
        return super._payloadDeserializer(payloadDeserializer);
    }

    /**
     * Set the fully qualified name of the desired class payload deserializer for the source.
     * @param payloadDeserializerClass the serializer class name.
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> payloadDeserializerClass(String payloadDeserializerClass) {
        return super._payloadDeserializerClass(payloadDeserializerClass);
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
