// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.source;

import io.synadia.flink.message.SourceConverter;
import io.synadia.flink.utils.BuilderBase;

import java.io.IOException;
import java.util.List;

/**
 * Builder to construct {@link NatsSource}.
 * @param <OutputT> type of the records emitted by the source
 */
public class NatsSourceBuilder<OutputT> extends BuilderBase<OutputT, NatsSourceBuilder<OutputT>> {

    /**
     * Construct a new NatsSourceBuilder instance
     */
    public NatsSourceBuilder() {
        super(true, false);
    }

    @Override
    protected NatsSourceBuilder<OutputT> getThis() {
        return this;
    }

    /**
     * Set source configuration from a JSON file
     * @param jsonFilePath the location of the file
     * @return the builder
     * @throws IOException if there is a problem loading or reading the file
     */
    public NatsSourceBuilder<OutputT> jsonConfigFile(String jsonFilePath) throws IOException {
        _jsonConfigFile(jsonFilePath);
        return this;
    }

    /**
     * Set source configuration from a YAML file
     * @param yamlFilePath the location of the file
     * @return the builder
     * @throws IOException if there is a problem loading or reading the file
     */
    public NatsSourceBuilder<OutputT> yamlConfigFile(String yamlFilePath) throws IOException {
        _yamlConfigFile(yamlFilePath);
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
     * Set the source converter.
     * @param sourceConverter the source converter.
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> sourceConverter(SourceConverter<OutputT> sourceConverter) {
        return super._sourceConverter(sourceConverter);
    }

    /**
     * Set the fully qualified name of the desired class source converter.
     * @param sourceConverterClass the converter class name.
     * @return the builder
     */
    public NatsSourceBuilder<OutputT> sourceConverterClass(String sourceConverterClass) {
        return super._sourceConverterClass(sourceConverterClass);
    }

    /**
     * Build a NatsSource
     * @return the source
     */
    public NatsSource<OutputT> build() {
        beforeBuild();
        return new NatsSource<>(sourceConverter, connectionFactory, subjects);
    }
}
