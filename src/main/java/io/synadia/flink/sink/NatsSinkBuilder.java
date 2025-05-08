// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink;

import io.synadia.flink.message.SinkConverter;
import io.synadia.flink.utils.BuilderBase;

import java.io.IOException;
import java.util.List;

/**
 * Builder to construct {@link NatsSink}.
 * @param <InputT> type expected as input to the sink
 */
public class NatsSinkBuilder<InputT> extends BuilderBase<InputT, NatsSinkBuilder<InputT>> {

    /**
     * Construct a new NatsSinkBuilder instance
     */
    public NatsSinkBuilder() {
        super(true, true);
    }

    @Override
    protected NatsSinkBuilder<InputT> getThis() {
        return this;
    }

    /**
     * Set one or more subjects. Replaces all subjects previously set in the builder.
     * @param subjects the subjects
     * @return the builder
     */
    public NatsSinkBuilder<InputT> subjects(String... subjects) {
        return super._subjects(subjects);
    }

    /**
     * Set the subjects. Replaces all subjects previously set in the builder.
     * @param subjects the list of subjects
     * @return the builder
     */
    public NatsSinkBuilder<InputT> subjects(List<String> subjects) {
        return super._subjects(subjects);
    }

    /**
     * Set the sink converter.
     * @param sinkConverter the supplier.
     * @return the builder
     */
    public NatsSinkBuilder<InputT> sinkConverter(SinkConverter<InputT> sinkConverter) {
        return super._sinkConverter(sinkConverter);
    }

    /**
     * Set the fully qualified name of the desired class sink converter.
     * @param sinkConverterClass the converter class name.
     * @return the builder
     */
    public NatsSinkBuilder<InputT> sinkConverterClass(String sinkConverterClass) {
        return super._sinkConverterClass(sinkConverterClass);
    }

    /**
     * Set sink configuration from a JSON file
     * @param jsonFilePath the location of the file
     * @return the builder
     * @throws IOException if there is a problem loading or reading the file
     */
    public NatsSinkBuilder<InputT> jsonConfigFile(String jsonFilePath) throws IOException {
        _jsonConfigFile(jsonFilePath);
        return this;
    }

    /**
     * Set sink configuration from a YAML file
     * @param yamlFilePath the location of the file
     * @return the builder
     * @throws IOException if there is a problem loading or reading the file
     */
    public NatsSinkBuilder<InputT> yamlConfigFile(String yamlFilePath) throws IOException {
        _yamlConfigFile(yamlFilePath);
        return this;
    }

    /**
     * Build a NatsSink.
     * @return the NatsSink
     */
    public NatsSink<InputT> build() {
        beforeBuild();
        return new NatsSink<>(subjects, sinkConverter, connectionFactory);
    }
}
