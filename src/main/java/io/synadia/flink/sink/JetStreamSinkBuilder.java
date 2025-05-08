// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.sink;

import io.synadia.flink.message.SinkConverter;
import io.synadia.flink.utils.BuilderBase;

import java.io.IOException;
import java.util.List;

/**
 * Builder to construct {@link JetStreamSink}.
 * @param <InputT> type expected as input to the sink
 */
public class JetStreamSinkBuilder<InputT> extends BuilderBase<InputT, JetStreamSinkBuilder<InputT>> {

    /**
     * Construct a new JetStreamSinkBuilder instance
     */
    public JetStreamSinkBuilder() {
        super(true, true);
    }

    @Override
    protected JetStreamSinkBuilder<InputT> getThis() {
        return this;
    }

    /**
     * Set one or more subjects. Replaces all subjects previously set in the builder.
     * @param subjects the subjects
     * @return the builder
     */
    public JetStreamSinkBuilder<InputT> subjects(String... subjects) {
        return _subjects(subjects);
    }

    /**
     * Set the subjects. Replaces all subjects previously set in the builder.
     * @param subjects the list of subjects
     * @return the builder
     */
    public JetStreamSinkBuilder<InputT> subjects(List<String> subjects) {
        return _subjects(subjects);
    }

    /**
     * Set the sink converter.
     * @param sinkConverter the converter.
     * @return the builder
     */
    public JetStreamSinkBuilder<InputT> sinkConverter(SinkConverter<InputT> sinkConverter) {
        return _sinkConverter(sinkConverter);
    }

    /**
     * Set the fully qualified name of the desired sink converter class.
     * @param sinkConverterClass the converter class name.
     * @return the builder
     */
    public JetStreamSinkBuilder<InputT> sinkConverterClass(String sinkConverterClass) {
        return _sinkConverterClass(sinkConverterClass);
    }

    /**
     * Set sink configuration from a JSON file
     * @param jsonFilePath the location of the file
     * @return the builder
     * @throws IOException if there is a problem loading or reading the file
     */
    public JetStreamSinkBuilder<InputT> jsonConfigFile(String jsonFilePath) throws IOException {
        _jsonConfigFile(jsonFilePath);
        return this;
    }

    /**
     * Set sink configuration from a YAML file
     * @param yamlFilePath the location of the file
     * @return the builder
     * @throws IOException if there is a problem loading or reading the file
     */
    public JetStreamSinkBuilder<InputT> yamlConfigFile(String yamlFilePath) throws IOException {
        _yamlConfigFile(yamlFilePath);
        return this;
    }

    /**
     * Build a JetStreamSink.
     * @return the JetStreamSink
     */
    public JetStreamSink<InputT> build() {
        beforeBuild();
        return new JetStreamSink<>(subjects, sinkConverter, connectionFactory);
    }
}
