// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import io.synadia.flink.message.SourceConverter;
import io.synadia.flink.utils.BuilderBase;
import io.synadia.flink.utils.YamlUtils;
import org.apache.flink.api.connector.source.Boundedness;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.synadia.flink.utils.Constants.SOURCE_QUEUE_CAPACITY;

/**
 * Builder to construct {@link NatsSource}.
 * @param <OutputT> type of the records emitted by the source
 */
public class NatsSourceBuilder<OutputT> extends BuilderBase<OutputT, NatsSourceBuilder<OutputT>> {

    /**
     * Default element queue capacity for the source reader. The NATS dispatcher
     * pushes messages with no flow control on our side, so size generously
     * rather than at Flink's ELEMENT_QUEUE_CAPACITY default of 2.
     */
    public static final int DEFAULT_SOURCE_QUEUE_CAPACITY = 1024;

    /**
     * Minimum accepted source queue capacity. Below this the NATS dispatcher
     * is liable to block its own thread on {@code queue.put} during brief
     * consumer slowdowns, since it has no flow control.
     */
    public static final int MIN_SOURCE_QUEUE_CAPACITY = 32;

    private int sourceQueueCapacity = DEFAULT_SOURCE_QUEUE_CAPACITY;

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
        JsonValue jv = _jsonConfigFile(jsonFilePath);
        return sourceQueueCapacity(JsonValueUtils.readInteger(jv, SOURCE_QUEUE_CAPACITY, DEFAULT_SOURCE_QUEUE_CAPACITY));
    }

    /**
     * Set source configuration from a YAML file
     * @param yamlFilePath the location of the file
     * @return the builder
     * @throws IOException if there is a problem loading or reading the file
     */
    public NatsSourceBuilder<OutputT> yamlConfigFile(String yamlFilePath) throws IOException {
        Map<String, Object> map = _yamlConfigFile(yamlFilePath);
        return sourceQueueCapacity(YamlUtils.readInteger(map, SOURCE_QUEUE_CAPACITY, DEFAULT_SOURCE_QUEUE_CAPACITY));
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
     * Set the source reader's element queue capacity. Defaults to
     * {@link #DEFAULT_SOURCE_QUEUE_CAPACITY}; the NATS dispatcher pushes with
     * no flow control, so size generously. Must be at least
     * {@link #MIN_SOURCE_QUEUE_CAPACITY}.
     * @param sourceQueueCapacity the element queue capacity
     * @return the builder
     * @throws IllegalArgumentException if {@code sourceQueueCapacity} is below
     *     {@link #MIN_SOURCE_QUEUE_CAPACITY}
     */
    public NatsSourceBuilder<OutputT> sourceQueueCapacity(int sourceQueueCapacity) {
        if (sourceQueueCapacity < MIN_SOURCE_QUEUE_CAPACITY) {
            throw new IllegalArgumentException(
                "sourceQueueCapacity must be >= " + MIN_SOURCE_QUEUE_CAPACITY
                    + " (got " + sourceQueueCapacity + ")");
        }
        this.sourceQueueCapacity = sourceQueueCapacity;
        return this;
    }

    /**
     * Build a NatsSource
     * @return the source
     */
    public NatsSource<OutputT> build() {
        beforeBuild();
        SourceConfig config = new SourceConfig(Boundedness.CONTINUOUS_UNBOUNDED, sourceQueueCapacity);
        return new NatsSource<>(config, subjects, sourceConverter, connectionFactory);
    }
}
