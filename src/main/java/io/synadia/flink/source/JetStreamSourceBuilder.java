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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.synadia.flink.utils.Constants.CONSUMER_STRATEGY;
import static io.synadia.flink.utils.Constants.JETSTREAM_SUBJECT_CONFIGURATIONS;

/**
 * Builder to construct {@link JetStreamSource}.
 * @param <OutputT> type of the records emitted by the source
 */
public class JetStreamSourceBuilder<OutputT> extends BuilderBase<OutputT, JetStreamSourceBuilder<OutputT>> {
    private final Map<String, JetStreamSubjectConfiguration> configById = new HashMap<>();
    private ConsumerStrategy consumerStrategy = ConsumerStrategy.Polled;

    /**
     * Construct a new JetStreamSourceBuilder instance
     */
    public JetStreamSourceBuilder() {
        super(false, false);
    }

    @Override
    protected JetStreamSourceBuilder<OutputT> getThis() {
        return this;
    }

    /**
     * Set source configuration from a JSON file
     * @param jsonFilePath the location of the file
     * @return the builder
     * @throws IOException if there is a problem loading or reading the file
     */
    public JetStreamSourceBuilder<OutputT> jsonConfigFile(String jsonFilePath) throws IOException {
        JsonValue jv = _jsonConfigFile(jsonFilePath);
        JsonValue jvConfigs = JsonValueUtils.readObject(jv, JETSTREAM_SUBJECT_CONFIGURATIONS);
        if (jvConfigs != null && jvConfigs.type == JsonValue.Type.ARRAY) {
            for (JsonValue config : jvConfigs.array) {
                addSubjectConfigurations(JetStreamSubjectConfiguration.fromJsonValue(config));
            }
        }
        ConsumerStrategy cs = ConsumerStrategy.get(JsonValueUtils.readString(jv, CONSUMER_STRATEGY, null));
        if (cs != null) {
            consumerStrategy(cs);
        }
        return this;
    }

    /**
     * Set source configuration from a YAML file
     * @param yamlFilePath the location of the file
     * @return the builder
     * @throws IOException if there is a problem loading or reading the file
     */
    public JetStreamSourceBuilder<OutputT> yamlConfigFile(String yamlFilePath) throws IOException {
        Map<String, Object> map = _yamlConfigFile(yamlFilePath);
        List<Map<String, Object>> mapConfigs = YamlUtils.readArray(map, JETSTREAM_SUBJECT_CONFIGURATIONS);
        if (mapConfigs != null) {
            for (Map<String, Object> config : mapConfigs) {
                addSubjectConfigurations(JetStreamSubjectConfiguration.fromMap(config));
            }
        }
        ConsumerStrategy cs = ConsumerStrategy.get(YamlUtils.readString(map, CONSUMER_STRATEGY, null));
        if (cs != null) {
            consumerStrategy(cs);
        }
        return this;
    }

    /**
     * Set the source converter.
     * @param sourceConverter the source converter.
     * @return the builder
     */
    public JetStreamSourceBuilder<OutputT> sourceConverter(SourceConverter<OutputT> sourceConverter) {
        return _sourceConverter(sourceConverter);
    }

    /**
     * Set the fully qualified name of the desired source converter class.
     * @param sourceConverterClass the converter class name.
     * @return the builder
     */
    public JetStreamSourceBuilder<OutputT> sourceConverterClass(String sourceConverterClass) {
        return _sourceConverterClass(sourceConverterClass);
    }

    /**
     * Set the source reader's element queue capacity. The reader floors the
     * value at Flink's ELEMENT_QUEUE_CAPACITY (configured or compile-time
     * default), so anything below that (-1 is conventional) yields the default.
     * @param sourceQueueCapacity the element queue capacity
     * @return The Builder
     */
    public JetStreamSourceBuilder<OutputT> sourceQueueCapacity(int sourceQueueCapacity) {
        return _sourceQueueCapacity(sourceQueueCapacity);
    }

    /**
     * Set the consumer strategy. Defaults to {@link ConsumerStrategy#Polled}.
     * A null argument is treated as the default.
     * @param consumerStrategy the consumer strategy
     * @return the builder
     */
    public JetStreamSourceBuilder<OutputT> consumerStrategy(ConsumerStrategy consumerStrategy) {
        this.consumerStrategy = consumerStrategy == null ? ConsumerStrategy.Polled : consumerStrategy;
        return this;
    }

    /**
     * Set one or more subject configurations, replacing any existing subject configurations
     * @param subjectConfigurations the subject configurations
     * @return the builder
     */
    public JetStreamSourceBuilder<OutputT> setSubjectConfigurations(JetStreamSubjectConfiguration... subjectConfigurations) {
        configById.clear();
        return addSubjectConfigurations(subjectConfigurations);
    }

    /**
     * Set one or more subject configurations, replacing any existing subject configurations
     * @param subjectConfigurations the subject configurations
     * @return the builder
     */
    public JetStreamSourceBuilder<OutputT> setSubjectConfigurations(List<JetStreamSubjectConfiguration> subjectConfigurations) {
        configById.clear();
        return addSubjectConfigurations(subjectConfigurations);
    }

    /**
     * Add one or more subject configurations to any existing subject configurations
     * @param subjectConfigurations the subject configurations
     * @return the builder
     */
    public JetStreamSourceBuilder<OutputT> addSubjectConfigurations(JetStreamSubjectConfiguration... subjectConfigurations) {
        if (subjectConfigurations != null) {
            for (JetStreamSubjectConfiguration subjectConfiguration : subjectConfigurations) {
                if (subjectConfiguration != null) {
                    configById.put(subjectConfiguration.id, subjectConfiguration);
                }
            }
        }
        return this;
    }

    /**
     * Add one or more subject configurations to any existing subject configurations
     * @param subjectConfigurations the subject configurations
     * @return the builder
     */
    public JetStreamSourceBuilder<OutputT> addSubjectConfigurations(List<JetStreamSubjectConfiguration> subjectConfigurations) {
        if (subjectConfigurations != null) {
            for (JetStreamSubjectConfiguration subjectConfiguration : subjectConfigurations) {
                if (subjectConfiguration != null) {
                    configById.put(subjectConfiguration.id, subjectConfiguration);
                }
            }
        }
        return this;
    }

    /**
     * Build a JetStreamSource
     * @return the JetStreamSource
     */
    public JetStreamSource<OutputT> build() {
        beforeBuild();

        if (configById.isEmpty()) {
            throw new IllegalArgumentException("At least 1 managed subject configuration is required.");
        }

        // check all the consume options of all the subject configs to
        // make sure they are the same boundedness if they are supplied
        Boundedness boundedness = null;
        for (JetStreamSubjectConfiguration msc : configById.values()) {
            if (boundedness == null) {
                boundedness = msc.boundedness;
            }
            else if (boundedness != msc.boundedness) {
                throw new IllegalArgumentException("All boundedness must be the same.");
            }
        }

        JetStreamSourceConfig config = new JetStreamSourceConfig(boundedness, sourceQueueCapacity, consumerStrategy);
        return new JetStreamSource<>(config, configById, sourceConverter, connectionFactory);
    }
}
