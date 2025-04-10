// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.utils.BuilderBase;
import io.synadia.flink.utils.Constants;
import io.synadia.flink.utils.YamlUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.synadia.flink.utils.Constants.JETSTREAM_SUBJECT_CONFIGURATIONS;
import static io.synadia.flink.utils.MiscUtils.getInputStream;
import static io.synadia.flink.utils.MiscUtils.readAllBytes;
import static io.synadia.flink.utils.PropertiesUtils.loadPropertiesFromFile;

public class JetStreamSourceBuilder<OutputT> extends BuilderBase<OutputT, JetStreamSourceBuilder<OutputT>> {
    private final Map<String, JetStreamSubjectConfiguration> configById = new HashMap<>();

    public JetStreamSourceBuilder() {
        super(false, false);
    }

    @Override
    protected JetStreamSourceBuilder<OutputT> getThis() {
        return this;
    }

    /**
     * Set source configuration from a properties file
     * See the readme and {@link Constants} for property keys
     * @param propertiesFilePath the location of the file
     * @return the builder
     */
    public JetStreamSourceBuilder<OutputT> sourceProperties(String propertiesFilePath) throws IOException {
        return properties(loadPropertiesFromFile(propertiesFilePath));
    }

    /**
     * Set source configuration from a json file
     * @param jsonFilePath the location of the file
     * @return the builder
     * @throws IOException if there is an
     */
    public JetStreamSourceBuilder<OutputT> sourceJson(String jsonFilePath) throws IOException {
        JsonValue jv = JsonParser.parse(readAllBytes(jsonFilePath));
        JsonValue jvConfigs = JsonValueUtils.readObject(jv, JETSTREAM_SUBJECT_CONFIGURATIONS);
        if (jvConfigs != null && jvConfigs.type == JsonValue.Type.ARRAY) {
            for (JsonValue config : jvConfigs.array) {
                addSubjectConfigurations(JetStreamSubjectConfiguration.fromJsonValue(config));
            }
        }
        return jsonValue(jv);
    }

    /**
     * Set source configuration from a yaml file
     * @param yamlFilePath the location of the file
     * @return the builder
     * @throws IOException if there is an
     */
    public JetStreamSourceBuilder<OutputT> sourceYaml(String yamlFilePath) throws IOException {
        Yaml yaml = new Yaml();
        Map<String, Object> map = yaml.load(getInputStream(yamlFilePath));
        List<Map<String, Object>> mapConfigs = YamlUtils.readArray(map, JETSTREAM_SUBJECT_CONFIGURATIONS);
        if (mapConfigs != null) {
            for (Map<String, Object> config : mapConfigs) {
                addSubjectConfigurations(JetStreamSubjectConfiguration.fromMap(config));
            }
        }
        return yamlMap(map);
    }

    /**
     * Set the payload deserializer for the source.
     * @param payloadDeserializer the deserializer.
     * @return the builder
     */
    public JetStreamSourceBuilder<OutputT> payloadDeserializer(PayloadDeserializer<OutputT> payloadDeserializer) {
        return super.payloadDeserializer(payloadDeserializer);
    }

    /**
     * Set the fully qualified name of the desired class payload deserializer for the source.
     * @param payloadDeserializerClass the serializer class name.
     * @return the builder
     */
    public JetStreamSourceBuilder<OutputT> payloadDeserializerClass(String payloadDeserializerClass) {
        return super.payloadDeserializerClass(payloadDeserializerClass);
    }

    public JetStreamSourceBuilder<OutputT> setSubjectConfigurations(JetStreamSubjectConfiguration... subjectConfigurations) {
        configById.clear();
        return addSubjectConfigurations(subjectConfigurations);
    }

    public JetStreamSourceBuilder<OutputT> setSubjectConfigurations(List<JetStreamSubjectConfiguration> subjectConfigurations) {
        configById.clear();
        return addSubjectConfigurations(subjectConfigurations);
    }

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

    public JetStreamSource<OutputT> build() {
        beforeBuild();

        if (configById.isEmpty()) {
            throw new IllegalArgumentException("At least 1 managed subject configuration is required");
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

        return new JetStreamSource<>(boundedness, configById, payloadDeserializer, connectionFactory);
    }
}
