// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.utils;

import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import io.synadia.flink.payload.PayloadDeserializer;
import io.synadia.flink.payload.PayloadSerializer;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

import static io.nats.client.support.ApiConstants.SUBJECTS;
import static io.synadia.flink.utils.MiscUtils.*;
import static io.synadia.flink.utils.PropertiesUtils.getAsList;
import static io.synadia.flink.utils.PropertiesUtils.loadPropertiesFromFile;
import static io.synadia.flink.utils.PropertyConstants.PAYLOAD_DESERIALIZER;
import static io.synadia.flink.utils.PropertyConstants.PAYLOAD_SERIALIZER;

public abstract class BuilderBase<SerialT, BuilderT> {
    protected Properties connectionProperties;
    protected String connectionPropertiesFile;
    protected long minConnectionJitter = 0;
    protected long maxConnectionJitter = 0;
    protected String payloadSerializerClass;
    protected String payloadDeserializerClass;
    protected List<String> subjects;

    protected ConnectionFactory connectionFactory;
    protected PayloadSerializer<SerialT> payloadSerializer;
    protected PayloadDeserializer<SerialT> payloadDeserializer;

    private final boolean expectsSubjects;
    private final boolean expectsSerializerNotDeserializer;

    protected BuilderBase(boolean expectsSubjects, boolean expectsSerializerNotDeserializer) {
        this.expectsSubjects = expectsSubjects;
        this.expectsSerializerNotDeserializer = expectsSerializerNotDeserializer;
    }

    protected abstract BuilderT getThis();

    /**
     * Set the properties used to instantiate the {@link io.nats.client.Connection Connection}
     * <p>The properties should include enough information to create a connection to a NATS server.
     * See {@link io.nats.client.Options Connection Options}</p>
     * @param connectionProperties the properties
     * @return the builder
     */
    public BuilderT connectionPropertiesFile(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
        this.connectionPropertiesFile = null;
        return getThis();
    }

    /**
     * Set the properties file path to a properties file to be used to instantiate the {@link io.nats.client.Connection Connection}
     * @param connectionPropertiesFile the properties file path that would be available on all servers executing the job.
     * @return the builder
     */
    public BuilderT connectionPropertiesFile(String connectionPropertiesFile) {
        this.connectionProperties = null;
        this.connectionPropertiesFile = connectionPropertiesFile;
        return getThis();
    }

    protected BuilderT _subjects(String... subjects) {
        this.subjects = subjects == null || subjects.length == 0 ? null : Arrays.asList(subjects);
        return getThis();
    }

    protected BuilderT _subjects(List<String> subjects) {
        if (subjects == null || subjects.isEmpty()) {
            this.subjects = null;
        }
        else {
            this.subjects = new ArrayList<>(subjects);
        }
        return getThis();
    }

    protected BuilderT _payloadDeserializer(PayloadDeserializer<SerialT> payloadDeserializer) {
        return _payloadDeserializerClass(getClassName(payloadDeserializer));
    }

    protected BuilderT _payloadDeserializerClass(String payloadDeserializerClass) {
        this.payloadDeserializerClass = payloadDeserializerClass;
        return getThis();
    }

    protected BuilderT _payloadSerializer(PayloadSerializer<SerialT> payloadSerializer) {
        return _payloadSerializerClass(getClassName(payloadSerializer));
    }

    public BuilderT _payloadSerializerClass(String payloadSerializerClass) {
        this.payloadSerializerClass = payloadSerializerClass;
        return getThis();
    }

    protected void setBaseProperties(Function<String, String> propertyFunction) {
        if (expectsSubjects) {
            _subjects(getAsList(propertyFunction.apply(SUBJECTS)));
        }

        if (expectsSerializerNotDeserializer) {
            String classname = propertyFunction.apply(PAYLOAD_SERIALIZER);
            if (classname != null) {
                _payloadSerializerClass(classname);
            }
        }
        else {
            String classname = propertyFunction.apply(PAYLOAD_DESERIALIZER);
            if (classname != null) {
                _payloadDeserializerClass(classname);
            }
        }
    }

    protected Properties fromPropertiesFile(String propertiesFilePath) throws IOException {
        Properties properties = loadPropertiesFromFile(propertiesFilePath);
        setBaseProperties(k -> PropertiesUtils.getStringProperty(properties, k));
        return properties;
    }

    protected JsonValue fromJsonFile(String jsonFilePath) throws IOException {
        JsonValue jv = JsonParser.parse(readAllBytes(jsonFilePath));
        setBaseProperties(k -> JsonValueUtils.readString(jv, k, null));
        return jv;
    }

    protected Map<String, Object> fromYamlFile(String yamlFilePath) throws IOException {
        Map<String, Object> map = new Yaml().load(getInputStream(yamlFilePath));
        setBaseProperties(k -> YamlUtils.readString(map, k, null));
        return map;
    }

    protected void beforeBuild() {
        if (expectsSubjects && MiscUtils.notProvided(subjects)) {
            throw new IllegalArgumentException("One or more subjects must be provided.");
        }

        // must have one or the other
        if (connectionProperties == null && connectionPropertiesFile == null) {
            throw new IllegalArgumentException ("Connection properties or properties file must be provided.");
        }

        // if there is a file, we must be able to load it
        if (connectionPropertiesFile != null) {
            try {
                PropertiesUtils.loadPropertiesFromFile(connectionPropertiesFile);
            }
            catch (IOException e) {
                throw new IllegalArgumentException ("Cannot load properties file.", e.getCause());
            }
        }

        if (minConnectionJitter > maxConnectionJitter) {
            throw new IllegalArgumentException("Minimum jitter must be less than or equal to maximum jitter.");
        }

        if (expectsSerializerNotDeserializer) {
            createPayloadSerializerInstance();
        }
        else {
            createPayloadDeserializerInstance();
        }

        connectionFactory = connectionProperties == null
            ? new ConnectionFactory(connectionPropertiesFile)
            : new ConnectionFactory(connectionProperties);
    }

    private void createPayloadSerializerInstance() {
        if (payloadSerializerClass == null) {
            throw new IllegalArgumentException("Valid payload serializer class must be provided.");
        }
        // so much can go wrong here... ClassNotFoundException, ClassCastException
        try {
            //noinspection unchecked
            payloadSerializer = (PayloadSerializer<SerialT>) createInstanceOf(payloadSerializerClass);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Valid payload serializer class must be provided.", e);
        }
    }

    private void createPayloadDeserializerInstance() {
        if (payloadDeserializerClass == null) {
            throw new IllegalArgumentException("Valid payload deserializer class must be provided.");
        }
        // so much can go wrong here... ClassNotFoundException, ClassCastException
        try {
            //noinspection unchecked
            payloadDeserializer = (PayloadDeserializer<SerialT>) createInstanceOf(payloadDeserializerClass);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Valid payload deserializer class must be provided.", e);
        }
    }
}
