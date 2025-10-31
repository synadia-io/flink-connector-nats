// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.utils;

import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import io.synadia.flink.message.SinkConverter;
import io.synadia.flink.message.SourceConverter;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.*;

import static io.synadia.flink.utils.Constants.*;
import static io.synadia.flink.utils.MiscUtils.*;

/**
 * The base builder
 * @param <SerialT> the builder data type
 * @param <BuilderT> The builder type
 */
public abstract class BuilderBase<SerialT, BuilderT> {
    /**
     * The connectionProperties
     */
    protected Properties connectionProperties;

    /**
     * the connection properties file path
     */
    protected String connectionPropertiesFile;

    /**
     * The sink converter class when the builder is for a sink
     */
    protected String sinkConverterClass;

    /**
     * The source convert class when the builder is a for source
     */
    protected String sourceConverterClass;

    /**
     * The subjects
     */
    protected List<String> subjects;

    /**
     * The connection factory
     */
    protected ConnectionFactory connectionFactory;

    /**
     * The sink converter when the builder is for a sink
     */
    protected SinkConverter<SerialT> sinkConverter;

    /**
     * The source converter when the builder is for a source
     */
    protected SourceConverter<SerialT> sourceConverter;

    private final boolean expectsSubjects;
    private final boolean forSink;

    /**
     * Construct a builder
     * @param expectsSubjects whether the builder expects the base behavior for having subjects
     * @param forSink whether the builder is for a sink
     */
    protected BuilderBase(boolean expectsSubjects, boolean forSink) {
        this.expectsSubjects = expectsSubjects;
        this.forSink = forSink;
    }

    /**
     * Get the instance of the builder. Needed for generic extension
     * @return The Builder
     */
    protected abstract BuilderT getThis();

    /**
     * Set the properties used to instantiate the {@link io.nats.client.Connection Connection}
     * <p>The properties should include enough information to create a connection to a NATS server.
     * See {@link io.nats.client.Options Connection Options}</p>
     * @param connectionProperties the properties
     * @return the builder
     */
    public BuilderT connectionProperties(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
        this.connectionPropertiesFile = null;
        return getThis();
    }

    /**
     * Set the properties file path to a properties file to be used to instantiate the {@link io.nats.client.Connection Connection}
     * The properties file path must be available on all servers executing the job.
     * <p>The properties should include enough information to create a connection to a NATS server.
     * See {@link io.nats.client.Options Connection Options}</p>
     * @param connectionPropertiesFile the properties file path
     * @return the builder
     */
    public BuilderT connectionPropertiesFile(String connectionPropertiesFile) {
        this.connectionProperties = null;
        this.connectionPropertiesFile = connectionPropertiesFile;
        return getThis();
    }

    /**
     * Set the subjects
     * @param subjects the subjects
     * @return The Builder
     */
    protected BuilderT _subjects(String... subjects) {
        this.subjects = subjects == null || subjects.length == 0 ? null : Arrays.asList(subjects);
        return getThis();
    }

    /**
     * Set the subjects
     * @param subjects the subjects
     * @return The Builder
     */
    protected BuilderT _subjects(List<String> subjects) {
        this.subjects = new ArrayList<>();
        if (subjects != null) {
            for (String subject : subjects) {
                if (subject != null && !subject.isEmpty()) {
                    this.subjects.add(subject);
                }
            }
        }
        if (this.subjects.isEmpty()) {
            this.subjects = null;
        }
        return getThis();
    }

    /**
     * Set the source converter class instance
     * @param sourceConverter the class instance
     * @return The Builder
     */
    protected BuilderT _sourceConverter(SourceConverter<SerialT> sourceConverter) {
        return _sourceConverterClass(getClassName(sourceConverter));
    }

    /**
     * Set the source converter class name
     * @param sourceConverterClass the class name
     * @return The Builder
     */
    protected BuilderT _sourceConverterClass(String sourceConverterClass) {
        this.sourceConverterClass = sourceConverterClass;
        return getThis();
    }

    /**
     * Set the SinkConverter
     * @param sinkConverter the sink converter
     * @return The Builder
     */
    protected BuilderT _sinkConverter(SinkConverter<SerialT> sinkConverter) {
        return _sinkConverterClass(getClassName(sinkConverter));
    }

    /**
     * Set the sinkConverterClass
     * @param sinkConverterClass the sinkConverterClass
     * @return The Builder
     */
    public BuilderT _sinkConverterClass(String sinkConverterClass) {
        this.sinkConverterClass = sinkConverterClass;
        return getThis();
    }

    /**
     * A ConfigurationAdapter class used to adapt builder specific strings
     */
    protected interface ConfigurationAdapter {
        /**
         * get a list from the key
         * @param key the key
         * @return the list
         */
        List<String> getList(String key);

        /**
         * Get a string from the key
         * @param key the key
         * @return the string
         */
        String getString(String key);
    }

    /**
     * Set the ConfigurationAdapter
     * @param adapter the adapter
     */
    protected void _config(ConfigurationAdapter adapter) {
        if (expectsSubjects) {
            // We support SUBJECT or SUBJECTS
            List<String> subjects = adapter.getList(SUBJECTS);
            if (subjects == null || subjects.isEmpty()) {
                String s = adapter.getString(SUBJECT);
                if (s != null && !s.trim().isEmpty()) {
                    _subjects(s.split(","));
                }
            }
            else {
                _subjects(subjects);
            }
        }

        if (forSink) {
            String classname = adapter.getString(SINK_CONVERTER_CLASS_NAME);
            if (classname != null) {
                _sinkConverterClass(classname);
            }
        }
        else {
            String classname = adapter.getString(SOURCE_CONVERTER_CLASS_NAME);
            if (classname != null) {
                _sourceConverterClass(classname);
            }
        }
    }

    /**
     * Accept a JSON file as config
     * @param jsonFilePath the path to the file
     * @return The Builder
     * @throws IOException if there is an issue opening or reading the file
     */
    protected JsonValue _jsonConfigFile(String jsonFilePath) throws IOException {
        JsonValue jv = JsonParser.parse(readAllBytes(jsonFilePath));
        _config(new ConfigurationAdapter() {
            @Override
            public List<String> getList(String key) {
                return JsonValueUtils.readStringList(jv, key);
            }

            @Override
            public String getString(String key) {
                return JsonValueUtils.readString(jv, key, null);
            }
        });
        return jv;
    }

    /**
     * Accept a YAML file as config
     * @param yamlFilePath the path to the file
     * @return The Builder
     * @throws IOException if there is an issue opening or reading the file
     */
    protected Map<String, Object> _yamlConfigFile(String yamlFilePath) throws IOException {
        Map<String, Object> map = new Yaml().load(getInputStream(yamlFilePath));
        _config(new ConfigurationAdapter() {
            @Override
            public List<String> getList(String key) {
                return YamlUtils.readArrayAsStrings(map, key);
            }

            @Override
            public String getString(String key) {
                return YamlUtils.readString(map, key, null);
            }
        });
        return map;
    }

    /**
     * Execute this before building
     */
    protected void beforeBuild() {
        if (expectsSubjects && MiscUtils.notProvided(subjects)) {
            throw new IllegalArgumentException("One or more subjects must be provided.");
        }

        // If neither are supplied, we will connect to localhost. Not really our problem.
        if (connectionProperties == null && connectionPropertiesFile == null) {
            connectionProperties = new Properties();
        }

        if (forSink) {
            createMessageSupplierInstance();
        }
        else {
            createMessageReaderInstance();
        }

        connectionFactory = connectionProperties == null
            ? new ConnectionFactory(connectionPropertiesFile)
            : new ConnectionFactory(connectionProperties);
    }

    private void createMessageSupplierInstance() {
        if (sinkConverterClass == null) {
            throw new IllegalArgumentException("Valid message supplier class must be provided.");
        }
        // so much can go wrong here... ClassNotFoundException, ClassCastException
        try {
            //noinspection unchecked
            sinkConverter = (SinkConverter<SerialT>) createInstanceOf(sinkConverterClass);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Valid message supplier class must be provided.", e);
        }
    }

    private void createMessageReaderInstance() {
        if (sourceConverterClass == null) {
            throw new IllegalArgumentException("Valid source converter class must be provided.");
        }
        // so much can go wrong here... ClassNotFoundException, ClassCastException
        try {
            //noinspection unchecked
            sourceConverter = (SourceConverter<SerialT>) createInstanceOf(sourceConverterClass);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Valid source converter class must be provided.", e);
        }
    }
}
