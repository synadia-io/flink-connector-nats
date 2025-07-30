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

public abstract class BuilderBase<SerialT, BuilderT> {
    protected Properties connectionProperties;
    protected String connectionPropertiesFile;
    protected String sinkConverterClass;
    protected String sourceConverterClass;
    protected List<String> subjects;

    protected ConnectionFactory connectionFactory;
    protected SinkConverter<SerialT> sinkConverter;
    protected SourceConverter<SerialT> sourceConverter;

    private final boolean expectsSubjects;
    private final boolean forSink;

    protected BuilderBase(boolean expectsSubjects, boolean forSink) {
        this.expectsSubjects = expectsSubjects;
        this.forSink = forSink;
    }

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

    protected BuilderT _subjects(String... subjects) {
        this.subjects = subjects == null || subjects.length == 0 ? null : Arrays.asList(subjects);
        return getThis();
    }

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

    protected BuilderT _sourceConverter(SourceConverter<SerialT> sourceConverter) {
        return _sourceConverterClass(getClassName(sourceConverter));
    }

    protected BuilderT _sourceConverterClass(String sourceConverterClass) {
        this.sourceConverterClass = sourceConverterClass;
        return getThis();
    }

    protected BuilderT _sinkConverter(SinkConverter<SerialT> sinkConverter) {
        return _sinkConverterClass(getClassName(sinkConverter));
    }

    public BuilderT _sinkConverterClass(String sinkConverterClass) {
        this.sinkConverterClass = sinkConverterClass;
        return getThis();
    }

    protected interface ConfigurationAdapter {
        List<String> getList(String key);
        String getString(String key);
    }

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
