// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.source;

import io.synadia.flink.v0.utils.ConnectionFactory;
import io.synadia.flink.v0.utils.Constants;
import io.synadia.flink.v0.utils.PropertiesUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static io.synadia.flink.v0.utils.Constants.*;
import static io.synadia.flink.v0.utils.PropertiesUtils.NO_PREFIX;

public abstract class NatsSinkOrSourceBuilder<BuilderT> {
    protected final String[] prefixes;

    protected List<String> subjects;
    protected Properties connectionProperties;
    protected String connectionPropertiesFile;
    protected long minConnectionJitter = 0;
    protected long maxConnectionJitter = 0;

    protected abstract BuilderT getThis();

    public NatsSinkOrSourceBuilder(String prefix) {
        prefixes = new String[]{NO_PREFIX, prefix, NATS_PREFIX + prefix};
    }

    /**
     * Set properties from a properties object
     * See the readme and {@link Constants} for property keys
     * @param properties the properties object
     */
    protected void baseProperties(Properties properties) {
        List<String> subjects = PropertiesUtils.getPropertyAsList(properties, SUBJECTS, prefixes);
        if (!subjects.isEmpty()) {
            subjects(subjects);
        }

        long l = PropertiesUtils.getLongProperty(properties, STARTUP_JITTER_MIN, -1, prefixes);
        if (l != -1) {
            minConnectionJitter(l);
        }

        l = PropertiesUtils.getLongProperty(properties, STARTUP_JITTER_MAX, -1, prefixes);
        if (l != -1) {
            maxConnectionJitter(l);
        }
    }

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
     * @param connectionPropertiesFile the properties file path that would be available on all servers executing the job.
     * @return the builder
     */
    public BuilderT connectionPropertiesFile(String connectionPropertiesFile) {
        this.connectionProperties = null;
        this.connectionPropertiesFile = connectionPropertiesFile;
        return getThis();
    }

    /**
     * Set the minimum jitter for connections in milliseconds. Default is 0.
     * Values less than 0 will be converted to 0.
     * @param minConnectionJitter the minimum jitter value
     * @return the builder
     */
    public BuilderT minConnectionJitter(long minConnectionJitter) {
        this.minConnectionJitter = minConnectionJitter;
        return getThis();
    }

    /**
     * Set the maximum jitter for connections in milliseconds. 0.
     * Values less than 0 will be converted to 0.
     * @param maxConnectionJitter the maximum jitter value
     * @return the builder
     */
    public BuilderT maxConnectionJitter(long maxConnectionJitter) {
        this.maxConnectionJitter = maxConnectionJitter;
        return getThis();
    }

    /**
     * Set one or more subjects for the sink. Replaces all subjects previously set in the builder.
     * @param subjects the subjects
     * @return the builder
     */
    public BuilderT subjects(String... subjects) {
        this.subjects = subjects == null || subjects.length == 0 ? null : Arrays.asList(subjects);
        return getThis();
    }

    /**
     * Set the subjects for the sink. Replaces all subjects previously set in the builder.
     * @param subjects the list of subjects
     * @return the builder
     */
    public BuilderT subjects(List<String> subjects) {
        if (subjects == null || subjects.isEmpty()) {
            this.subjects = null;
        }
        else {
            this.subjects = new ArrayList<>(subjects);
        }
        return getThis();
    }

    protected void baseBuild() {
        if (subjects == null || subjects.isEmpty()) {
            throw new IllegalStateException("One or more subjects must be provided.");
        }

        // must have one or the other
        if (connectionProperties == null && connectionPropertiesFile == null) {
            throw new IllegalStateException ("Sink properties or propertiesFile must be provided.");
        }

        // if there is a file, we must be able to load it
        if (connectionPropertiesFile != null) {
            try {
                PropertiesUtils.loadPropertiesFromFile(connectionPropertiesFile);
            }
            catch (IOException e) {
                throw new IllegalStateException ("Cannot load properties file.", e.getCause());
            }
        }

        if (minConnectionJitter > maxConnectionJitter) {
            throw new IllegalStateException("Minimum jitter must be less than or equal to maximum jitter.");
        }
    }

    protected ConnectionFactory createConnectionFactory() {
        return new ConnectionFactory(
            connectionProperties,
            connectionPropertiesFile,
            minConnectionJitter,
            maxConnectionJitter);
    }
}
