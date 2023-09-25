// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.common;

import io.synadia.flink.Utils;

import java.io.IOException;
import java.util.Properties;

public abstract class NatsConnectionBuilder<BuilderT> {
    protected Properties connectionProperties;
    protected String connectionPropertiesFile;
    protected long minConnectionJitter = 0;
    protected long maxConnectionJitter = 0;

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
    
    protected void beforeBuild() {
        // must have one or the other
        if (connectionProperties == null && connectionPropertiesFile == null) {
            throw new IllegalStateException ("Sink properties or propertiesFile must be provided.");
        }

        // if there is a file, we must be able to load it
        // I considered not loading it and letting it only be necessary when the writer needs it,
        // but wanted it to fail before execution
        if (connectionPropertiesFile != null) {
            try {
                Utils.loadPropertiesFromFile(connectionPropertiesFile);
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
