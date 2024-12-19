// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.utils;

import io.nats.client.Connection;
import io.nats.client.JetStreamOptions;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Properties;

import static io.synadia.flink.v0.utils.Constants.*;
import static io.synadia.flink.v0.utils.PropertiesUtils.*;

public class ConnectionFactory implements Serializable {
    static String[] JSO_PROPERTY_PREFIXES = new String[]{NO_PREFIX, NATS_PREFIX};

    private final Properties connectionProperties;
    private final String connectionPropertiesFile;
    private final long minConnectionJitter;
    private final long maxConnectionJitter;

    public ConnectionFactory(Properties connectionProperties) {
        this(connectionProperties, null, 0, 0);
    }

    public ConnectionFactory(Properties connectionProperties, long minConnectionJitter, long maxConnectionJitter) {
        this(connectionProperties, null, minConnectionJitter, maxConnectionJitter);
    }

    public ConnectionFactory(String connectionPropertiesFile) {
        this(null, connectionPropertiesFile, 0, 0);
    }

    public ConnectionFactory(String connectionPropertiesFile, long minConnectionJitter, long maxConnectionJitter) {
        this(null, connectionPropertiesFile, minConnectionJitter, maxConnectionJitter);
    }

    public ConnectionFactory(Properties connectionProperties, String connectionPropertiesFile, long minConnectionJitter, long maxConnectionJitter) {
        this.connectionProperties = connectionProperties;
        this.connectionPropertiesFile = connectionPropertiesFile;
        this.minConnectionJitter = minConnectionJitter;
        this.maxConnectionJitter = maxConnectionJitter;
    }

    public Connection connect() throws IOException {
        return connectContext().connection;
    }

    public ConnectionContext connectContext() throws IOException {
        Options.Builder builder = new Options.Builder();
        Properties props = connectionProperties;
        if (connectionPropertiesFile != null) {
            props = loadPropertiesFromFile(connectionPropertiesFile);
        }
        builder = builder.properties(props);

        try {
            Options options = builder.maxReconnects(0).build();
            jitter(minConnectionJitter, maxConnectionJitter);

            return new ConnectionContext(Nats.connect(options), getJetStreamOptions(props));
        }
        catch (Exception e) {
            throw new IOException("Cannot connect to NATS server.", e);
        }
    }

    private JetStreamOptions getJetStreamOptions(Properties props) throws IOException {
        JetStreamOptions.Builder b = JetStreamOptions.builder();
        long rtMillis = getLongProperty(props, JSO_REQUEST_TIMEOUT, 0, JSO_PROPERTY_PREFIXES);
        if (rtMillis > 0) {
            b.requestTimeout(Duration.ofMillis(rtMillis));
        }
        String temp = getStringProperty(props, JSO_PREFIX, JSO_PROPERTY_PREFIXES);
        if (temp != null) {
            b.prefix(temp);
        }
        else {
            temp = getStringProperty(props, JSO_DOMAIN, JSO_PROPERTY_PREFIXES);
            if (temp != null) {
                b.domain(temp);
            }
        }
        return b.build();
    }

    /**
     * Get the connection properties
     * @return a copy of the properties object
     */
    public Properties getConnectionProperties() {
        return new Properties(connectionProperties);
    }

    /**
     * Get the connection properties file
     * @return the properties file string
     */
    public String getConnectionPropertiesFile() {
        return connectionPropertiesFile;
    }

    /**
     * Get the min jitter setting
     * @return the min jitter
     */
    public long getMinConnectionJitter() {
        return minConnectionJitter;
    }

    /**
     * Get the max jitter setting
     * @return the max jitter
     */
    public long getMaxConnectionJitter() {
        return maxConnectionJitter;
    }

    @Override
    public String toString() {
        String c = connectionPropertiesFile == null
            ? ("connectionProperties=" + connectionProperties)
            : ("connectionPropertiesFile='" + connectionPropertiesFile + '\'');

        return "ConnectionFactory{" + c +
            ", minConnectionJitter=" + minConnectionJitter +
            ", maxConnectionJitter=" + maxConnectionJitter +
            '}';
    }
}
