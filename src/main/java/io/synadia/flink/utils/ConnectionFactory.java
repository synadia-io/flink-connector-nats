// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

import io.nats.client.Connection;
import io.nats.client.JetStreamOptions;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class ConnectionFactory implements Serializable {
    private final Properties connectionProperties;
    private final String connectionPropertiesFile;

    public ConnectionFactory(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
        this.connectionPropertiesFile = null;
    }

    public ConnectionFactory(String connectionPropertiesFile) {
        this.connectionProperties = null;
        this.connectionPropertiesFile = connectionPropertiesFile;
    }

    public Connection connect() throws IOException {
        return getConnectionContext().connection;
    }

    public ConnectionContext getConnectionContext() throws IOException {
        try {
            Properties props = connectionProperties;
            if (connectionPropertiesFile != null) {
                props = PropertiesUtils.loadPropertiesFromFile(connectionPropertiesFile);
            }
            Options options = getOptions(props);
            return new ConnectionContext(Nats.connect(options), getJetStreamOptions(props));
        }
        catch (Exception e) {
            throw new IOException("Cannot connect to NATS server.", e);
        }
    }

    private static Options getOptions(Properties props) {
        Options.Builder b = Options.builder();
        if (props != null) {
            b.properties(props);
        }
        return b.maxReconnects(0).build();
    }

    private static JetStreamOptions getJetStreamOptions(Properties props) {
        JetStreamOptions.Builder b = JetStreamOptions.builder();
        String temp = PropertiesUtils.getStringProperty(props, Constants.JSO_PREFIX);
        if (temp != null) {
            b.prefix(temp);
        }
        else {
            temp = PropertiesUtils.getStringProperty(props, Constants.JSO_DOMAIN);
            if (temp != null) {
                b.domain(temp);
            }
        }
        return b.build();
    }

    /**
     * Get the connection properties or null if it was not provided via configuration
     * @return a copy of the connection Properties object
     */
    public Properties getConnectionProperties() {
        return connectionProperties == null ? null : new Properties(connectionProperties);
    }

    /**
     * Get the connection properties file or null if it was not provided via configuration
     * @return the properties file string
     */
    public String getConnectionPropertiesFile() {
        return connectionPropertiesFile;
    }

    @Override
    public String toString() {
        return connectionPropertiesFile == null
            ? ("connectionProperties=" + connectionProperties)
            : ("connectionPropertiesFile='" + connectionPropertiesFile + '\'');
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConnectionFactory)) return false;

        ConnectionFactory that = (ConnectionFactory) o;
        return Objects.equals(connectionProperties, that.connectionProperties)
            && Objects.equals(connectionPropertiesFile, that.connectionPropertiesFile);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(connectionProperties);
        result = 31 * result + Objects.hashCode(connectionPropertiesFile);
        return result;
    }
}
