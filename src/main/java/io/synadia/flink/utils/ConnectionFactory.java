// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

import io.nats.client.Connection;
import io.nats.client.JetStreamOptions;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

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
        return connectContext().connection;
    }

    public ConnectionContext connectContext() throws IOException {
        Properties props = connectionProperties;
        if (connectionPropertiesFile != null) {
            props = PropertiesUtils.loadPropertiesFromFile(connectionPropertiesFile);
        }

        long jitter = PropertiesUtils.getLongProperty(props, Constants.CONNECT_JITTER, 0);
        if (jitter > 0) {
            try {
                long sleep = ThreadLocalRandom.current().nextLong(jitter) + 1;
                Thread.sleep(sleep);
            }
            catch (InterruptedException e) {
                throw new FlinkRuntimeException(e);
            }
        }

        try {
            Options options = getOptions(props);
            return new ConnectionContext(Nats.connect(options), getJetStreamOptions(props));
        }
        catch (Exception e) {
            throw new IOException("Cannot connect to NATS server.", e);
        }
    }

    private static Options getOptions(Properties props) {
        return Options.builder().properties(props).maxReconnects(0).build();
    }

    private static JetStreamOptions getJetStreamOptions(Properties props) {
        JetStreamOptions.Builder b = JetStreamOptions.builder();
        long rtMillis = PropertiesUtils.getLongProperty(props, Constants.JSO_REQUEST_TIMEOUT, 0);
        if (rtMillis > 0) {
            b.requestTimeout(Duration.ofMillis(rtMillis));
        }
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
