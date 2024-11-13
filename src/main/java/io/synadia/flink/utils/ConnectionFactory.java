package io.synadia.flink.utils;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import static io.synadia.flink.utils.PropertiesUtils.jitter;
import static io.synadia.flink.utils.PropertiesUtils.loadPropertiesFromFile;

public class ConnectionFactory implements Serializable {
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
        Options.Builder builder = new Options.Builder();
        if (connectionPropertiesFile == null) {
            builder = builder.properties(connectionProperties);
        }
        else {
            builder = builder.properties(loadPropertiesFromFile(connectionPropertiesFile));
        }

        try {
            Options options = builder.maxReconnects(0).build();
            jitter(minConnectionJitter, maxConnectionJitter);
            return Nats.connect(options);
        }
        catch (Exception e) {
            throw new IOException("Cannot connect to NATS server.", e);
        }
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
        return "ConnectionFactory{" +
            "connectionProperties=" + connectionProperties +
            ", connectionPropertiesFile='" + connectionPropertiesFile + '\'' +
            ", minConnectionJitter=" + minConnectionJitter +
            ", maxConnectionJitter=" + maxConnectionJitter +
            '}';
    }
}
