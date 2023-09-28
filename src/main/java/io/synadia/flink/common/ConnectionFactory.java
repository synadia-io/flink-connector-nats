package io.synadia.flink.common;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import static io.synadia.flink.Utils.jitter;
import static io.synadia.flink.Utils.loadPropertiesFromFile;

public class ConnectionFactory implements Serializable {
    private final Properties connectionProperties;
    private final String connectionPropertiesFile;
    private final long minConnectionJitter;
    private final long maxConnectionJitter;

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
}
