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

import static io.nats.client.support.Validator.emptyAsNull;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public class ConnectionFactory implements Serializable {
    /**
     * The properties
     */
    private final Properties connectionProperties;

    /**
     * the file specification for the properties file
     */
    private final String connectionPropertiesFile;

    /**
     * Construct a connection factory
     * @param connectionProperties properties to use for constructions
     */
    public ConnectionFactory(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
        this.connectionPropertiesFile = null;
    }

    /**
     /**
     * Construct a connection factory
     * @param connectionPropertiesFile the file specification for the properties file
     */
    public ConnectionFactory(String connectionPropertiesFile) {
        this.connectionProperties = null;
        this.connectionPropertiesFile = connectionPropertiesFile;
    }

    /**
     * connect / get the connection
     * @return the connection
     * @throws IOException if an IO error getting the connection
     */
    public Connection connect() throws IOException {
        return getConnectionContext().connection;
    }

    /**
     * get the connection context
     * @return the connection context
     * @throws IOException if an IO error getting the connection
     */
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
        if (props == null) {
            b.maxReconnects(0);
        }
        else {
            // this is a workaround b/c b.properties(props) will set the value
            // to the JNats client default if the property is not supplied.
            // This will be fixed in the client and then this code will be updated
            // and the workaround removed.
            String maxReconnects = getPropertyValue(props, Options.PROP_MAX_RECONNECT);
            if (maxReconnects == null) {
                props.setProperty(Options.PROP_MAX_RECONNECT, "0");
            }
            b.properties(props);
        }
        return b.build();
    }

    static final String PFX = "io.nats.client.";
    static final int PFX_LEN = PFX.length();

    private static String getPropertyValue(Properties props, String key) {
        String value = emptyAsNull(props.getProperty(key));
        if (value != null) {
            return value;
        }
        if (key.startsWith(PFX)) { // if the key starts with the PFX, check the non PFX
            return emptyAsNull(props.getProperty(key.substring(PFX_LEN)));
        }
        // otherwise check with the PFX
        value = emptyAsNull(props.getProperty(PFX + key));
        if (value == null && key.contains("_")) {
            // addressing where underscore was used in a key value instead of dot
            return getPropertyValue(props, key.replace("_", "."));
        }
        return value;
    }

    private static JetStreamOptions getJetStreamOptions(Properties props) {
        if (props == null) {
            return JetStreamOptions.DEFAULT_JS_OPTIONS;
        }

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
