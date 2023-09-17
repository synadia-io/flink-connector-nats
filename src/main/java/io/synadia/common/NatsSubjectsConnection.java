// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.common;

import java.util.List;
import java.util.Properties;

public class NatsSubjectsConnection {
    protected final List<String> subjects;
    protected final Properties connectionProperties;
    protected final String connectionPropertiesFile;
    protected final long minConnectionJitter;
    protected final long maxConnectionJitter;

    protected NatsSubjectsConnection(List<String> subjects,
                           Properties connectionProperties,
                           String connectionPropertiesFile,
                           long minConnectionJitter,
                           long maxConnectionJitter
    ) {
        this.subjects = subjects;
        this.connectionProperties = connectionProperties;
        this.connectionPropertiesFile = connectionPropertiesFile;
        this.minConnectionJitter = minConnectionJitter;
        this.maxConnectionJitter = maxConnectionJitter;
    }

    /**
     * Get the subjects registered for this sink.
     * @return the subjects.
     */
    public List<String> getSubjects() {
        return subjects;
    }

    /**
     * Get the connection properties registered for this sink
     * @return a copy of the properties object
     */
    public Properties getConnectionProperties() {
        return new Properties(connectionProperties);
    }

    /**
     * Get the connection properties file registered for this sink
     * @return the properties file string
     */
    public String getConnectionPropertiesFile() {
        return connectionPropertiesFile;
    }

    /**
     * Get the min jitter setting
     * @return the min jitter
     */
    public long getminConnectionJitter() {
        return minConnectionJitter;
    }

    /**
     * Get the max jitter setting
     * @return the max jitter
     */
    public long getmaxConnectionJitter() {
        return maxConnectionJitter;
    }
}
