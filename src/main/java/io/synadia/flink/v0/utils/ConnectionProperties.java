package io.synadia.flink.v0.utils;

import java.io.Serializable;
import java.util.Properties;

/**
 * Wrapper for connection properties.
 */

public class ConnectionProperties<T> implements Serializable {
    private final T t;

    public ConnectionProperties(T t) {
        if (t == null) {
            throw new IllegalArgumentException("Connection properties cannot be null");
        }

        if (!(t instanceof Properties) && !(t instanceof String)) {
            throw new IllegalArgumentException("Connection properties must be a Properties object or a String File name");
        }

        this.t = t;
    }

    public Properties getProperties() {
        if (t instanceof Properties) {
            return (Properties) t;
        }
        return null;
    }

    public String getFile() {
        if (t instanceof String) {
            return (String) t;
        }
        return null;
    }

    @Override
    public String toString() {
        Properties properties = getProperties();
        String file = getFile();

        if (properties != null) {
            return "ConnectionProperties{" +
                    "properties=" + properties +
                    '}';
        }

        if (file != null) {
            return "ConnectionProperties{" +
                    "file=" + file +
                    '}';
        }

        return "ConnectionProperties{" + '}';
    }
}
