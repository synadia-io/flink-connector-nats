// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia;

import org.apache.flink.util.FlinkRuntimeException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public abstract class Utils {

    /**
     * Create and load a properties object from a file.
     * @param path a resolvable path from the location the application is running, etiher relative or absolute
     * @return the properties object loaded from the file
     */
    public static Properties loadPropertiesFromFile(String path) {
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(path));
            return properties;
        }
        catch (IOException e) {
            throw new FlinkRuntimeException("Cannot load properties file: ", e);
        }
    }

    /**
     * Get a list for a key in the properties. Value should be comma delimited, i.e. a,b,c
     * The method returns {@code null} if the property is not found.
     * @param key the property key
     * @return the list represented by the key or null if the key is not found
     */
    public static List<String> getPropertyAsList(Properties properties, String key) {
        String val = properties.getProperty(key);
        if (val == null) {
            return null;
        }
        return Arrays.asList(val.split(","));
    }
}
