// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.utils;

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.synadia.flink.utils.MiscUtils.getInputStream;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public abstract class PropertiesUtils {
    /**
     * Create and load a Properties object from a file.
     * @param propertiesFilePath a resolvable path to a file from the location the application is running, either relative or absolute
     * @throws IOException if there is an exception opening the properties file
     * @return the Properties object loaded from the file
     */
    public static Properties loadPropertiesFromFile(String propertiesFilePath) throws IOException {
        Properties properties = new Properties();
        properties.load(getInputStream(propertiesFilePath));
        return properties;
    }

    /**
     * Get a list for a key in the properties. Value should be comma delimited, i.e. a,b,c
     * The method returns an empty list if the property is not found.
     * @param properties the properties object
     * @param key the property key
     * @return the list represented by the key or an empty list if the key is not found
     */
    public static List<String> getPropertyAsList(Properties properties, String key) {
        return getAsList(getStringProperty(properties, key));
    }

    public static List<String> getAsList(String val) {
        if (val == null) {
            return Collections.emptyList();
        }
        String[] split = val.split(",");
        List<String> list = new ArrayList<>();
        for (String s : split) {
            String trim = s.trim();
            if (!trim.isEmpty()) {
                list.add(trim);
            }
        }
        return list;
    }

    /**
     * Get a string property
     * The method returns {@code null} if the property is not found.
     * @param properties the properties object
     * @param key the property key
     * @return the string represented by the key or null if the property is not found
     */
    public static String getStringProperty(Properties properties, String key) {
        return properties.getProperty(key, null);
    }

    /**
     * Get an int property or the default if the property is not found.
     * @param properties the properties object
     * @param key the property key
     * @param dflt the default value if the property is not found
     * @return the int represented by the key or the default if the property is not found
     */
    public static int getIntProperty(Properties properties, String key, int dflt) {
        String temp = getStringProperty(properties, key);
        return temp == null ? dflt : Integer.parseInt(temp);
    }

    /**
     * Get a long property or the default if the property is not found.
     * @param properties the properties object
     * @param key the property key
     * @param dflt the default value if the property is not found
     * @return the long represented by the key or the default if the property is not found
     */
    public static long getLongProperty(Properties properties, String key, long dflt) {
        String temp = getStringProperty(properties, key);
        return temp == null ? dflt : Long.parseLong(temp);
    }

    /**
     * Get a boolean property or the default if the property is not found.
     * <p>true or t (case-insensitive) or 1 resolve to true
     * <p>false or f (case-insensitive) or 0 resolve to false
     * <p>all other values, i.e. empty string, other string are the same as not property not found
     * @param properties the properties object
     * @param key the property key
     * @param dflt the default value if the property is not found
     * @return the boolean represented by the key or the default if the property is not found
     */
    public static boolean getBooleanProperty(Properties properties, String key, boolean dflt) {
        String temp = getStringProperty(properties, key);
        return "true".equalsIgnoreCase(temp) || "t".equalsIgnoreCase(temp) || "1".equals(temp) || dflt;
    }

    /**
     * Get a Duration property or the default if the property is not found.
     * Expects a number that represents milliseconds
     * @param properties the properties object
     * @param key the property key
     * @param dflt the default value if the property is not found
     * @return the Duration represented by the key or the default if the property is not found
     */
    public static Duration getDurationProperty(Properties properties, String key, Duration dflt) {
        return Duration.ofMillis(getLongProperty(properties, key, dflt.toMillis()));
    }
}
