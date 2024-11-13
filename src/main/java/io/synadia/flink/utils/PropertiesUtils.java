// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.utils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public abstract class PropertiesUtils {
    public static final String NO_PREFIX = "";

    /**
     * Create and load a properties object from a file.
     * @param propertiesFilePath a resolvable path to a file from the location the application is running, either relative or absolute
     * @return the properties object loaded from the file
     */
    public static Properties loadPropertiesFromFile(String propertiesFilePath) throws IOException {
        Properties properties = new Properties();
        properties.load(Files.newInputStream(Paths.get(propertiesFilePath)));
        return properties;
    }

    /**
     * Get a list for a key in the properties. Value should be comma delimited, i.e. a,b,c
     * The method returns an empty list if the property is not found.
     * @param properties the properties object
     * @param key the property key
     * @param prefixes optional strings to prefix the key with
     * @return the list represented by the key or an empty list if the key is not found
     */
    public static List<String> getPropertyAsList(Properties properties, String key, String... prefixes) {
        String val = getStringProperty(properties, key, prefixes);
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
     * @param prefixes optional strings to prefix the key with
     * @return the string represented by the key or null if the property is not found
     */
    public static String getStringProperty(Properties properties, String key, String... prefixes) {
        prefixes = ensureAtleastNoPrefix(prefixes);
        for (String prefix : prefixes) {
            try {
                String temp = properties.getProperty(prefix + key, null);
                if (temp != null) {
                    return temp;
                }
            }
            catch (Exception ignored) {}
        }
        return null;
    }

    /**
     * Get an int property or the default if the property is not found.
     * @param properties the properties object
     * @param key the property key
     * @param dflt the default value if the property is not found
     * @param prefixes optional strings to prefix the key with
     * @return the int represented by the key or the default if the property is not found
     */
    public static long getIntProperty(Properties properties, String key, int dflt, String... prefixes) {
        prefixes = ensureAtleastNoPrefix(prefixes);
        for (String prefix : prefixes) {
            try {
                String temp = properties.getProperty(prefix + key, NO_PREFIX + dflt);
                if (temp != null) {
                    return Integer.parseInt(temp);
                }
            }
            catch (Exception ignored) {}
        }
        return dflt;
    }

    /**
     * Get a long property or the default if the property is not found.
     * @param properties the properties object
     * @param key the property key
     * @param dflt the default value if the property is not found
     * @param prefixes optional strings to prefix the key with
     * @return the long represented by the key or the default if the property is not found
     */
    public static long getLongProperty(Properties properties, String key, long dflt, String... prefixes) {
        prefixes = ensureAtleastNoPrefix(prefixes);
        for (String prefix : prefixes) {
            try {
                String temp = properties.getProperty(prefix + key, NO_PREFIX + dflt);
                if (temp != null) {
                    return Long.parseLong(temp);
                }
            }
            catch (Exception ignored) {}
        }
        return dflt;
    }

    /**
     * Get a boolean property or the default if the property is not found.
     * <p>true or t (case-insensitive) or 1 resolve to true
     * <p>false or f (case-insensitive) or 0 resolve to false
     * <p>all other values, i.e. empty string, other string are the same as not property not found
     * @param properties the properties object
     * @param key the property key
     * @param dflt the default value if the property is not found
     * @param prefixes optional strings to prefix the key with
     * @return the boolean represented by the key or the default if the property is not found
     */
    public static boolean getBooleanProperty(Properties properties, String key, boolean dflt, String... prefixes) {
        prefixes = ensureAtleastNoPrefix(prefixes);
        for (String prefix : prefixes) {
            try {
                String temp = properties.getProperty(prefix + key, NO_PREFIX + dflt);
                if ("true".equalsIgnoreCase(temp) || "t".equalsIgnoreCase(temp) || "1".equals(temp)) {
                    return true;
                }
                if ("false".equalsIgnoreCase(temp) || "f".equalsIgnoreCase(temp) || "0".equals(temp)) {
                    return false;
                }
            }
            catch (Exception ignored) {}
        }
        return dflt;
    }

    /**
     * Get a Duration property or the default if the property is not found.
     * Expects a number that represents milliseconds
     * @param properties the properties object
     * @param key the property key
     * @param dflt the default value if the property is not found
     * @param prefixes optional strings to prefix the key with
     * @return the Duration represented by the key or the default if the property is not found
     */
    public static Duration getDurationProperty(Properties properties, String key, Duration dflt, String... prefixes) {
        return Duration.ofMillis(getLongProperty(properties, key, dflt.toMillis(), prefixes));
    }

    private static String[] ensureAtleastNoPrefix(String[] prefixes) {
        if (prefixes == null || prefixes.length == 0) {
            prefixes = new String[] {NO_PREFIX};
        }
        return prefixes;
    }

    /**
     * Execute a jitter sleep
     * @param minConnectionJitter the minimum jitter
     * @param maxConnectionJitter the maximum jitter
     */
    public static void jitter(long minConnectionJitter, long maxConnectionJitter) {
        long sleep;
        if (maxConnectionJitter <= minConnectionJitter) {
            if (minConnectionJitter < 1) {
                return;
            }
            sleep = ThreadLocalRandom.current().nextLong(minConnectionJitter);
        }
        else {
            sleep = ThreadLocalRandom.current().nextLong(minConnectionJitter, maxConnectionJitter);
        }
        try {
            Thread.sleep(sleep);
        }
        catch (InterruptedException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public static <T> TypeInformation<T> getTypeInformation(Class<T> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        List<PojoField> pojoFields = new ArrayList<>(fields.length);
        for (Field field : fields) {
            pojoFields.add(new PojoField(field, BasicTypeInfo.of(field.getType())));
        }
        return new PojoTypeInfo<T>(clazz, pojoFields);
    }
}
