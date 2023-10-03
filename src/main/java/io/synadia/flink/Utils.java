// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink;

import io.nats.client.NUID;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public abstract class Utils {
    /**
     * Current version of the library
     */
    public static final String CLIENT_VERSION;

    static {
        String cv;
        try { cv = Utils.class.getPackage().getImplementationVersion(); }
        catch (Exception ignore) { cv = null; }
        if (cv == null) {
            try {
                List<String> lines = Files.readAllLines(new File("build.gradle").toPath());
                for (String l : lines) {
                    if (l.startsWith("def jarVersion")) {
                        int at = l.indexOf('"');
                        int lat = l.lastIndexOf('"');
                        cv = l.substring(at + 1, lat) + ".dev";
                        break;
                    }
                }
            }
            catch (Exception ignore) {}
        }
        CLIENT_VERSION = cv == null ? "development" : cv;
    }

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
     * The method returns {@code null} if the property is not found.
     * @param key the property key
     * @return the list represented by the key or null if the key is not found
     */
    public static List<String> getPropertyAsList(Properties properties, String key) {
        String val = properties.getProperty(key);
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

    public static int getIntProperty(Properties properties, String key, int dflt) {
        try {
            String temp = properties.getProperty(key, "" + dflt);
            return Integer.parseInt(temp);
        }
        catch (Exception e) {
            return dflt;
        }
    }

    public static long getLongProperty(Properties properties, String key, long dflt) {
        try {
            String temp = properties.getProperty(key, "" + dflt);
            return Long.parseLong(temp);
        }
        catch (Exception e) {
            return dflt;
        }
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

    public static String generateId() {
        return NUID.nextGlobal().substring(0, 4);
    }

    public static String generatePrefixedId(String prefix) {
        String temp = NUID.nextGlobal();
        return prefix + "-" + temp.substring(temp.length() - 5);
    }
}
