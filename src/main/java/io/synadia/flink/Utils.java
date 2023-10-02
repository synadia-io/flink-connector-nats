// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink;

import io.nats.client.NUID;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public abstract class Utils {

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

    public static String generateId() {
        return NUID.nextGlobal().substring(0, 4);
    }

    public static String generatePrefixedId(String prefix) {
        String temp = NUID.nextGlobal();
        return prefix + "-" + temp.substring(temp.length() - 5);
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
