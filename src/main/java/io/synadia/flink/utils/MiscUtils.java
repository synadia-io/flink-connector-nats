// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.utils;

import io.nats.client.NUID;
import io.nats.client.support.JsonSerializable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public abstract class MiscUtils {

    private static final String SEP = "--";
    private static final String NULL_SEGMENT = "na";

    private MiscUtils() {} /* ensures cannot be constructed */

    /**
     * Current version of the library
     */
    public static final String CLIENT_VERSION;

    static {
        String cv;
        try { cv = MiscUtils.class.getPackage().getImplementationVersion(); }
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
     * Generate an id
     * @return the id
     */
    public static String generateId() {
        return new NUID().next().substring(0, 4).toLowerCase();
    }

    /**
     * Generate an id and prefix it with the prefix
     * @param prefix the prefix
     * @return the prefixed id
     */
    public static String generatePrefixedId(String prefix) {
        return prefix + "-" + generateId();
    }

    /**
     * Is the string not null and not empty?
     * @param s the string
     * @return the result
     */
    public static boolean provided(String s) {
        return s != null && !s.isEmpty();
    }

    /**
     * Is the string null or empty?
     * @param s the string
     * @return the result
     */
    public static boolean notProvided(String s) {
        return s == null || s.isEmpty();
    }

    /**
     * Is the Collection not null and not empty?
     * @param c the collection
     * @return the result
     */
    public static boolean provided(Collection<?> c) {
        return c != null && !c.isEmpty();
    }

    /**
     * Is the Collection null or empty?
     * @param c the collection
     * @return the result
     */
    public static boolean notProvided(Collection<?> c) {
        return c == null || c.isEmpty();
    }

    /**
     * Generate a random string
     * @return the result
     */
    public static String random() {
        return NUID.nextGlobalSequence();
    }

    /**
     * Generate a random string and prefix it with the prefix
     * @param prefix the prefix
     * @return the prefixed random strinig
     */
    public static String random(String prefix) {
        return prefix + "-" + NUID.nextGlobalSequence();
    }

    /**
     * Get the TypeInformation for a class
     * @param clazz the class
     * @return the TypeInformation
     * @param <T> The type
     */
    public static <T> TypeInformation<T> getTypeInformation(Class<T> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        List<PojoField> pojoFields = new ArrayList<>(fields.length);
        for (Field field : fields) {
            pojoFields.add(new PojoField(field, BasicTypeInfo.of(field.getType())));
        }
        return new PojoTypeInfo<T>(clazz, pojoFields);
    }

    /**
     * Get the class name of an object
     * @param o the object
     * @return the class name
     */
    public static String getClassName(Object o) {
        return o.getClass().getName();
    }

    /**
     * create an instance of a class from its name
     * @param className the class name
     * @return the Object
     * @throws ReflectiveOperationException if the class can not be created from the name
     */
    public static Object createInstanceOf(String className) throws ReflectiveOperationException {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor = clazz.getConstructor();
        return constructor.newInstance();
    }

    /**
     * create a crc checksum for a bunch of objects by stringing them together
     * @param parts the parts
     * @return the result
     */
    public static String checksum(Object... parts) {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (Object o : parts) {
            if (first) {
                first = false;
            }
            else {
                sb.append(SEP);
            }
            if (o == null) {
                sb.append(NULL_SEGMENT);
            }
            else if (o instanceof JsonSerializable) {
                sb.append(((JsonSerializable)o).toJson());
            }
            else if (o instanceof ZonedDateTime) {
                sb.append(((ZonedDateTime)o).toEpochSecond());
            }
            else {
                sb.append(o);
            }
        }
        Checksum crc32 = new CRC32();
        byte[] bytes = sb.toString().getBytes();
        crc32.update(bytes, 0, bytes.length);
        return Long.toHexString(crc32.getValue()).toUpperCase();
    }

    /**
     * Utility to get an InputStream for a file specification
     * @param filespec the file specification
     * @return the InputStream
     * @throws IOException any IO exception while trying to create the input stream from the filespec
     */
    public static InputStream getInputStream(String filespec) throws IOException {
        return Files.newInputStream(Paths.get(filespec));
    }

    /**
     * Read all bytes from a file
     * @param filespec the file specification
     * @return the resulting byte array
     * @throws IOException any IO exception while trying to create the input stream from the filespec
     */
    public static byte[] readAllBytes(String filespec) throws IOException {
        return Files.readAllBytes(Paths.get(filespec));
    }
}
