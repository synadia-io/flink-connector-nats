// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.utils;

import io.nats.client.NUID;
import io.nats.client.support.JsonSerializable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;

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

    public static final String SEP = "--";
    public static final String NULL_SEGMENT = "na";

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

    public static String generateId() {
        return new NUID().next().substring(0, 4).toLowerCase();
    }

    public static String generatePrefixedId(String prefix) {
        return prefix + "-" + generateId();
    }

    public static boolean provided(String s) {
        return s != null && !s.isEmpty();
    }

    public static boolean notProvided(String s) {
        return s == null || s.isEmpty();
    }

    public static boolean provided(Collection<?> c) {
        return c != null && !c.isEmpty();
    }

    public static boolean notProvided(Collection<?> c) {
        return c == null || c.isEmpty();
    }

    public static String random() {
        return NUID.nextGlobalSequence();
    }

    public static String random(String prefix) {
        return prefix + "-" + NUID.nextGlobalSequence();
    }

    public static <T> TypeInformation<T> getTypeInformation(Class<T> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        List<PojoField> pojoFields = new ArrayList<>(fields.length);
        for (Field field : fields) {
            pojoFields.add(new PojoField(field, BasicTypeInfo.of(field.getType())));
        }
        return new PojoTypeInfo<T>(clazz, pojoFields);
    }

    public static String getClassName(Object o) {
        return o.getClass().getName();
    }

    public static Object createInstanceOf(String className) throws ReflectiveOperationException {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor = clazz.getConstructor();
        return constructor.newInstance();
    }

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

    public static InputStream getInputStream(String filespec) throws IOException {
        return Files.newInputStream(Paths.get(filespec));
    }

    public static byte[] readAllBytes(String filespec) throws IOException {
        return Files.readAllBytes(Paths.get(filespec));
    }

    /**
     * Compute the element queue capacity for a source reader.
     * Returns max(sourceQueueCapacity, {@link #getFloorCapacity(SourceReaderContext)}) —
     * so an explicit value above the floor is honored, anything below falls
     * back to the floor (the Flink-configured ELEMENT_QUEUE_CAPACITY, or its
     * compile-time default when unset).
     */
    public static int figureCapacity(SourceReaderContext readerContext, int sourceQueueCapacity) {
        return Math.max(sourceQueueCapacity, getFloorCapacity(readerContext));
    }

    /**
     * The floor used by {@link #figureCapacity}. Reads ELEMENT_QUEUE_CAPACITY
     * from the reader context's Configuration when present; otherwise returns
     * the option's compile-time default.
     */
    public static int getFloorCapacity(SourceReaderContext readerContext) {
        int floor = SourceReaderOptions.ELEMENT_QUEUE_CAPACITY.defaultValue();
        if (readerContext != null
            && readerContext.getConfiguration() != null
            && readerContext.getConfiguration().contains(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY)) {
            floor = readerContext.getConfiguration().get(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY);
        }
        return floor;
    }
}
