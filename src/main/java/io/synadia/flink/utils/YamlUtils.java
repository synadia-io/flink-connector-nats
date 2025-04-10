// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.utils;

import io.nats.client.support.DateTimeUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class YamlUtils {

    public interface YamlValueSupplier<Object> {
        Object get(Map<String, Object> v);
    }

    public static Object readObject(Map<String, Object> map, String key) {
        return map == null ? null : map.get(key);
    }

    public static Map<String, Object> readMap(Map<String, Object> map, String key) {
        Object o = readObject(map, key);
        //noinspection unchecked
        return o instanceof Map ? (Map<String, Object>) o : null;
    }

    public static List<Map<String, Object>> readArray(Map<String, Object> map, String key) {
        Object o = readObject(map, key);
        //noinspection unchecked
        return o instanceof ArrayList ? (ArrayList<Map<String, Object>>) o : null;
    }

    public static String readString(Map<String, Object> map, String key) {
        return readString(map, key, null);
    }

    public static String readString(Map<String, Object> map, String key, String dflt) {
        Object o = readObject(map, key);
        return o instanceof String ? (String) o : dflt;
    }

    public static String readStringEmptyAsNull(Map<String, Object> map, String key) {
        String s = readString(map, key);
        return s == null || s.isEmpty() ? null : s;
    }

    public static Boolean readBoolean(Map<String, Object> map, String key) {
        return readBoolean(map, key, null);
    }

    public static Boolean readBoolean(Map<String, Object> map, String key, Boolean dflt) {
        Object o = readObject(map, key);
        return o instanceof Boolean ? (Boolean) o : dflt;
    }

    public static ZonedDateTime readDate(Map<String, Object> map, String key) {
        String s = readString(map, key);
        return s == null ? null : DateTimeUtils.parseDateTimeThrowParseError(s);
    }

    public static Integer readInteger(Map<String, Object> map, String key) {
        Object o = readObject(map, key);
        return o instanceof Number ? ((Number)o).intValue() : null;
    }

    public static int readInteger(Map<String, Object> map, String key, int dflt) {
        Object o = readObject(map, key);
        return o instanceof Number ? ((Number)o).intValue() : dflt;
    }

    public static Long readLong(Map<String, Object> map, String key) {
        Object o = readObject(map, key);
        return o instanceof Number ? ((Number)o).longValue() : null;
    }

    public static long readLong(Map<String, Object> map, String key, long dflt) {
        Object o = readObject(map, key);
        return o instanceof Number ? ((Number)o).longValue() : dflt;
    }
}
