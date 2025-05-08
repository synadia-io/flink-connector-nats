// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.utils;

import io.nats.client.support.DateTimeUtils;
import org.apache.flink.annotation.Internal;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static io.nats.client.support.DateTimeUtils.DEFAULT_TIME;
import static io.nats.client.support.DateTimeUtils.ZONE_ID_GMT;

/**
 * INTERNAL CLASS SUBJECT TO CHANGE
 */
@Internal
public abstract class YamlUtils {

    static final String PAD = "                    ";

    static String getPad(int indentLevel) {
        return indentLevel == 0 ? "" : PAD.substring(0, indentLevel * 2);
    }

    public static StringBuilder beginYaml() {
        return new StringBuilder("---").append(System.lineSeparator());
    }

    public static StringBuilder beginChild(int indentLevel, String key, String value) {
        return new StringBuilder()
            .append(getPad(indentLevel))
            .append("- ")
            .append(key)
            .append(": ")
            .append(value)
            .append(System.lineSeparator());
    }

    private static void _addField(StringBuilder sb, int indentLevel, String key, String value) {
        sb.append(getPad(indentLevel))
            .append(key)
            .append(": ")
            .append(value)
            .append(System.lineSeparator());
    }

    public static void addField(StringBuilder sb, int indentLevel, String key) {
        _addField(sb, indentLevel, key, "");
    }

    public static void addField(StringBuilder sb, int indentLevel, String key, String value) {
        if (value != null && !value.isEmpty()) {
            _addField(sb, indentLevel, key, value);
        }
    }

    public static void addField(StringBuilder sb, int indentLevel, String key, Integer value) {
        if (value != null && value >= 0) {
            _addField(sb, indentLevel, key, value.toString());
        }
    }

    public static void addField(StringBuilder sb, int indentLevel, String key, Long value) {
        if (value != null && value >= 0) {
            _addField(sb, indentLevel, key, value.toString());
        }
    }

    public static void addFieldGtZero(StringBuilder sb, int indentLevel, String key, Integer value) {
        if (value != null && value > 0) {
            _addField(sb, indentLevel, key, value.toString());
        }
    }

    public static void addFieldGtZero(StringBuilder sb, int indentLevel, String key, Long value) {
        if (value != null && value > 0) {
            _addField(sb, indentLevel, key, value.toString());
        }
    }

    public static void addField(StringBuilder sb, int indentLevel, String key, ZonedDateTime zonedDateTime) {
        if (zonedDateTime != null && !DEFAULT_TIME.equals(zonedDateTime)) {
            _addField(sb, indentLevel, key, "'" + DateTimeUtils.toRfc3339(zonedDateTime) + "'");
        }
    }

    public static void addFldWhenTrue(StringBuilder sb, int indentLevel, String key, boolean value) {
        if (value) {
            _addField(sb, indentLevel, key, "true");
        }
    }

    public static void addStrings(StringBuilder sb, int indentLevel, String key, List<String> values) {
        sb.append(getPad(indentLevel))
            .append(key)
            .append(":")
            .append(System.lineSeparator());
        String pad = getPad(indentLevel + 1);
        for (String value : values) {
            sb.append(pad).append("- ").append(value).append(System.lineSeparator());
        }
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
        Object o = readObject(map, key);
        if (o instanceof Date) {
            return ((Date)o).toInstant().atZone(ZONE_ID_GMT);
        }
        if (o instanceof String) {
            return DateTimeUtils.parseDateTimeThrowParseError(o.toString());
        }
        return null;
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

    public static List<String> readStringList(Map<String, Object> map, String key) {
        Object o = readObject(map, key);
        if (o instanceof ArrayList) {
            //noinspection unchecked
            return (ArrayList<String>)o;
        }
        return new ArrayList<>();
    }
}
