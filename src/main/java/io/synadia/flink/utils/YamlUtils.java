// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.utils;

import io.nats.client.support.DateTimeUtils;
import org.apache.flink.annotation.Internal;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
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
    private YamlUtils() {} /* ensures cannot be constructed */

    static final String PAD = "                    ";

    static String getPad(int indentLevel) {
        return indentLevel == 0 ? "" : PAD.substring(0, indentLevel * 2);
    }

    /**
     * Start (Begin) a String Builder where YAML will be built
     * @return the string builder
     */
    public static StringBuilder beginYaml() {
        return new StringBuilder("---").append(System.lineSeparator());
    }

    /**
     * begin a YAML child
     * @param indentLevel the indent level
     * @param key the YAML key for the value
     * @param value the value
     * @return the string builder
     */
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

    /**
     * add an empty string value
     * @param sb the string builder
     * @param indentLevel the indent level
     * @param key the YAML key for the value
     */
    public static void addField(StringBuilder sb, int indentLevel, String key) {
        _addField(sb, indentLevel, key, "");
    }

    /**
     * add a string value if it's not null or empty
     * @param sb the string builder
     * @param indentLevel the indent level
     * @param key the YAML key for the value
     * @param value the value
     */
    public static void addField(StringBuilder sb, int indentLevel, String key, String value) {
        if (value != null && !value.isEmpty()) {
            _addField(sb, indentLevel, key, value);
        }
    }

    /**
     * add an Integer value if it's not null and is greater than or equal to 0
     * @param sb the string builder
     * @param indentLevel the indent level
     * @param key the YAML key for the value
     * @param value the value
     */
    public static void addField(StringBuilder sb, int indentLevel, String key, Integer value) {
        if (value != null && value >= 0) {
            _addField(sb, indentLevel, key, value.toString());
        }
    }

    /**
     * add a Long value if it's not null and is greater than or equal to 0
     * @param sb the string builder
     * @param indentLevel the indent level
     * @param key the YAML key for the value
     * @param value the value
     */
    public static void addField(StringBuilder sb, int indentLevel, String key, Long value) {
        if (value != null && value >= 0) {
            _addField(sb, indentLevel, key, value.toString());
        }
    }

    /**
     * add an Integer value if it's not null and is greater than 0
     * @param sb the string builder
     * @param indentLevel the indent level
     * @param key the YAML key for the value
     * @param value the value
     */
    public static void addFieldGtZero(StringBuilder sb, int indentLevel, String key, Integer value) {
        if (value != null && value > 0) {
            _addField(sb, indentLevel, key, value.toString());
        }
    }

    /**
     * add a Long value if it's not null and is greater than 0
     * @param sb the string builder
     * @param indentLevel the indent level
     * @param key the YAML key for the value
     * @param value the value
     */
    public static void addFieldGtZero(StringBuilder sb, int indentLevel, String key, Long value) {
        if (value != null && value > 0) {
            _addField(sb, indentLevel, key, value.toString());
        }
    }

    /**
     * add a Duration as the number of nanos if not null and is greater than 0
     * @param sb the string builder
     * @param indentLevel the indent level
     * @param key the YAML key for the value
     * @param value the value
     */
    public static void addFieldAsNanos(StringBuilder sb, int indentLevel, String key, Duration value) {
        if (value != null && !value.isZero() && !value.isNegative()) {
            _addField(sb, indentLevel, key, String.valueOf(value.toNanos()));
        }
    }

    /**
     * add a Long value if it's not null and is not the default time constant
     * @param sb the string builder
     * @param indentLevel the indent level
     * @param key the YAML key for the value
     * @param value the value
     */
    public static void addField(StringBuilder sb, int indentLevel, String key, ZonedDateTime value) {
        if (value != null && !DEFAULT_TIME.equals(value)) {
            _addField(sb, indentLevel, key, "'" + DateTimeUtils.toRfc3339(value) + "'");
        }
    }

    /**
     * add when the value is true
     * @param sb the string builder
     * @param indentLevel the indent level
     * @param value the value
     * @param key the YAML key for the value
     */
    public static void addFldWhenTrue(StringBuilder sb, int indentLevel, String key, boolean value) {
        if (value) {
            _addField(sb, indentLevel, key, "true");
        }
    }

    /**
     * add the enum string if it's not null and if it doesn't match the enum match
     * @param sb the string builder
     * @param indentLevel the indent level
     * @param fieldName the YAML key for the value
     * @param dontAddIfThis the enum that indicates what value is not added
     * @param e the enum
     * @param <E> the enum type
     */
    public static <E extends Enum<E>> void addEnumWhenNot(StringBuilder sb, int indentLevel, @NonNull String fieldName, @Nullable E e, @Nullable E dontAddIfThis) {
        if (e != null && e != dontAddIfThis) {
            _addField(sb, indentLevel, fieldName, e.toString());
        }
    }


    /**
     * add a list of strings
     * @param sb the string builder
     * @param indentLevel the indent level
     * @param key the YAML key for the value
     * @param values The list of string values
     */
    public static void addStrings(StringBuilder sb, int indentLevel, String key, @NonNull List<String> values) {
        sb.append(getPad(indentLevel))
            .append(key)
            .append(":")
            .append(System.lineSeparator());
        String pad = getPad(indentLevel + 1);
        for (String value : values) {
            sb.append(pad).append("- ").append(value).append(System.lineSeparator());
        }
    }

    /**
     * Read a value as an object from a map
     * @param map the map
     * @param key the YAML key for the value
     * @return the Object
     */
    public static Object readObject(Map<String, Object> map, String key) {
        return map == null ? null : map.get(key);
    }

    /**
     * Read a value as a map from a map
     * @param map the map
     * @param key the YAML key for the value
     * @return the map
     */
    public static Map<String, Object> readMap(Map<String, Object> map, String key) {
        Object o = readObject(map, key);
        //noinspection unchecked
        return o instanceof Map ? (Map<String, Object>) o : null;
    }

    /**
     * Read a value as a list of maps from a map
     * @param map the map
     * @param key the YAML key for the value
     * @return the list of maps
     */
    public static List<Map<String, Object>> readArray(Map<String, Object> map, String key) {
        Object o = readObject(map, key);
        //noinspection unchecked
        return o instanceof ArrayList ? (ArrayList<Map<String, Object>>) o : null;
    }

    /**
     * Read a value as a list of strings from a map
     * @param map the map
     * @param key the YAML key for the value
     * @return the list
     */
    public static List<String> readArrayAsStrings(Map<String, Object> map, String key) {
        Object o = readObject(map, key);
        List<String> list = new ArrayList<>();
        if (o instanceof ArrayList) {
            List<?> l = (List<?>)o;
            for (Object o2 : l) {
                list.add(o2.toString());
            }
        }
        return list;
    }

    /**
     * Read a value as a string from a map
     * @param map the map
     * @param key the YAML key for the value
     * @return the string
     */
    public static String readString(Map<String, Object> map, String key) {
        return readString(map, key, null);
    }

    /**
     * Read a value as a string from a map and return a default if the key is not found
     * @param map the map
     * @param key the YAML key for the value
     * @param dflt the default string
     * @return the string or default
     */
    public static String readString(Map<String, Object> map, String key, String dflt) {
        Object o = readObject(map, key);
        return o instanceof String ? (String) o : dflt;
    }

    /**
     * Read a value as a string from a map and return a null default if the key is not found or is empty
     * @param map the map
     * @param key the YAML key for the value
     * @return the string or null
     */
    public static String readStringEmptyAsNull(Map<String, Object> map, String key) {
        String s = readString(map, key);
        return s == null || s.isEmpty() ? null : s;
    }

    /**
     * Read a value as a boolean from a map
     * @param map the map
     * @param key the YAML key for the value
     * @return the Boolean value or null
     */
    public static Boolean readBoolean(Map<String, Object> map, String key) {
        return readBoolean(map, key, null);
    }

    /**
     * Read a value as a boolean from a map or the default if not found
     * @param map the map
     * @param key the YAML key for the value
     * @param dflt the default boolean
     * @return the Boolean value or the default
     */
    public static Boolean readBoolean(Map<String, Object> map, String key, Boolean dflt) {
        Object o = readObject(map, key);
        return o instanceof Boolean ? (Boolean) o : dflt;
    }

    /**
     * Read a value as a ZonedDateTime from a map
     * @param map the map
     * @param key the YAML key for the value
     * @return the ZoneDate time if found otherwise null
     * @throws DateTimeParseException if the text cannot be parsed
     */
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

    /**
     * Read a value as an Integer from a map
     * @param map the map
     * @param key the YAML key for the value
     * @return the Integer if found and is a number otherwise null
     */
    public static Integer readInteger(Map<String, Object> map, String key) {
        Object o = readObject(map, key);
        return o instanceof Number ? ((Number)o).intValue() : null;
    }

    /**
     * Read a value as an Integer from a map or the default if not found
     * @param map the map
     * @param key the YAML key for the value
     * @param dflt the default int
     * @return the Integer if found and is a number otherwise the default
     */
    public static int readInteger(Map<String, Object> map, String key, int dflt) {
        Object o = readObject(map, key);
        return o instanceof Number ? ((Number)o).intValue() : dflt;
    }

    /**
     * Read a value as a Long from a map
     * @param map the map
     * @param key the YAML key for the value
     * @return the Long if found and is a number otherwise null
     */
    public static Long readLong(Map<String, Object> map, String key) {
        Object o = readObject(map, key);
        return o instanceof Number ? ((Number)o).longValue() : null;
    }

    /**
     * Read a value as a Long from a map or the default if not found
     * @param map the map
     * @param key the YAML key for the value
     * @param dflt the default long
     * @return the Long if found and is a number otherwise the default
     */
    public static long readLong(Map<String, Object> map, String key, long dflt) {
        Object o = readObject(map, key);
        return o instanceof Number ? ((Number)o).longValue() : dflt;
    }

    /**
     * Read a value as a Duration from a map
     * @param map the map
     * @param key the YAML key for the value
     * @return the Duration if found and is a number otherwise null
     */
    public static Duration readNanos(Map<String, Object> map, String key) {
        Long l = readLong(map, key);
        return l == null? null : Duration.ofNanos(l);
    }

    /**
     * Read a value as a Duration from a map or the default if not found
     * @param map the map
     * @param key the YAML key for the value
     * @param dflt the default duration
     * @return the Duration if found and is a number otherwise the default
     */
    public static Duration readNanos(Map<String, Object> map, String key, Duration dflt) {
        Long l = readLong(map, key);
        return l == null ? dflt : Duration.ofNanos(l);
    }
}
