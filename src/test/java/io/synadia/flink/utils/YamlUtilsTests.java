// Copyright 2025 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.synadia.flink.utils;

import io.nats.client.support.DateTimeUtils;
import io.synadia.flink.TestBase;
import io.synadia.flink.source.AckBehavior;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.nats.client.support.DateTimeUtils.DEFAULT_TIME;
import static io.synadia.flink.utils.YamlUtils.*;
import static org.junit.jupiter.api.Assertions.*;

public final class YamlUtilsTests extends TestBase {
    private static final String TEST_YAML = resourceAsString("test.yaml");
    private static final Map<String, Object> TEST_MAP = new Yaml().load(TEST_YAML);
    private static final String STRING_STRING = "Hello";
    private static final String DATE_STRING = "2021-01-25T20:09:10.6225191Z";
    private static final ZonedDateTime TEST_DATE = DateTimeUtils.parseDateTime(DATE_STRING);

    private static final String STRING = "string";
    private static final String INTEGER = "integer";
    private static final String LONG = "long";
    private static final String BOOL = "bool";
    private static final String DATE = "date";
    private static final String NANOS = "nanos";
    private static final String MAP = "map";
    private static final String ARRAY = "array";
    private static final String MMAP = "mmap";
    private static final String SMAP = "smap";
    private static final String SLIST = "slist";
    private static final String MLIST = "mlist";
    private static final String ILIST = "ilist";
    private static final String LLIST = "llist";
    private static final String NLIST = "nlist";
    private static final String NOT_A_KEY = "not-a-key";

    static int LINE_SEP_LEN = System.lineSeparator().length();

    @Test
    public void testAdd() {
        StringBuilder sb = new StringBuilder();

        addField(sb, 0, "n/a", (String) null);
        validateNoChange(0, sb);

        addFldWhenTrue(sb, 0, "n/a", true);
        int last = validateLength(0, 9, 1, sb);

        addField(sb, 0, "n/a", "");
        validateNoChange(last, sb);

        addFldWhenTrue(sb, 0, "n/a", false);
        validateNoChange(last, sb);

        addField(sb, 0, "n/a", (Integer) null);
        validateNoChange(last, sb);

        addField(sb, 0, "n/a", (Long) null);
        validateNoChange(last, sb);

        addField(sb, 0, "iminusone", -1);
        validateNoChange(last, sb);

        addField(sb, 0, "lminusone", -1);
        validateNoChange(last, sb);

        List<String> list = new ArrayList<>();
        list.add("bbb");
        addStrings(sb, 0, "foo", list);
        last = validateLength(last, 11, 2, sb);

        addField(sb, 0, "zero", 0);
        last = validateLength(last, 7, sb);

        addField(sb, 0, "lone", 1);
        last = validateLength(last, 7, sb);

        addField(sb, 0, "lmax", Long.MAX_VALUE);
        last = validateLength(last, 25, sb);

        addFldWhenTrue(sb, 0, "btrue", true);
        last = validateLength(last, 11, sb);

        addFldWhenTrue(sb, 0, "bfalse", false);
        validateNoChange(last, sb);

        addFieldGtZero(sb, 0, "intnull", (Integer) null);
        validateNoChange(last, sb);

        addFieldGtZero(sb, 0, "longnull", (Long) null);
        validateNoChange(last, sb);

        addFieldGtZero(sb, 0, "intnotgt0", 0);
        validateNoChange(last, sb);

        addFieldGtZero(sb, 0, "longnotgt0", 0L);
        validateNoChange(last, sb);

        addFieldGtZero(sb, 0, "intgt0", 1);
        last = validateLength(last, 9, sb);

        addFieldGtZero(sb, 0, "longgt0", 1L);
        last = validateLength(last, 10, sb);

        addField(sb, 0, "zdt", (ZonedDateTime)null);
        validateNoChange(last, sb);

        addField(sb, 0, "zdt", DEFAULT_TIME);
        validateNoChange(last, sb);

        addField(sb, 0, "zdt", DateTimeUtils.gmtNow());
        last = validateLength(last, 37, sb);

        addFieldAsNanos(sb, 0, "nano", null);
        validateNoChange(last, sb);

        addFieldAsNanos(sb, 0, "nano", Duration.ZERO);
        validateNoChange(last, sb);

        addFieldAsNanos(sb, 0, "nano", Duration.ofNanos(-1));
        validateNoChange(last, sb);

        addFieldAsNanos(sb, 0, "nano", Duration.ofNanos(1));
        last = validateLength(last, 7, sb);

        addEnumWhenNot(sb, 0, "enum", null, AckBehavior.NoAck);
        validateNoChange(last, sb);

        addEnumWhenNot(sb, 0, "enum", AckBehavior.NoAck, AckBehavior.NoAck);
        validateNoChange(last, sb);

        addEnumWhenNot(sb, 0, "enum", AckBehavior.NoAck, AckBehavior.AckAll);
        last = validateLength(last, 11, sb);
    }

    private void validateNoChange(int last, StringBuilder sb) {
        validateLength(last, 0, 0, sb);
    }

    private int validateLength(int last, int change, StringBuilder sb) {
        return validateLength(last, change, 1, sb);
    }

    private int validateLength(int last, int change, int numLineSeps, StringBuilder sb) {
        int expected = last + change + (LINE_SEP_LEN * numLineSeps);
        if (expected != sb.length()) {
            System.out.println(sb.length() + " == " + last + " + " + change + " + " + (LINE_SEP_LEN * numLineSeps) + " = " + expected + " \n---\n" + sb.toString() + "\n---");
        }
        assertEquals(expected, sb.length());
        return expected;
    }

    @Test
    public void testRead() {
        assertNotNull(readObject(TEST_MAP, STRING));
        assertNull(readObject(null, NOT_A_KEY));
    }

    @Test
    public void testReadStrings() {
        String s = readString(TEST_MAP, STRING);
        assertEquals(STRING_STRING, s);

        assertEquals(DATE_STRING, readString(TEST_MAP, DATE));

        assertNull(readString(TEST_MAP, INTEGER));
        assertNull(readString(TEST_MAP, LONG));
        assertNull(readString(TEST_MAP, BOOL));
        assertNull(readString(TEST_MAP, MAP));
        assertNull(readString(TEST_MAP, ARRAY));
        assertNull(readString(TEST_MAP, NOT_A_KEY));

        assertEquals(STRING_STRING, readStringEmptyAsNull(TEST_MAP, STRING));
        assertNull(readStringEmptyAsNull(TEST_MAP, NOT_A_KEY));
        Map<String, Object> hasEmptyMap = new HashMap<>();
        hasEmptyMap.put("has_empty", "");
        assertNull(readStringEmptyAsNull(hasEmptyMap, "has_empty"));

        String dflt = "dflt";
        assertEquals(dflt, readString(TEST_MAP, INTEGER, dflt));
        assertEquals(dflt, readString(TEST_MAP, LONG, dflt));
        assertEquals(dflt, readString(TEST_MAP, BOOL, dflt));
        assertEquals(dflt, readString(TEST_MAP, MAP, dflt));
        assertEquals(dflt, readString(TEST_MAP, ARRAY, dflt));
        assertEquals(dflt, readString(TEST_MAP, NOT_A_KEY, dflt));

        assertNull(readString(TEST_MAP, NOT_A_KEY));
        assertNull(readString(TEST_MAP, INTEGER));

        assertEquals(STRING_STRING, readString(TEST_MAP, STRING, dflt));
        assertEquals(dflt, readString(TEST_MAP, NOT_A_KEY, dflt));
        assertEquals(dflt, readString(TEST_MAP, INTEGER, dflt));
        assertEquals(dflt, readString(null, NOT_A_KEY, dflt));
    }

    @Test
    public void testReadInteger() {
        Integer i = readInteger(TEST_MAP, INTEGER);
        assertEquals(42, i);

        assertNull(readInteger(TEST_MAP, STRING));
        assertNull(readInteger(TEST_MAP, BOOL));
        assertNull(readInteger(TEST_MAP, MAP));
        assertNull(readInteger(TEST_MAP, ARRAY));
        assertNull(readInteger(TEST_MAP, NOT_A_KEY));

        assertEquals(i, readInteger(TEST_MAP, STRING, i));
        assertEquals(i, readInteger(TEST_MAP, BOOL, i));
        assertEquals(i, readInteger(TEST_MAP, MAP, i));
        assertEquals(i, readInteger(TEST_MAP, ARRAY, i));
        assertEquals(i, readInteger(TEST_MAP, NOT_A_KEY, i));

        int dflt = 99;
        assertEquals(42, readInteger(TEST_MAP, INTEGER, dflt));
        assertEquals(dflt, readInteger(TEST_MAP, STRING, dflt));
        assertEquals(dflt, readInteger(TEST_MAP, BOOL, dflt));
        assertEquals(dflt, readInteger(TEST_MAP, MAP, dflt));
        assertEquals(dflt, readInteger(TEST_MAP, ARRAY, dflt));
        assertEquals(dflt, readInteger(TEST_MAP, NOT_A_KEY, dflt));
    }

    @Test
    public void testReadLong() {
        assertEquals(42, readLong(TEST_MAP, INTEGER));
        assertEquals(12345678901L, readLong(TEST_MAP, LONG));

        assertNull(readLong(TEST_MAP, STRING));
        assertNull(readLong(TEST_MAP, BOOL));
        assertNull(readLong(TEST_MAP, MAP));
        assertNull(readLong(TEST_MAP, ARRAY));
        assertNull(readLong(TEST_MAP, NOT_A_KEY));

        long dflt = 99;
        assertEquals(dflt, readLong(TEST_MAP, STRING, dflt));
        assertEquals(dflt, readLong(TEST_MAP, BOOL, dflt));
        assertEquals(dflt, readLong(TEST_MAP, MAP, dflt));
        assertEquals(dflt, readLong(TEST_MAP, ARRAY, dflt));
        assertEquals(dflt, readLong(TEST_MAP, NOT_A_KEY, dflt));

        assertEquals(42, readLong(TEST_MAP, INTEGER, dflt));
        assertEquals(12345678901L, readLong(TEST_MAP, LONG, dflt));
        assertEquals(dflt, readLong(TEST_MAP, STRING, dflt));
        assertEquals(dflt, readLong(TEST_MAP, BOOL, dflt));
        assertEquals(dflt, readLong(TEST_MAP, MAP, dflt));
        assertEquals(dflt, readLong(TEST_MAP, ARRAY, dflt));
        assertEquals(dflt, readLong(TEST_MAP, NOT_A_KEY, dflt));
    }

    @Test
    public void testReadBoolean() {
        Boolean b = readBoolean(TEST_MAP, BOOL);
        assertNotNull(b);
        assertTrue(b);
        assertNull(readBoolean(TEST_MAP, STRING));
        assertNull(readBoolean(TEST_MAP, INTEGER));
        assertNull(readBoolean(TEST_MAP, LONG));
        assertNull(readBoolean(TEST_MAP, MAP));
        assertNull(readBoolean(TEST_MAP, ARRAY));
        assertNull(readBoolean(TEST_MAP, NOT_A_KEY));

        assertTrue(readBoolean(TEST_MAP, BOOL, true));

        assertTrue(readBoolean(TEST_MAP, STRING, true));
        assertTrue(readBoolean(TEST_MAP, INTEGER, true));
        assertTrue(readBoolean(TEST_MAP, LONG, true));
        assertTrue(readBoolean(TEST_MAP, MAP, true));
        assertTrue(readBoolean(TEST_MAP, ARRAY, true));
        assertTrue(readBoolean(TEST_MAP, NOT_A_KEY, true));

        assertFalse(readBoolean(TEST_MAP, STRING, false));
        assertFalse(readBoolean(TEST_MAP, INTEGER, false));
        assertFalse(readBoolean(TEST_MAP, LONG, false));
        assertFalse(readBoolean(TEST_MAP, MAP, false));
        assertFalse(readBoolean(TEST_MAP, ARRAY, false));
        assertFalse(readBoolean(TEST_MAP, NOT_A_KEY, false));
    }

    @Test
    public void testReadDate() {
        ZonedDateTime t = readDate(TEST_MAP, DATE);
        assertEquals(TEST_DATE, t);

        assertThrows(DateTimeParseException.class, () -> readDate(TEST_MAP, STRING));

        assertNull(readDate(TEST_MAP, BOOL));
        assertNull(readDate(TEST_MAP, MAP));
        assertNull(readDate(TEST_MAP, ARRAY));
        assertNull(readDate(TEST_MAP, NOT_A_KEY));
    }

    @Test
    public void testReadNanos() {
        assertEquals(Duration.ofSeconds(1), readNanos(TEST_MAP, NANOS));

        assertNull(readNanos(TEST_MAP, STRING));
        assertNull(readNanos(TEST_MAP, BOOL));
        assertNull(readNanos(TEST_MAP, MAP));
        assertNull(readNanos(TEST_MAP, ARRAY));
        assertNull(readNanos(TEST_MAP, NOT_A_KEY));

        Duration dflt = Duration.ofSeconds(99);
        assertEquals(Duration.ofSeconds(1), readNanos(TEST_MAP, NANOS, dflt));
        assertEquals(dflt, readNanos(TEST_MAP, STRING, dflt));
        assertEquals(dflt, readNanos(TEST_MAP, BOOL, dflt));
        assertEquals(dflt, readNanos(TEST_MAP, MAP, dflt));
        assertEquals(dflt, readNanos(TEST_MAP, ARRAY, dflt));
        assertEquals(dflt, readNanos(TEST_MAP, NOT_A_KEY, dflt));
    }

    @Test
    public void testArraysAndMaps() {
        // smap has all string values
        List<Map<String, Object>> maps = new ArrayList<>();
        maps.add(readMap(TEST_MAP, SMAP));
        for (Map<String, Object> map : maps) {
            assertEquals("A", readString(map, "a"));
            assertEquals("B", readString(map, "b"));
            assertEquals("C", readString(map, "c"));
        }
        assertNull(readMap(TEST_MAP, STRING));
        assertNull(readArray(TEST_MAP, STRING));

        List<String> list = readArrayAsStrings(TEST_MAP, STRING);
        assertNotNull(list);
        assertTrue(list.isEmpty());

        list = readArrayAsStrings(TEST_MAP, SLIST);
        assertNotNull(list);
        assertEquals(3, list.size());
        assertTrue(list.contains("X"));
        assertTrue(list.contains("Y"));
        assertTrue(list.contains("Z"));

        list = readArrayAsStrings(TEST_MAP, MLIST);
        assertNotNull(list);
        assertEquals(4, list.size());
        assertTrue(list.contains("Q"));
        assertTrue(list.contains("R"));
        assertTrue(list.contains(" "));
        assertTrue(list.contains("98"));

        list = readArrayAsStrings(TEST_MAP, ILIST);
        assertNotNull(list);
        assertEquals(3, list.size());
        assertTrue(list.contains("42"));
        assertTrue(list.contains("73"));
        assertTrue(list.contains("99"));
    }
}
