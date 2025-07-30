// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

import io.nats.client.support.JsonSerializable;
import io.synadia.flink.helpers.WordCount;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MiscUtilsTest {

    @Test
    void testClientVersion() {
        assertNotNull(MiscUtils.CLIENT_VERSION);
        assertFalse(MiscUtils.CLIENT_VERSION.isEmpty());
    }

    @Test
    void testGenerateId() {
        String id1 = MiscUtils.generateId();
        String id2 = MiscUtils.generateId();
        
        assertNotNull(id1);
        assertNotNull(id2);
        assertEquals(4, id1.length());
        assertEquals(4, id2.length());
        assertNotEquals(id1, id2);
    }

    @Test
    void testGeneratePrefixedId() {
        String prefix = "test";
        String id = MiscUtils.generatePrefixedId(prefix);
        
        assertNotNull(id);
        assertTrue(id.startsWith(prefix + "-"));
        assertEquals(prefix.length() + 5, id.length());
    }

    @Test
    void testProvidedString() {
        assertTrue(MiscUtils.provided("hello"));
        assertTrue(MiscUtils.provided("a"));
        assertFalse(MiscUtils.provided(""));
        //noinspection ConstantValue
        assertFalse(MiscUtils.provided((String)null));
    }

    @Test
    void testNotProvidedString() {
        assertFalse(MiscUtils.notProvided("hello"));
        assertFalse(MiscUtils.notProvided("a"));
        assertTrue(MiscUtils.notProvided(""));
        //noinspection ConstantValue
        assertTrue(MiscUtils.notProvided((String)null));
    }

    @Test
    void testProvidedCollection() {
        List<String> list = new ArrayList<>(Arrays.asList("a", "b"));
        assertTrue(MiscUtils.provided(list));
        list.clear();
        assertFalse(MiscUtils.provided(list));
        //noinspection ConstantValue
        assertFalse(MiscUtils.provided((Collection<?>)null));
    }

    @Test
    void testNotProvidedCollection() {
        List<String> list = new ArrayList<>(Arrays.asList("a", "b"));
        assertFalse(MiscUtils.notProvided(list));
        list.clear();
        assertTrue(MiscUtils.notProvided(list));
        //noinspection ConstantValue
        assertTrue(MiscUtils.notProvided((Collection<?>) null));
    }

    @Test
    void testRandom() {
        String random1 = MiscUtils.random();
        String random2 = MiscUtils.random();
        assertNotNull(random1);
        assertNotNull(random2);
        assertNotEquals(random1, random2);
    }

    @Test
    void testRandomWithPrefix() {
        String prefix = "prefix";
        String random1 = MiscUtils.random(prefix);
        String random2 = MiscUtils.random(prefix);
        assertNotNull(random1);
        assertNotNull(random2);
        assertTrue(random1.startsWith(prefix + "-"));
        assertTrue(random2.startsWith(prefix + "-"));
        assertNotEquals(random1, random2);
    }

    @Test
    void testGetTypeInformation() {
        TypeInformation<TestClass> ti = MiscUtils.getTypeInformation(TestClass.class);
        
        assertNotNull(ti);
        assertFalse(ti.isBasicType());
        assertSame(TestClass.class, ti.getTypeClass());
        assertEquals(2, ti.getArity());

        TypeInformation<WordCount> tiwc = MiscUtils.getTypeInformation(WordCount.class);
        assertNotNull(tiwc);
        assertSame(WordCount.class, tiwc.getTypeClass());
        assertFalse(tiwc.isBasicType());
        assertEquals(2, tiwc.getArity());
    }

    @Test
    void testGetClassName() {
        String s = "test";
        assertEquals("java.lang.String", MiscUtils.getClassName(s));
        TestClass testObj = new TestClass();
        String testClassName = MiscUtils.getClassName(testObj);
        assertEquals("io.synadia.flink.utils.MiscUtilsTest$TestClass", testClassName);
    }

    @Test
    void testCreateInstanceOf() throws ReflectiveOperationException {
        Object instance = MiscUtils.createInstanceOf("java.lang.String");
        assertNotNull(instance);
        assertTrue(instance instanceof String);
        
        Object testInstance = MiscUtils.createInstanceOf("io.synadia.flink.utils.MiscUtilsTest$TestClass");
        assertNotNull(testInstance);
        assertTrue(testInstance instanceof TestClass);
    }

    @Test
    void testCreateInstanceOfInvalidClass() {
        assertThrows(ClassNotFoundException.class, () -> {
            MiscUtils.createInstanceOf("invalid.class.Name");
        });
    }

    @Test
    void testChecksum() {
        String checksum1 = MiscUtils.checksum("hello", "world");
        String checksum2 = MiscUtils.checksum("hello", "world");
        String checksum3 = MiscUtils.checksum("world", "hello");
        
        assertNotNull(checksum1);
        assertNotNull(checksum2);
        assertNotNull(checksum3);
        assertEquals(checksum1, checksum2);
        assertNotEquals(checksum1, checksum3);
    }

    @Test
    void testChecksumWithNull() {
        String checksum = MiscUtils.checksum("hello", null, "world");
        assertNotNull(checksum);
        assertTrue(checksum.matches("[0-9A-F]+"));
    }

    @Test
    void testChecksumWithJsonSerializable() {
        TestJsonSerializable json = new TestJsonSerializable();
        String checksum = MiscUtils.checksum("test", json);
        assertNotNull(checksum);
    }

    @Test
    void testChecksumWithZonedDateTime() {
        ZonedDateTime now = ZonedDateTime.now();
        String checksum = MiscUtils.checksum("test", now);
        assertNotNull(checksum);
    }

    @Test
    void testGetInputStream(@TempDir Path tempDir) throws IOException {
        Path testFile = tempDir.resolve("test.txt");
        String content = "test content";
        Files.write(testFile, content.getBytes());
        
        try (InputStream is = MiscUtils.getInputStream(testFile.toString())) {
            assertNotNull(is);
            byte[] bytes = is.readAllBytes();
            assertEquals(content, new String(bytes));
        }
    }

    @Test
    void testGetInputStreamNonExistentFile() {
        assertThrows(IOException.class, () -> {
            MiscUtils.getInputStream("/non/existent/file.txt");
        });
    }

    @Test
    void testReadAllBytes(@TempDir Path tempDir) throws IOException {
        Path testFile = tempDir.resolve("test.txt");
        String content = "test content for reading all bytes";
        Files.write(testFile, content.getBytes());
        
        byte[] bytes = MiscUtils.readAllBytes(testFile.toString());
        assertNotNull(bytes);
        assertEquals(content, new String(bytes));
    }

    @Test
    void testReadAllBytesNonExistentFile() {
        assertThrows(IOException.class, () -> {
            MiscUtils.readAllBytes("/non/existent/file.txt");
        });
    }

    @Test
    void testConstants() {
        assertEquals("--", MiscUtils.SEP);
        assertEquals("na", MiscUtils.NULL_SEGMENT);
    }

    public static class TestClass {
        private String field1;
        private int field2;

        public TestClass() {}

        public String getField1() { return field1; }
        public void setField1(String field1) { this.field1 = field1; }
        public int getField2() { return field2; }
        public void setField2(int field2) { this.field2 = field2; }
    }

    private static class TestJsonSerializable implements JsonSerializable {
        @Override
        public String toJson() {
            return "{\"test\": \"value\"}";
        }
    }
}