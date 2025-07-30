// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class PropertiesUtilsTest {

    @Test
    void testLoadPropertiesFromFile(@TempDir Path tempDir) throws IOException {
        Path propsFile = tempDir.resolve("test.properties");
        String content = "key1=value1\nkey2=value2\nintProp=123\n";
        Files.write(propsFile, content.getBytes());

        Properties props = PropertiesUtils.loadPropertiesFromFile(propsFile.toString());

        assertNotNull(props);
        assertEquals("value1", props.getProperty("key1"));
        assertEquals("value2", props.getProperty("key2"));
        assertEquals("123", props.getProperty("intProp"));
    }

    @Test
    void testLoadPropertiesFromNonExistentFile() {
        assertThrows(IOException.class, () -> {
            PropertiesUtils.loadPropertiesFromFile("/non/existent/file.properties");
        });
    }

    @Test
    void testGetPropertyAsList() {
        Properties props = new Properties();
        props.setProperty("listProp", "a,b,c");
        props.setProperty("listWithSpaces", " a , b , c ");
        props.setProperty("emptyItems", "a,,b,");
        props.setProperty("singleItem", "single");

        List<String> list1 = PropertiesUtils.getPropertyAsList(props, "listProp");
        assertEquals(3, list1.size());
        assertEquals("a", list1.get(0));
        assertEquals("b", list1.get(1));
        assertEquals("c", list1.get(2));

        List<String> list2 = PropertiesUtils.getPropertyAsList(props, "listWithSpaces");
        assertEquals(3, list2.size());
        assertEquals("a", list2.get(0));
        assertEquals("b", list2.get(1));
        assertEquals("c", list2.get(2));

        List<String> list3 = PropertiesUtils.getPropertyAsList(props, "emptyItems");
        assertEquals(2, list3.size());
        assertEquals("a", list3.get(0));
        assertEquals("b", list3.get(1));

        List<String> list4 = PropertiesUtils.getPropertyAsList(props, "singleItem");
        assertEquals(1, list4.size());
        assertEquals("single", list4.get(0));

        List<String> list5 = PropertiesUtils.getPropertyAsList(props, "nonExistent");
        assertTrue(list5.isEmpty());
    }

    @Test
    void testGetAsList() {
        List<String> list1 = PropertiesUtils.getAsList("a,b,c");
        assertEquals(3, list1.size());
        assertEquals("a", list1.get(0));
        assertEquals("b", list1.get(1));
        assertEquals("c", list1.get(2));

        List<String> list2 = PropertiesUtils.getAsList(" a , b , c ");
        assertEquals(3, list2.size());
        assertEquals("a", list2.get(0));
        assertEquals("b", list2.get(1));
        assertEquals("c", list2.get(2));

        List<String> list3 = PropertiesUtils.getAsList("a,,b,");
        assertEquals(2, list3.size());
        assertEquals("a", list3.get(0));
        assertEquals("b", list3.get(1));

        List<String> list4 = PropertiesUtils.getAsList("single");
        assertEquals(1, list4.size());
        assertEquals("single", list4.get(0));

        List<String> list5 = PropertiesUtils.getAsList("");
        assertTrue(list5.isEmpty());

        List<String> list6 = PropertiesUtils.getAsList(null);
        assertTrue(list6.isEmpty());

        List<String> list7 = PropertiesUtils.getAsList("   ");
        assertTrue(list7.isEmpty());

        List<String> list8 = PropertiesUtils.getAsList(",,,");
        assertTrue(list8.isEmpty());
    }

    @Test
    void testGetStringProperty() {
        Properties props = new Properties();
        props.setProperty("stringProp", "testValue");

        String value1 = PropertiesUtils.getStringProperty(props, "stringProp");
        assertEquals("testValue", value1);

        String value2 = PropertiesUtils.getStringProperty(props, "nonExistent");
        assertNull(value2);
    }

    @Test
    void testGetIntProperty() {
        Properties props = new Properties();
        props.setProperty("intProp", "123");
        props.setProperty("invalidInt", "notANumber");

        int value1 = PropertiesUtils.getIntProperty(props, "intProp", 0);
        assertEquals(123, value1);

        int value2 = PropertiesUtils.getIntProperty(props, "nonExistent", 456);
        assertEquals(456, value2);

        assertThrows(NumberFormatException.class, () -> {
            PropertiesUtils.getIntProperty(props, "invalidInt", 0);
        });
    }

    @Test
    void testGetLongProperty() {
        Properties props = new Properties();
        props.setProperty("longProp", "123456789");
        props.setProperty("invalidLong", "notANumber");

        long value1 = PropertiesUtils.getLongProperty(props, "longProp", 0L);
        assertEquals(123456789L, value1);

        long value2 = PropertiesUtils.getLongProperty(props, "nonExistent", 987654321L);
        assertEquals(987654321L, value2);

        assertThrows(NumberFormatException.class, () -> {
            PropertiesUtils.getLongProperty(props, "invalidLong", 0L);
        });
    }

    @Test
    void testGetBooleanProperty() {
        Properties props = new Properties();
        props.setProperty("trueValue1", "true");
        props.setProperty("trueValue2", "TRUE");
        props.setProperty("trueValue3", "t");
        props.setProperty("trueValue4", "T");
        props.setProperty("trueValue5", "1");
        props.setProperty("notTrue", "notTrue");
        props.setProperty("emptyValue", "");

        assertTrue(PropertiesUtils.getBooleanProperty(props, "trueValue1", false));
        assertTrue(PropertiesUtils.getBooleanProperty(props, "trueValue2", false));
        assertTrue(PropertiesUtils.getBooleanProperty(props, "trueValue3", false));
        assertTrue(PropertiesUtils.getBooleanProperty(props, "trueValue4", false));
        assertTrue(PropertiesUtils.getBooleanProperty(props, "trueValue5", false));

        assertFalse(PropertiesUtils.getBooleanProperty(props, "notTrue", true));
        assertFalse(PropertiesUtils.getBooleanProperty(props, "notTrue", false));

        assertFalse(PropertiesUtils.getBooleanProperty(props, "emptyValue", true));
        assertFalse(PropertiesUtils.getBooleanProperty(props, "emptyValue", false));

        assertTrue(PropertiesUtils.getBooleanProperty(props, "nonExistent", true));
        assertFalse(PropertiesUtils.getBooleanProperty(props, "nonExistent", false));
    }

    @Test
    void testGetDurationProperty() {
        Properties props = new Properties();
        props.setProperty("durationProp", "5000");
        props.setProperty("invalidDuration", "notANumber");

        Duration duration1 = PropertiesUtils.getDurationProperty(props, "durationProp", Duration.ofSeconds(10));
        assertEquals(Duration.ofMillis(5000), duration1);

        Duration duration2 = PropertiesUtils.getDurationProperty(props, "nonExistent", Duration.ofSeconds(30));
        assertEquals(Duration.ofSeconds(30), duration2);

        assertThrows(NumberFormatException.class, () -> {
            PropertiesUtils.getDurationProperty(props, "invalidDuration", Duration.ofSeconds(10));
        });
    }
}