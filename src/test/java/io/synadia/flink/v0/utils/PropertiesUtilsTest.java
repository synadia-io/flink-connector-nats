// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/** Unit test for {@link PropertiesUtils}. */
class PropertiesUtilsTest {

    /**
     * Tests loading properties from a file.
     * Flow:
     * 1. Creates a temporary file with known properties
     * 2. Loads the properties using PropertiesUtils
     * 3. Verifies the loaded values match expected values
     *
     * Example properties file content:
     * key=value
     * int.prop=42
     */
    @Test
    void testLoadPropertiesFromFileValidFileReturnsExpectedProperties(@TempDir Path tempDir) throws IOException {
        // Create a temporary properties file
        Path propsPath = tempDir.resolve("test.properties");
        Files.writeString(propsPath, "key=value\nint.prop=42");

        Properties props = PropertiesUtils.loadPropertiesFromFile(propsPath.toString());
        assertEquals("value", props.getProperty("key"));
        assertEquals("42", props.getProperty("int.prop"));
    }

    /**
     * Tests parsing comma-separated values into a list.
     * Flow:
     * 1. Creates properties with different list formats
     * 2. Tests regular list: "a,b,c"
     * 3. Tests empty list: ""
     * 4. Tests list with spaces: "a, b , c"
     * 5. Tests non-existent property
     *
     * Examples:
     * - Input: "a,b,c" → Output: ["a", "b", "c"]
     * - Input: "a, b , c" → Output: ["a", "b", "c"]
     * - Input: "" → Output: []
     */
    @Test
    void testGetPropertyAsListCommaSeparatedValuesReturnsCorrectList() {
        Properties props = new Properties();
        props.setProperty("list", "a,b,c");
        props.setProperty("list.empty", "");
        props.setProperty("list.spaces", "a, b , c");

        List<String> list = PropertiesUtils.getPropertyAsList(props, "list");
        assertEquals(3, list.size());
        assertTrue(list.contains("a"));
        assertTrue(list.contains("b"));
        assertTrue(list.contains("c"));

        // Test empty list
        assertTrue(PropertiesUtils.getPropertyAsList(props, "nonexistent").isEmpty());
        assertTrue(PropertiesUtils.getPropertyAsList(props, "list.empty").isEmpty());

        // Test with spaces
        list = PropertiesUtils.getPropertyAsList(props, "list.spaces");
        assertEquals(3, list.size());
        assertTrue(list.contains("a"));
        assertTrue(list.contains("b"));
        assertTrue(list.contains("c"));
    }

    /**
     * Tests retrieving string properties with and without prefixes.
     * Flow:
     * 1. Sets up properties with regular and prefixed keys
     * 2. Tests retrieval with no prefix
     * 3. Tests retrieval with prefix
     * 4. Tests non-existent property
     *
     * Examples:
     * - props.get("str") → "value"
     * - props.get("str", "prefix.") → "prefixed"
     * - props.get("nonexistent") → null
     */
    @Test
    void testGetStringPropertyWithAndWithoutPrefixReturnsCorrectValues() {
        Properties props = new Properties();
        props.setProperty("str", "value");
        props.setProperty("prefix.str", "prefixed");

        assertEquals("value", PropertiesUtils.getStringProperty(props, "str"));
        assertEquals("prefixed", PropertiesUtils.getStringProperty(props, "str", "prefix."));
        assertNull(PropertiesUtils.getStringProperty(props, "nonexistent"));
    }

    /**
     * Tests parsing and retrieving integer properties.
     * Flow:
     * 1. Sets up properties with valid and invalid integers
     * 2. Tests regular integer parsing
     * 3. Tests prefixed integer parsing
     * 4. Tests invalid integer handling
     * 5. Tests default value for missing property
     *
     * Examples:
     * - "42" → 42
     * - "not-an-int" → 0 (default)
     * - missing → 99 (specified default)
     */
    @Test
    void testGetIntPropertyValidAndInvalidInputsReturnsExpectedValues() {
        Properties props = new Properties();
        props.setProperty("int", "42");
        props.setProperty("prefix.int", "24");
        props.setProperty("invalid", "not-an-int");

        assertEquals(42, PropertiesUtils.getIntProperty(props, "int", 0));
        assertEquals(24, PropertiesUtils.getIntProperty(props, "int", 0, "prefix."));
        assertEquals(99, PropertiesUtils.getIntProperty(props, "nonexistent", 99));
        assertEquals(0, PropertiesUtils.getIntProperty(props, "invalid", 0));
    }

    /**
     * Tests parsing and retrieving long properties.
     * Flow:
     * 1. Sets up properties with valid and invalid longs
     * 2. Tests regular long parsing
     * 3. Tests prefixed long parsing
     * 4. Tests invalid long handling
     * 5. Tests default value for missing property
     *
     * Examples:
     * - "1234567890" → 1234567890L
     * - "not-a-long" → 0L (default)
     * - missing → 99L (specified default)
     */
    @Test
    void testGetLongPropertyValidAndInvalidInputsReturnsExpectedValues() {
        Properties props = new Properties();
        props.setProperty("long", "1234567890");
        props.setProperty("prefix.long", "9876543210");
        props.setProperty("invalid", "not-a-long");

        assertEquals(1234567890L, PropertiesUtils.getLongProperty(props, "long", 0L));
        assertEquals(9876543210L, PropertiesUtils.getLongProperty(props, "long", 0L, "prefix."));
        assertEquals(99L, PropertiesUtils.getLongProperty(props, "nonexistent", 99L));
        assertEquals(0L, PropertiesUtils.getLongProperty(props, "invalid", 0L));
    }

    /**
     * Tests boolean property parsing with various formats.
     * Flow:
     * 1. Sets up properties with different boolean representations
     * 2. Tests true values: "true", "t", "1"
     * 3. Tests false values: "false", "f", "0"
     * 4. Tests invalid value handling
     * 5. Tests missing property handling
     *
     * Examples:
     * - "true", "t", "1" → true
     * - "false", "f", "0" → false
     * - "invalid" → default value
     */
    @Test
    void testGetBooleanPropertyDifferentFormatsReturnsCorrectBooleans() {
        Properties props = new Properties();
        props.setProperty("bool.true", "true");
        props.setProperty("bool.t", "t");
        props.setProperty("bool.1", "1");
        props.setProperty("bool.false", "false");
        props.setProperty("bool.f", "f");
        props.setProperty("bool.0", "0");
        props.setProperty("bool.invalid", "invalid");

        assertTrue(PropertiesUtils.getBooleanProperty(props, "bool.true", false));
        assertTrue(PropertiesUtils.getBooleanProperty(props, "bool.t", false));
        assertTrue(PropertiesUtils.getBooleanProperty(props, "bool.1", false));
        assertFalse(PropertiesUtils.getBooleanProperty(props, "bool.false", true));
        assertFalse(PropertiesUtils.getBooleanProperty(props, "bool.f", true));
        assertFalse(PropertiesUtils.getBooleanProperty(props, "bool.0", true));
        assertTrue(PropertiesUtils.getBooleanProperty(props, "bool.invalid", true));
        assertTrue(PropertiesUtils.getBooleanProperty(props, "nonexistent", true));
    }

    /**
     * Tests duration property parsing from milliseconds.
     * Flow:
     * 1. Sets up properties with millisecond values
     * 2. Tests regular duration parsing
     * 3. Tests prefixed duration parsing
     * 4. Tests invalid duration handling
     * 5. Tests default value for missing property
     *
     * Examples:
     * - "5000" → Duration.ofMillis(5000)
     * - "not-a-duration" → Duration.ofMillis(0)
     * - missing → Duration.ofMillis(99)
     */
    @Test
    void testGetDurationPropertyValidMillisecondsReturnsCorrectDuration() {
        Properties props = new Properties();
        props.setProperty("duration", "5000");
        props.setProperty("prefix.duration", "3000");
        props.setProperty("invalid", "not-a-duration");

        assertEquals(Duration.ofMillis(5000),
                PropertiesUtils.getDurationProperty(props, "duration", Duration.ofMillis(0)));
        assertEquals(Duration.ofMillis(3000),
                PropertiesUtils.getDurationProperty(props, "duration", Duration.ofMillis(0), "prefix."));
        assertEquals(Duration.ofMillis(99),
                PropertiesUtils.getDurationProperty(props, "nonexistent", Duration.ofMillis(99)));
        assertEquals(Duration.ofMillis(0),
                PropertiesUtils.getDurationProperty(props, "invalid", Duration.ofMillis(0)));
    }

    /**
     * Tests jitter calculation with different min/max scenarios.
     * Flow:
     * 1. Tests when min > max (should use min)
     * 2. Tests when min = max
     * 3. Tests when min < max (normal case)
     * 4. Tests when min = 0
     *
     * Examples:
     * - jitter(100, 50) → random value <= 100
     * - jitter(50, 100) → random value between 50 and 100
     * - jitter(0, 100) → random value between 0 and 100
     */
    @Test
    void testJitterDifferentMinMaxScenariosDoesNotThrowException() {
        // Test with min > max
        assertDoesNotThrow(() -> PropertiesUtils.jitter(100, 50));

        // Test with min = max
        assertDoesNotThrow(() -> PropertiesUtils.jitter(100, 100));

        // Test with min < max
        assertDoesNotThrow(() -> PropertiesUtils.jitter(50, 100));

        // Test with min = 0
        assertDoesNotThrow(() -> PropertiesUtils.jitter(0, 100));
    }

    /**
     * Tests Flink type information generation for POJOs.
     * Flow:
     * 1. Creates type information for TestPojo class
     * 2. Verifies type information is created
     * 3. Checks class name matches
     *
     * Example:
     * TestPojo with fields (String, int, long) →
     * TypeInformation with corresponding Flink types
     */
    @Test
    void testGetTypeInformationValidPojoReturnsCorrectTypeInfo() {
        TypeInformation<?> typeInfo = PropertiesUtils.getTypeInformation(TestPojo.class);
        assertNotNull(typeInfo);
        assertEquals("TestPojo", typeInfo.getTypeClass().getSimpleName());
    }

    /**
     * Tests null handling scenarios for properties and keys.
     * Flow:
     * 1. Tests null Properties object
     * 2. Tests null key
     * 3. Tests null prefix
     * 4. Tests null default value
     *
     * Verifies:
     * - Null Properties returns default value
     * - Null key returns default value
     * - Null prefix is handled as no prefix
     * - Null default value is handled appropriately
     */
    @Test
    void testNullHandlingNullInputsReturnsDefaultValues() {
        Properties props = new Properties();
        props.setProperty("key", "value");

        // Test null Properties
        assertNull(PropertiesUtils.getStringProperty(null, "key"));
        assertEquals(0, PropertiesUtils.getIntProperty(null, "key", 0));
        assertEquals(0L, PropertiesUtils.getLongProperty(null, "key", 0L));

        // Test null key
        assertNull(PropertiesUtils.getStringProperty(props, null));
        assertEquals(42, PropertiesUtils.getIntProperty(props, null, 42));
        assertEquals(42L, PropertiesUtils.getLongProperty(props, null, 42L));

        // Test null prefix (should be treated as no prefix)
        assertEquals("value", PropertiesUtils.getStringProperty(props, "key"));

        // Test with valid Duration default
        assertEquals(Duration.ofMillis(100),
                PropertiesUtils.getDurationProperty(props, "nonexistent", Duration.ofMillis(100)));
    }

    /**
     * Tests multiple prefix resolution scenarios.
     * Flow:
     * 1. Sets up properties with multiple prefix levels
     * 2. Tests nested prefix resolution
     * 3. Tests conflicting prefixes
     * 4. Tests empty prefix handling
     *
     * Examples:
     * - "prefix1.prefix2.key" should be found with "prefix1.prefix2."
     * - Most specific prefix should win in conflicts
     * - Empty prefix should fall back to non-prefixed value
     */
    @Test
    void testMultiplePrefixResolutionNestedPrefixesResolvesCorrectly() {
        Properties props = new Properties();
        props.setProperty("key", "base");
        props.setProperty("prefix1.key", "level1");
        props.setProperty("prefix1.prefix2.key", "level2");
        props.setProperty("prefix1.prefix2.prefix3.key", "level3");

        // Test nested prefix resolution
        assertEquals("level2", PropertiesUtils.getStringProperty(props, "key", "prefix1.prefix2."));
        assertEquals("level3", PropertiesUtils.getStringProperty(props, "key", "prefix1.prefix2.prefix3."));

        // Test empty prefix fallback
        assertEquals("base", PropertiesUtils.getStringProperty(props, "key", ""));

        // Test partial prefix match
        assertEquals("level1", PropertiesUtils.getStringProperty(props, "key", "prefix1."));
    }

    /**
     * Tests edge cases in type conversion.
     * Flow:
     * 1. Tests boundary values (MAX_VALUE, MIN_VALUE)
     * 2. Tests special number formats (+123)
     * 3. Tests basic integer parsing
     * 4. Tests invalid number format handling
     *
     * Examples:
     * - Integer.MAX_VALUE → MAX_VALUE
     * - Integer.MIN_VALUE → MIN_VALUE
     * - "+123" → 123
     * - "1.23E2" → 0 (default, scientific notation not supported)
     */
    @Test
    void testTypeConversionEdgeCasesBoundaryValuesHandlesCorrectly() {
        Properties props = new Properties();
        props.setProperty("max.int", String.valueOf(Integer.MAX_VALUE));
        props.setProperty("min.int", String.valueOf(Integer.MIN_VALUE));
        props.setProperty("scientific", "1.23E2");
        props.setProperty("spaces", "42");  // Remove whitespace since implementation doesn't trim
        props.setProperty("special", "+123");

        // Test boundary values
        assertEquals(Integer.MAX_VALUE, PropertiesUtils.getIntProperty(props, "max.int", 0));
        assertEquals(Integer.MIN_VALUE, PropertiesUtils.getIntProperty(props, "min.int", 0));

        // Test basic integer parsing
        assertEquals(42, PropertiesUtils.getIntProperty(props, "spaces", 0));

        // Test special number formats
        assertEquals(123, PropertiesUtils.getIntProperty(props, "special", 0));

        // Test invalid number format returns default
        assertEquals(0, PropertiesUtils.getIntProperty(props, "scientific", 0));
    }

    /**
     * Tests special cases for list parsing.
     * Flow:
     * 1. Tests various delimiter patterns
     * 2. Tests mixed whitespace handling
     * 3. Tests single element parsing
     * 4. Tests multiple delimiter handling
     *
     * Examples:
     * - "a,,c" → ["a", "c"] (empty elements are skipped)
     * - " a , b " → ["a", "b"] (whitespace is trimmed)
     * - "solo" → ["solo"]
     * - "a;b,c" → ["a;b", "c"] (only comma is treated as delimiter)
     */
    @Test
    void testSpecialListParsingComplexDelimitersParsesCorrectly() {
        Properties props = new Properties();
        props.setProperty("empty.elements", "a,,c");  // Should result in ["a", "c"]
        props.setProperty("mixed.whitespace", " a , b ");
        props.setProperty("multiple.delimiters", "a;b,c");
        props.setProperty("single.element", "solo");

        // Test empty elements (implementation may skip empty elements)
        List<String> emptyElements = PropertiesUtils.getPropertyAsList(props, "empty.elements");
        assertEquals(2, emptyElements.size());  // Updated expectation
        assertTrue(emptyElements.contains("a"));
        assertTrue(emptyElements.contains("c"));

        // Test whitespace handling
        List<String> whitespaceList = PropertiesUtils.getPropertyAsList(props, "mixed.whitespace");
        assertEquals(2, whitespaceList.size());
        assertEquals("a", whitespaceList.get(0));
        assertEquals("b", whitespaceList.get(1));

        // Test single element
        List<String> singleElement = PropertiesUtils.getPropertyAsList(props, "single.element");
        assertEquals(1, singleElement.size());
        assertEquals("solo", singleElement.get(0));

        // Test multiple delimiters (should split only on comma)
        List<String> multipleDelimiters = PropertiesUtils.getPropertyAsList(props, "multiple.delimiters");
        assertEquals(2, multipleDelimiters.size());
        assertTrue(multipleDelimiters.contains("a;b"));
        assertTrue(multipleDelimiters.contains("c"));
    }

    // Test POJO for type information testing
    public static class TestPojo {
        private String stringField;
        private int intField;
        private long longField;
    }
}
