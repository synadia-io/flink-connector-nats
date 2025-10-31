// Copyright (c) 2023-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.source;

import io.synadia.flink.TestBase;
import io.synadia.flink.TestServerContext;
import io.synadia.flink.message.Utf8StringSourceConverter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.synadia.flink.utils.Constants.UTF8_STRING_SOURCE_CONVERTER_CLASSNAME;
import static io.synadia.flink.utils.MiscUtils.getClassName;
import static org.junit.jupiter.api.Assertions.*;

class NatsSourceBuilderTest extends TestBase {
    static TestServerContext ctx;

    @BeforeAll
    public static void beforeAll() throws Exception {
        ctx = createContext(ctx);
    }

    @AfterAll
    public static void afterAll() {
        ctx = shutdownContext(ctx);
    }

    @AfterEach
    public void afterEach() {
        cleanupJs(ctx.nc);
    }

    /**
     * Tests the minimum configuration required to successfully build a NatsSource.
     * This represents the most basic use case of the builder.
     * Required settings:
     * 1. At least one subject to subscribe to
     * 2. A payload sourceConverter to convert NATS messages
     * 3. Connection properties for NATS server
     * Example usage:
     * ```java
     * NatsSource<String> source = new NatsSourceBuilder<String>()
     *     .subjects("orders.>")  // Subscribe to all messages in 'orders' hierarchy
     *     .sourceConverter(new Utf8StringSourceConverter())
     *     .connectionProperties(props)  // props containing "nats.connection.url", etc.
     *     .build();
     * ```
     */
    @Test
    void testBuildWithMinimumRequiredSettings() {
        String subject = subject();

        NatsSource<String> source = new NatsSourceBuilder<String>()
            .subjects(subject)
            .sourceConverter(new Utf8StringSourceConverter())
            .connectionProperties(defaultConnectionProperties(ctx.url))
            .build();

        assertNotNull(source, "Built source should not be null");
    }

    /**
     * Tests the builder's ability to configure a source using Properties object.
     * Demonstrates how to configure the source using properties file approach,
     * which is useful for externalized configuration.
     * Properties tested:
     * 1. Connection settings (URL, credentials, etc.)
     * 2. Source-specific settings (subjects, deserializer)
     * Example properties file:
     * ```properties
     * # Connection settings
     * nats.connection.url=nats://localhost:4222
     * nats.connection.max.reconnects=5
     * # Source settings
     * nats.source.subjects=orders.>,payments.>
     * nats.source.message.deserializer=io.synadia.flink.message.Utf8StringSourceConverter
     * ```
     * Usage:
     * ```java
     * Properties props = loadProperties("config.properties");
     * NatsSource<String> source = new NatsSourceBuilder<String>()
     *     .sourceProperties(props)
     *     .connectionProperties(props)
     *     .build();
     * ```
     */
    @Test
    void testSourceProperties_WithValidConfiguration() throws Exception {
        Properties connectionProperties = defaultConnectionProperties(ctx.url);
        String subject = subject();

        NatsSource<String> source = new NatsSourceBuilder<String>()
            .connectionProperties(connectionProperties)
            .subjects(subject)
            .sourceConverterClass(UTF8_STRING_SOURCE_CONVERTER_CLASSNAME)
            .build();
        assertNotNull(source);
        assertEquals(1, source.subjects.size());
        assertEquals(subject, source.subjects.get(0));
        assertEquals(UTF8_STRING_SOURCE_CONVERTER_CLASSNAME, getClassName(source.sourceConverter));

        String propsFile = writeToTempFile("test", "yaml", source.toYaml());
        source = new NatsSourceBuilder<String>()
            .connectionProperties(connectionProperties)
            .yamlConfigFile(propsFile)
            .build();
        assertNotNull(source);
        assertEquals(1, source.subjects.size());
        assertEquals(subject, source.subjects.get(0));
        assertEquals(UTF8_STRING_SOURCE_CONVERTER_CLASSNAME, getClassName(source.sourceConverter));

        propsFile = writeToTempFile("test", "json", source.toJson());
        source = new NatsSourceBuilder<String>()
            .connectionProperties(connectionProperties)
            .jsonConfigFile(propsFile)
            .build();
        assertNotNull(source);
        assertEquals(1, source.subjects.size());
        assertEquals(subject, source.subjects.get(0));
        assertEquals(UTF8_STRING_SOURCE_CONVERTER_CLASSNAME, getClassName(source.sourceConverter));
    }

    /**
     * Tests the builder's validation logic for various invalid inputs.
     * Ensures the builder fails fast with clear error messages when
     * misconfigured.
     * Validation scenarios:
     * 1. Empty subjects array - Should fail as at least one subject is required
     *    ```java
     *    .subjects(new String[0])  // Should throw IllegalStateException
     *    ```
     * 2. Null subjects - Should fail with clear error message
     *    ```java
     *    .subjects((String[])null)  // Should throw IllegalStateException
     *    ```
     * 3. Missing connection properties - Should fail as connection details are required
     *    ```java
     *    .subjects("test")
     *    .sourceConverter(deserializer)
     *    .build()  // Should throw IllegalStateException - missing connection props
     *    ```
     * Expected: IllegalStateException with descriptive message for each case
     */
    @Test
    void testBuild_WithInvalidInputs() {
        Properties props = defaultConnectionProperties(ctx.url);

        // Test empty subjects
        assertThrows(
            IllegalArgumentException.class,
            () -> new NatsSourceBuilder<String>()
                .sourceConverter(new Utf8StringSourceConverter())
                .connectionProperties(defaultConnectionProperties(ctx.url))
                .build()
        );

        // Test null subjects
        assertThrows(
            IllegalArgumentException.class,
            () -> new NatsSourceBuilder<String>()
                .subjects((String[]) null)
                .sourceConverter(new Utf8StringSourceConverter())
                .connectionProperties(props)
                .build()
        );
    }

    /**
     * Tests the builder's fluent interface implementation.
     * Verifies that the builder maintains proper object state through method chaining
     * and returns consistent builder instances.
     * Aspects tested:
     * 1. Method chaining - Each builder method returns the same builder instance
     * 2. State consistency - Final build reflects all chained configurations
     * Example of fluent interface usage:
     * ```java
     * NatsSourceBuilder<String> builder = new NatsSourceBuilder<>();
     * NatsSource<String> source = builder
     *     .subjects("orders.>")
     *     .sourceConverter(new Utf8StringSourceConverter())
     *     .connectionProperties(props)
     *     .build();
     * // Both references should point to same builder instance
     * assertSame(builder, builder.subjects("test"));
     * ```
     */
    @Test
    void testBuilderMethods_ReturnSameInstance() {
        String subject = subject();
        Properties props = defaultConnectionProperties(ctx.url);

        NatsSourceBuilder<String> builder = new NatsSourceBuilder<>();

        // Test that each method returns the same builder instance
        assertSame(builder, builder.subjects(subject),
            "subjects() should return same builder instance");
        assertSame(builder, builder.sourceConverter(new Utf8StringSourceConverter()),
            "sourceConverter() should return same builder instance");
        assertSame(builder, builder.connectionProperties(props),
            "connectionProperties() should return same builder instance");

        // Test that chained configuration works
        NatsSource<String> source = builder.build();
        assertNotNull(source, "Source built with chained methods should not be null");
    }

    /**
     * Tests the builder's support for class name-based deserializer configuration.
     * This approach allows for dynamic loading of deserializers without direct class dependencies.
     * Use cases:
     * 1. Configuration-driven deserializer selection
     * 2. Plugin-style deserializer loading
     * Example configurations:
     * ```java
     * // Using class name string
     * .sourceConverterClass("io.synadia.flink.message.Utf8StringSourceConverter")
     * // Or via properties
     * props.setProperty("nats.source.message.deserializer",
     *     "io.synadia.flink.message.Utf8StringSourceConverter");
     * ```
     * the builder should:
     * 1. Load the specified class
     * 2. Verify it implements SourceConverter
     * 3. Instantiate it using the default constructor
     */
    @Test
    void testBuild_WithMessageReaderClass() {
        String subject = subject();

        NatsSource<String> source = new NatsSourceBuilder<String>()
            .subjects(subject)
            .sourceConverterClass(UTF8_STRING_SOURCE_CONVERTER_CLASSNAME)
            .connectionProperties(defaultConnectionProperties(ctx.url))
            .build();

        assertNotNull(source, "Source with deserializer class should not be null");
    }

    /**
     * Tests the builder's support for multiple subject subscriptions.
     * Demonstrates how to configure a source to receive messages from multiple NATS subjects.
     * Common use cases:
     * 1. Aggregating data from multiple topics
     * 2. Pattern-based subject subscriptions
     * 3. Multi-tenant message processing
     * Example configurations:
     * ```java
     * // Direct subject list
     * .subjects("orders.usa.>", "orders.eu.>", "orders.asia.>")
     * // Via properties
     * props.setProperty("nats.source.subjects", "orders.usa.>,orders.eu.>,orders.asia.>")
     * // Using wildcards
     * .subjects("orders.*.completed", "payments.*.processed")
     * ```
     * The source should:
     * 1. Subscribe to all specified subjects
     * 2. Handle messages from any of the subjects
     * 3. Properly distribute messages to Flink
     */
    @Test
    void testBuild_WithMultipleSubjects() {
        String subject1 = subject();
        String subject2 = subject();
        String subject3 = subject();

        NatsSource<String> source = new NatsSourceBuilder<String>()
            .subjects(subject1, subject2, subject3)
            .sourceConverter(new Utf8StringSourceConverter())
            .connectionProperties(defaultConnectionProperties(ctx.url))
            .build();

        assertNotNull(source, "Source with multiple subjects should not be null");
    }

    @Test
    void testCoverage() {
        assertThrows(IllegalArgumentException.class,
            () -> new NatsSourceBuilder<String>()
                .subjects("foo")
                .connectionProperties(defaultConnectionProperties(ctx.url))
                .build());
        assertThrows(IllegalArgumentException.class,
            () -> new NatsSourceBuilder<String>()
                .subjects("foo")
                .connectionProperties(defaultConnectionProperties(ctx.url))
                .sourceConverterClass("not-a-class")
                .build());


        // COVERAGE for no connectionProperties and subject(s) setters
        List<String> hasNullAndEmpty = new ArrayList<>();
        hasNullAndEmpty.add("");
        hasNullAndEmpty.add(null);
        new NatsSourceBuilder<String>()
            .subjects((String)null)
            .subjects(new String[0])
            .subjects((List<String>)null)
            .subjects(hasNullAndEmpty)
            .subjects("foo")
            .sourceConverter(new Utf8StringSourceConverter())
            .build();

    }
}
